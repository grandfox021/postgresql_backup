#!/usr/bin/env python3
import subprocess
import logging
from pathlib import Path
from datetime import datetime, timedelta
import shutil
import re
import os
import sys
from urllib.parse import urlparse

# =========================
# Manual .env loader
# =========================
def load_env_file(path, override=False):
    loaded = {}
    if not Path(path).exists():
        logging.warning(f"⚠️ Env file not found: {path}")
        return loaded

    def _strip_inline_comment(s):
        in_q = False
        qchar = ''
        for i, ch in enumerate(s):
            if ch in ('"', "'"):
                if not in_q:
                    in_q = True
                    qchar = ch
                elif qchar == ch:
                    in_q = False
                    qchar = ''
            elif ch == '#' and not in_q:
                return s[:i].rstrip()
        return s

    def _expand_vars(s, local_env):
        pattern = re.compile(r'\$\{([^}]+)\}')
        def repl(m):
            k = m.group(1)
            return local_env.get(k, os.environ.get(k, ''))
        return pattern.sub(repl, s)

    with Path(path).open("r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[len("export "):].lstrip()
            if "=" not in line:
                continue
            key, val = line.split("=", 1)
            key = key.strip()
            val = _strip_inline_comment(val.strip()).strip()
            if (val.startswith('"') and val.endswith('"')) or (val.startswith("'") and val.endswith("'")):
                val = val[1:-1]
            local_env = dict(os.environ)
            local_env.update(loaded)
            val = _expand_vars(val, local_env)
            if key in os.environ and not override:
                continue
            os.environ[key] = val
            loaded[key] = val
    return loaded

# =========================
# Backup logic
# =========================
def run_backup(env_file_path):
    if not Path(env_file_path).exists():
        print(f"❌ {env_file_path} not found!")
        return

    load_env_file(env_file_path)

    # ===== Config =====
    SERVERS = []
    for key, value in os.environ.items():
        if key.startswith("SERVER_") and value.strip():
            SERVERS.append(value.strip())
    if not SERVERS:
        print(f"❌ No servers found in {env_file_path}")
        return

    BACKUP_ROOT = Path(os.getenv("BACKUP_ROOT", "./postgres_backup"))
    LOG_ROOT = Path(os.getenv("LOG_ROOT", str(BACKUP_ROOT / "pg_logs")))
    RETENTION_DAYS = int(os.getenv("RETENTION_DAYS", 7))

    LOG_ROOT.mkdir(parents=True, exist_ok=True)
    BACKUP_ROOT.mkdir(parents=True, exist_ok=True)

    DATE = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    global_log_path = LOG_ROOT / f"backup_run_{DATE}.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.FileHandler(global_log_path), logging.StreamHandler()]
    )

    def run_cmd_stream(cmd, log_file, env=None):
        with open(log_file, "a") as f:
            f.write(f"\n=== Running: {cmd} ===\n")
            f.flush()
            process = subprocess.Popen(
                cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1, env=env
            )
            for line in process.stdout:
                f.write(line)
                f.flush()
            process.wait()
            return process.returncode

    def cleanup_old_files():
        cutoff = datetime.now() - timedelta(days=RETENTION_DAYS)
        for path in BACKUP_ROOT.glob("postgres_*.tar.gz"):
            if path.is_file() and datetime.fromtimestamp(path.stat().st_mtime) < cutoff:
                try:
                    path.unlink()
                    logging.info(f"🗑 Removing old backup {path}")
                except Exception as e:
                    logging.error(f"Failed to remove {path}: {e}")
        for path in LOG_ROOT.glob("*.log"):
            if path.is_file() and datetime.fromtimestamp(path.stat().st_mtime) < cutoff:
                try:
                    path.unlink()
                    logging.info(f"🗑 Removing old log {path}")
                except Exception as e:
                    logging.error(f"Failed to remove log {path}: {e}")

    logging.info(f"=== PostgreSQL Backup Started using {env_file_path} ===")
    summary = {}

    for server_uri in SERVERS:
        parsed = urlparse(server_uri)
        host = parsed.hostname
        port = parsed.port or 5432
        last_octet = host.split('.')[-1]

        server_log = LOG_ROOT / f"backup_{host}_{DATE}.log"
        logging.info(f"➡️ Backing up server: {host}")

        db_index = 1
        success_count, fail_count = 0, 0
        temp_backup_dir = BACKUP_ROOT / f"tmp_backup_{host}_{DATE}"
        temp_backup_dir.mkdir(exist_ok=True)

        while True:
            db_name = os.getenv(f"DB_{db_index}_NAME")
            db_user = os.getenv(f"DB_{db_index}_USER")
            db_pass = os.getenv(f"DB_{db_index}_PASS")
            if not db_name:
                break

            archive_path = temp_backup_dir / f"{db_name}_{DATE}.dump"
            cmd = f'pg_dump -h {host} -p {port} -U {db_user} -F c -Z 9 -f "{archive_path}" "{db_name}"'
            env_cmd = os.environ.copy()
            if db_pass:
                env_cmd["PGPASSWORD"] = db_pass
            rc = run_cmd_stream(cmd, server_log, env=env_cmd)
            if rc == 0 and archive_path.exists() and archive_path.stat().st_size > 0:
                logging.info(f"✅ Backup created: {db_name} ({archive_path.stat().st_size} bytes)")
                success_count += 1
            else:
                logging.error(f"❌ Failed to backup {db_name} (rc={rc})")
                fail_count += 1
            db_index += 1

        if success_count > 0:
            tar_name = f"postgres_{last_octet}_{DATE}.tar.gz"
            tar_path = BACKUP_ROOT / tar_name
            try:
                shutil.make_archive(str(tar_path).replace(".tar.gz", ""), "gztar", temp_backup_dir)
                logging.info(f"✅ Compressed: {tar_path}")
            except Exception as e:
                logging.error(f"❌ Compression failed: {e}")
        else:
            logging.warning(f"⚠️ No successful backups for {host}")

        # پاک کردن فولدر موقت بعد از فشرده سازی
        shutil.rmtree(temp_backup_dir, ignore_errors=True)
        summary[host] = (success_count, fail_count)

    logging.info("➡️ Cleaning up old backups/logs...")
    cleanup_old_files()

    logging.info("=== Backup Summary ===")
    total_success = sum(s for s, f in summary.values())
    total_fail = sum(f for s, f in summary.values())
    for name, (s, f) in summary.items():
        logging.info(f" - {name} → Success: {s}, Fail: {f}")
    logging.info(f"Total → Success: {total_success}, Fail: {total_fail}")
    logging.info("=== Backup Finished ===")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 postgres_backup_v2_fixed.py <env_file_path>")
        exit(1)
    env_file = sys.argv[1]
    run_backup(env_file)
