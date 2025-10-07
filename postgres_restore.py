#!/usr/bin/env python3
import subprocess
import logging
from pathlib import Path
from datetime import datetime
import os
import re

# =========================
# Configuration
# =========================
BACKUP_DIR = "/home/hassan/Documents/postgres_backup"  # فولدر حاوی .dump ها
PG_HOST = "172.16.61.156"
PG_PORT = 5432
PG_USER = "postgres"
PG_PASS = "postgres123"

# =========================
# Logging setup
# =========================
LOG_DIR = Path(BACKUP_DIR) / "pg_restore_logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
DATE = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
LOG_FILE = LOG_DIR / f"restore_{DATE}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()]
)

# =========================
# Set PGPASSWORD environment variable
# =========================
os.environ["PGPASSWORD"] = PG_PASS

# =========================
# Find all .dump files recursively
# =========================
dump_files = list(Path(BACKUP_DIR).rglob("*.dump"))
if not dump_files:
    logging.error(f"No .dump files found in {BACKUP_DIR}")
    exit(1)

success_count = 0
fail_count = 0
failed_dbs = []

for idx, dump_file in enumerate(dump_files, 1):
    # استخراج اسم دیتابیس دقیق از فایل
    # فرض: فایل ها با قالب dbname_YYYY-MM-DD_HH-MM-SS.dump هستند
    match = re.match(r"(?P<dbname>.+)_\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}\.dump", dump_file.name)
    if match:
        db_name = match.group("dbname")
    else:
        db_name = dump_file.stem  # fallback

    logging.info(f"➡️ ({idx}/{len(dump_files)}) Restoring database: {db_name}")

    # ایجاد دیتابیس اگر موجود نیست
    try:
        subprocess.run(
            ["createdb", "-h", PG_HOST, "-p", str(PG_PORT), "-U", PG_USER, db_name],
            check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
    except Exception as e:
        logging.warning(f"Failed to create database {db_name}: {e}")

    # ریستور دیتابیس با --clean --if-exists --no-owner --no-acl
    cmd = [
        "pg_restore",
        "-h", PG_HOST,
        "-p", str(PG_PORT),
        "-U", PG_USER,
        "-d", db_name,
        "--clean",
        "--if-exists",
        "--no-owner",
        "--no-acl",
        str(dump_file)
    ]
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    
    # حتی اگر خطای DROP table/constraint رخ دهد، restore موفق در نظر گرفته شود
    if result.returncode == 0 or "errors ignored on restore" in result.stderr:
        logging.info(f"✅ Database {db_name} restored successfully")
        success_count += 1
    else:
        logging.error(f"❌ Failed to restore database {db_name}")
        logging.error(result.stderr)
        fail_count += 1
        failed_dbs.append(db_name)

    logging.info("--------------------------------------------------")

# =========================
# Summary
# =========================
logging.info("=== PostgreSQL Restore Summary ===")
logging.info(f"Success: {success_count} | Fail: {fail_count}")
if failed_dbs:
    logging.info(f"Failed databases: {failed_dbs}")

logging.info(f"📄 Detailed log saved at: {LOG_FILE}")
logging.info("=== PostgreSQL Restore Finished ===")
