#!/usr/bin/env python3
"""
fakedata_insertion_debug.py

Like previous script but:
- Logs SQL exceptions
- Commits each successful insert
- Keeps inserting until target number of rows reached (with attempt cap)
- Uses ON CONFLICT DO NOTHING to avoid failing on unique constraint collisions
"""

import os
from pathlib import Path
from dotenv import load_dotenv
import psycopg2
from psycopg2 import sql, errors
from faker import Faker
import secrets
import time
import sys
import traceback

# Load .env if present
env_path = Path(".env")
if env_path.exists():
    load_dotenv(env_path)

PG_ADMIN_HOST = os.getenv("PG_ADMIN_HOST", "172.16.61.155")
PG_ADMIN_PORT = int(os.getenv("PG_ADMIN_PORT", "5432"))
PG_ADMIN_DB = os.getenv("PG_ADMIN_DB", "postgres")
PG_ADMIN_USER = os.getenv("PG_ADMIN_USER", "postgres")
PG_ADMIN_PASS = os.getenv("PG_ADMIN_PASS", "")

DBS_TO_CREATE = [
    {"name": "app_db_1", "owner_prefix": "app1", "rows": 100},
    {"name": "app_db_2", "owner_prefix": "app2", "rows": 50},
    {"name": "analytics_db", "owner_prefix": "anl", "rows": 200},
]

fake = Faker()
Faker.seed(1234)

def admin_connect(dbname=PG_ADMIN_DB):
    return psycopg2.connect(
        host=PG_ADMIN_HOST,
        port=PG_ADMIN_PORT,
        dbname=dbname,
        user=PG_ADMIN_USER,
        password=PG_ADMIN_PASS,
        connect_timeout=10
    )

def ensure_table(cur):
    cur.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id serial PRIMARY KEY,
        full_name text NOT NULL,
        email text NOT NULL UNIQUE,
        username text NOT NULL UNIQUE,
        bio text,
        created_at timestamp default now()
    );
    """)

def seed_fake_data(dbname, rows=100, max_attempts_per_row=10):
    conn = admin_connect(dbname)
    cur = conn.cursor()
    # We'll manage transactions manually to ensure commit on success
    conn.autocommit = False

    ensure_table(cur)
    conn.commit()
    print(f"[ok] ensured table 'users' exists in {dbname}.")

    inserted = 0
    attempts = 0
    max_total_attempts = rows * max_attempts_per_row
    while inserted < rows and attempts < max_total_attempts:
        attempts += 1
        try:
            # generate values
            name = fake.name()
            # use simple random email/username (not relying solely on faker.unique across runs)
            # to reduce chance of exhausting unique generator across multiple dbs/runs.
            email = f"{name.lower().replace(' ','_')}_{secrets.token_hex(3)}@example.com"
            username = f"user_{secrets.token_hex(4)}"
            bio = fake.sentence(nb_words=12)

            # Use ON CONFLICT DO NOTHING to avoid unique violations stopping us
            cur.execute(
                """
                INSERT INTO users (full_name, email, username, bio)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (email, username) DO NOTHING
                RETURNING id;
                """,
                (name, email, username, bio)
            )
            res = cur.fetchone()
            if res:
                conn.commit()
                inserted += 1
                if inserted % 10 == 0 or inserted == rows:
                    print(f"[ok] {inserted}/{rows} inserted into {dbname} (attempts {attempts}).")
            else:
                # conflict happened (no row returned)
                conn.rollback()
                # continue trying until we reach desired rows
        except Exception as e:
            # rollback this transaction, log error, and continue
            try:
                conn.rollback()
            except Exception:
                pass
            print(f"[error] insert failed on attempt {attempts} for db {dbname}: {e}")
            # print stack for debugging
            traceback.print_exc()
            # small sleep to avoid tight error loops in some rare cases
            time.sleep(0.05)
    cur.close()
    conn.close()
    print(f"[result] finished {dbname}: inserted {inserted} rows in {attempts} attempts (max attempts {max_total_attempts}).")
    return inserted

def main():
    # Make sure dependencies are reachable
    try:
        conn = admin_connect()
        conn.autocommit = True
        cur = conn.cursor()
    except Exception as e:
        print(f"Cannot connect to admin DB: {e}")
        traceback.print_exc()
        sys.exit(1)

    results = []
    for spec in DBS_TO_CREATE:
        db_name = spec["name"]
        owner_prefix = spec.get("owner_prefix", db_name)
        rows = spec.get("rows", 100)

        username = f"{owner_prefix}_user"
        password = secrets.token_urlsafe(12)

        print("\n---")
        print(f"Processing DB: {db_name}  user: {username}")

        # Create role if not exists
        cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s;", (username,))
        if cur.fetchone() is None:
            cur.execute(sql.SQL("CREATE ROLE {} WITH LOGIN PASSWORD %s;").format(sql.Identifier(username)), [password])
            print(f"[ok] created role {username}")
        else:
            print(f"[skip] role '{username}' already exists.")

        # Create database if not exists
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (db_name,))
        if cur.fetchone() is None:
            cur.execute(sql.SQL("CREATE DATABASE {} OWNER {};").format(sql.Identifier(db_name), sql.Identifier(username)))
            print(f"[ok] created database '{db_name}' owner '{username}'.")
        else:
            print(f"[skip] database '{db_name}' already exists.")

        # Grant privileges
        cur.execute(sql.SQL("GRANT ALL PRIVILEGES ON DATABASE {} TO {};").format(sql.Identifier(db_name), sql.Identifier(username)))

        # Seed data (connect as admin)
        inserted = seed_fake_data(db_name, rows=rows)
        results.append({"db": db_name, "user": username, "password": password, "rows_inserted": inserted})

    cur.close()
    conn.close()

    print("\nSummary:")
    for r in results:
        print(f"- DB: {r['db']}, user: {r['user']}, password: {r['password']}, inserted: {r['rows_inserted']}")

if __name__ == "__main__":
    main()
