#!/usr/bin/env python3
"""Quick smoke test: verify Lakebase connectivity and OAuth token generation.

Loads config from app/.env, generates an OAuth token via the Databricks SDK,
connects to Lakebase via psycopg2, and runs a simple query.
"""

import os
import sys
from pathlib import Path

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / "app" / ".env")

PROFILE = os.getenv("DATABRICKS_CONFIG_PROFILE", "")
ENDPOINT = os.getenv("LAKEBASE_ENDPOINT", "")
HOST = os.getenv("LAKEBASE_HOST", "")
DATABASE = os.getenv("LAKEBASE_DATABASE", "databricks_postgres")
USER = os.getenv("LAKEBASE_USER", "")

print("=" * 60)
print("Lakebase Smoke Test")
print("=" * 60)
print(f"  Profile:   {PROFILE or '(default)'}")
print(f"  Endpoint:  {ENDPOINT}")
print(f"  Host:      {HOST}")
print(f"  Database:  {DATABASE}")
print(f"  User:      {USER}")
print()

if not HOST or not USER:
    print("ERROR: LAKEBASE_HOST and LAKEBASE_USER must be set in app/.env")
    sys.exit(1)

if not ENDPOINT:
    print("ERROR: LAKEBASE_ENDPOINT must be set for OAuth token generation")
    sys.exit(1)

# ── Step 1: Generate OAuth token ──
print("[1/3] Generating OAuth token via Databricks SDK...")
try:
    from databricks.sdk import WorkspaceClient
    kwargs = {"profile": PROFILE} if PROFILE else {}
    w = WorkspaceClient(**kwargs)
    cred = w.postgres.generate_database_credential(endpoint=ENDPOINT)
    token = cred.token
    print(f"  Token obtained (first 20 chars): {token[:20]}...")
    print(f"  Expires: {cred.expire_time}")
except Exception as e:
    print(f"  FAILED: {e}")
    sys.exit(1)

# ── Step 2: Connect to Lakebase ──
print("\n[2/3] Connecting to Lakebase via psycopg2...")
try:
    import psycopg2
    conn = psycopg2.connect(
        host=HOST,
        port=5432,
        dbname=DATABASE,
        user=USER,
        password=token,
        sslmode="require",
        connect_timeout=10,
    )
    print(f"  Connected to {HOST}/{DATABASE}")
except Exception as e:
    print(f"  FAILED: {e}")
    sys.exit(1)

# ── Step 3: List tables and check for scored_transactions ──
print("\n[3/3] Querying database...")
try:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY table_name
        """)
        tables = [row[0] for row in cur.fetchall()]

    if tables:
        print(f"  Tables found: {', '.join(tables)}")
    else:
        print("  No tables found in public schema (empty database)")

    if "scored_transactions" in tables:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM scored_transactions")
            count = cur.fetchone()[0]
        print(f"  scored_transactions row count: {count}")
    else:
        print("  scored_transactions table not yet created (the app creates it on first startup)")

    conn.close()
except Exception as e:
    print(f"  FAILED: {e}")
    conn.close()
    sys.exit(1)

print("\n" + "=" * 60)
print("SMOKE TEST PASSED -- Lakebase connectivity OK")
print("=" * 60)
