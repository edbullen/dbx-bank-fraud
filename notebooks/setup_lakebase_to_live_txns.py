# Databricks notebook source
# MAGIC %md
# MAGIC # Lakebase -> Lakehouse Sync setup for live transactions
# MAGIC
# MAGIC This notebook helps configure Lakehouse Sync for the Lakebase table
# MAGIC `public.scored_transactions`, then creates a Delta **view**
# MAGIC `live_transactions` in Unity Catalog using the synced CDC history.
# MAGIC
# MAGIC ## What this notebook does
# MAGIC - Validates Lakebase connectivity using OAuth credentials
# MAGIC - Verifies the source table exists
# MAGIC - Sets `REPLICA IDENTITY FULL` on the source table
# MAGIC - Checks Lakehouse Sync status from `wal2delta.tables`
# MAGIC - Creates or refreshes Delta view `live_transactions` from
# MAGIC   `lb_scored_transactions_history`
# MAGIC
# MAGIC ## Prerequisites and constraints
# MAGIC - Lakehouse Sync is configured at schema-level and started in Lakebase UI
# MAGIC   (manual step documented below).
# MAGIC - Destination synced table naming is managed by Lakebase and follows:
# MAGIC   `lb_<table_name>_history` (for this flow: `lb_scored_transactions_history`).
# MAGIC - Source tables must be in Lakebase database `databricks_postgres`
# MAGIC   (current beta limitation).
# MAGIC - The identity running this notebook needs:
# MAGIC   - Lakebase project `CAN MANAGE` permissions
# MAGIC   - UC destination permissions: `USE CATALOG`, `USE SCHEMA`, `CREATE VIEW`

# COMMAND ----------

# MAGIC %pip install psycopg2-binary databricks-sdk --quiet --upgrade
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("lakebase_endpoint", "", "Lakebase Endpoint (projects/.../endpoints/primary)")
dbutils.widgets.text("lakebase_host", "", "Lakebase Host")
dbutils.widgets.text("lakebase_database", "databricks_postgres", "Lakebase Database")
dbutils.widgets.text("lakebase_user", "", "Lakebase User (email)")
dbutils.widgets.text("source_schema", "public", "Source Postgres Schema")
dbutils.widgets.text("source_table", "scored_transactions", "Source Postgres Table")
dbutils.widgets.text("uc_catalog", "", "Destination Unity Catalog")
dbutils.widgets.text("uc_schema", "", "Destination Unity Schema")
dbutils.widgets.text("live_table", "live_transactions", "Output View Name")
dbutils.widgets.dropdown("create_or_refresh_live_view", "true", ["true", "false"], "Create/Refresh live_transactions view")

# COMMAND ----------

lakebase_endpoint = dbutils.widgets.get("lakebase_endpoint").strip()
lakebase_host = dbutils.widgets.get("lakebase_host").strip()
lakebase_database = dbutils.widgets.get("lakebase_database").strip() or "databricks_postgres"
lakebase_user = dbutils.widgets.get("lakebase_user").strip()
source_schema = dbutils.widgets.get("source_schema").strip() or "public"
source_table = dbutils.widgets.get("source_table").strip() or "scored_transactions"
uc_catalog = dbutils.widgets.get("uc_catalog").strip()
uc_schema = dbutils.widgets.get("uc_schema").strip()
live_table = dbutils.widgets.get("live_table").strip() or "live_transactions"
create_or_refresh_live_view = dbutils.widgets.get("create_or_refresh_live_view").lower() == "true"

assert lakebase_endpoint, "lakebase_endpoint is required"
assert lakebase_host, "lakebase_host is required"
assert lakebase_user, "lakebase_user is required"
assert uc_catalog, "uc_catalog is required"
assert uc_schema, "uc_schema is required"

synced_history_table = f"lb_{source_table}_history"
fully_qualified_history = f"`{uc_catalog}`.`{uc_schema}`.`{synced_history_table}`"
fully_qualified_live = f"`{uc_catalog}`.`{uc_schema}`.`{live_table}`"

print("Lakebase endpoint:", lakebase_endpoint)
print("Lakebase host:", lakebase_host)
print("Source table:", f"{source_schema}.{source_table}")
print("Expected synced history table:", fully_qualified_history)
print("Output current-state view:", fully_qualified_live)

# COMMAND ----------

from databricks.sdk import WorkspaceClient
import psycopg2

w = WorkspaceClient()
cred = w.postgres.generate_database_credential(endpoint=lakebase_endpoint)

conn = psycopg2.connect(
    host=lakebase_host,
    port=5432,
    dbname=lakebase_database,
    user=lakebase_user,
    password=cred.token,
    sslmode="require",
)
conn.autocommit = True
print("Connected to Lakebase. Token expiry:", cred.expire_time)

# COMMAND ----------

# Verify source table exists.
with conn.cursor() as cur:
    cur.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = %s
          AND table_name = %s
        """,
        (source_schema, source_table),
    )
    exists = cur.fetchone() is not None

assert exists, f"Source table not found: {source_schema}.{source_table}"
print("Source table exists.")

# COMMAND ----------

# Set REPLICA IDENTITY FULL (required for Lakehouse Sync).
with conn.cursor() as cur:
    cur.execute(f'ALTER TABLE "{source_schema}"."{source_table}" REPLICA IDENTITY FULL;')

print(f"Set REPLICA IDENTITY FULL on {source_schema}.{source_table}")

# COMMAND ----------

# Verify replica identity state.
with conn.cursor() as cur:
    cur.execute(
        """
        SELECT n.nspname AS table_schema,
               c.relname AS table_name,
               CASE c.relreplident
                 WHEN 'd' THEN 'default'
                 WHEN 'n' THEN 'nothing'
                 WHEN 'f' THEN 'full'
                 WHEN 'i' THEN 'index'
               END AS replica_identity
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind = 'r'
          AND n.nspname = %s
          AND c.relname = %s
        """,
        (source_schema, source_table),
    )
    row = cur.fetchone()

print("Replica identity status:", row)
assert row and row[2] == "full", "Replica identity is not FULL"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Manual UI step required (one-time setup)
# MAGIC
# MAGIC Starting Lakehouse Sync is currently a Lakebase UI operation.
# MAGIC
# MAGIC 1. Open **Lakebase Postgres** in Databricks.
# MAGIC 2. Select your project and branch (typically `production`).
# MAGIC 3. Go to **Branch overview -> Lakehouse sync**.
# MAGIC 4. Click **Start sync** and configure:
# MAGIC    - **Database**: `databricks_postgres`
# MAGIC    - **Source schema**: value of `source_schema` widget
# MAGIC    - **To Catalog**: value of `uc_catalog` widget
# MAGIC    - **To Schema**: value of `uc_schema` widget
# MAGIC 5. Confirm status shows **Syncing**.
# MAGIC
# MAGIC After sync starts, Lakebase creates/updates:
# MAGIC `lb_<table_name>_history` in your chosen UC destination.

# COMMAND ----------

# Optional status check from Lakebase metadata.
try:
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM wal2delta.tables;")
        rows = cur.fetchall()
    if rows:
        print("wal2delta.tables rows:")
        for r in rows:
            print(r)
    else:
        print("No rows in wal2delta.tables yet. If sync was just enabled, wait and rerun.")
except Exception as e:
    print("Could not query wal2delta.tables yet:", e)

# COMMAND ----------

# Check whether synced history table is visible in Unity Catalog.
history_exists = spark.catalog.tableExists(f"{uc_catalog}.{uc_schema}.{synced_history_table}")
print("Synced history table exists:", history_exists)
if not history_exists:
    print("Expected table not found yet. Ensure UI sync is started and source table has rows.")

# COMMAND ----------

# Create or refresh current-state view from synced CDC history.
# This view always reflects the latest synced state (subject to sync latency).
# It derives the latest row per id using _lsn and excludes deletes.
if create_or_refresh_live_view:
    spark.sql(
        f"""
        CREATE OR REPLACE VIEW {fully_qualified_live}
        AS
        SELECT * EXCEPT (rn)
        FROM (
          SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY id ORDER BY _lsn DESC) AS rn
          FROM {fully_qualified_history}
          WHERE _change_type IN ('insert', 'update_postimage', 'delete')
        )
        WHERE rn = 1
          AND _change_type != 'delete'
        """
    )
    print(f"Created/refreshed view {fully_qualified_live}")
else:
    print("Skipped view creation (create_or_refresh_live_view=false)")

# COMMAND ----------

# Quick row counts.
if spark.catalog.tableExists(f"{uc_catalog}.{uc_schema}.{synced_history_table}"):
    c1 = spark.sql(f"SELECT COUNT(*) AS c FROM {fully_qualified_history}").collect()[0][0]
    print(f"{fully_qualified_history} rows: {c1}")

if spark.catalog.tableExists(f"{uc_catalog}.{uc_schema}.{live_table}"):
    c2 = spark.sql(f"SELECT COUNT(*) AS c FROM {fully_qualified_live}").collect()[0][0]
    print(f"{fully_qualified_live} rows: {c2}")

# COMMAND ----------

conn.close()
print("Done.")
