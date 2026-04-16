import os

# --- Kafka (Phase 4) ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "")
KAFKA_API_KEY = os.getenv("KAFKA_API_KEY", "")
KAFKA_API_SECRET = os.getenv("KAFKA_API_SECRET", "")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "fraud-transactions")

# --- Lakebase ---
# Databricks App mode: PGHOST is auto-injected by the platform when a Lakebase
# resource is configured in app.yaml. No manual config needed.
# Local dev mode: set LAKEBASE_ENDPOINT + LAKEBASE_HOST + LAKEBASE_USER in .env
# and ensure DATABRICKS_CONFIG_PROFILE points to the correct workspace.
LAKEBASE_ENDPOINT = os.getenv("LAKEBASE_ENDPOINT", "")
LAKEBASE_HOST = os.getenv("LAKEBASE_HOST", os.getenv("PGHOST", ""))
LAKEBASE_PORT = int(os.getenv("LAKEBASE_PORT", os.getenv("PGPORT", "5432")))
LAKEBASE_DATABASE = os.getenv("LAKEBASE_DATABASE", os.getenv("PGDATABASE", "databricks_postgres"))
LAKEBASE_USER = os.getenv("LAKEBASE_USER", os.getenv("PGUSER", ""))
LAKEBASE_SSLMODE = os.getenv("LAKEBASE_SSLMODE", os.getenv("PGSSLMODE", "require"))

# --- Model Serving ---
MODEL_SERVING_ENDPOINT = os.getenv("MODEL_SERVING_ENDPOINT", "bank-fraud-predict")

# --- Unity Catalog ---
UNITY_CATALOG = os.getenv("UNITY_CATALOG", "")
UNITY_SCHEMA = os.getenv("UNITY_SCHEMA", "")

# --- SQL Warehouse (used by profiling script, not at app runtime) ---
SQL_WAREHOUSE_ID = os.getenv("SQL_WAREHOUSE_ID", "")

# --- Generator tuning ---
FRAUD_INJECTION_RATE = float(os.getenv("FRAUD_INJECTION_RATE", "0.15"))

# --- Mode ---
# If LAKEBASE_HOST is set (either via PGHOST from Databricks App or manually),
# use Lakebase. Otherwise fall back to in-memory mock.
USE_MOCK_DATA = os.getenv("USE_MOCK_DATA", "").lower() == "true"


def is_lakebase_configured() -> bool:
    """Returns True if we have enough config to connect to Lakebase."""
    if USE_MOCK_DATA:
        return False
    return bool(LAKEBASE_HOST and LAKEBASE_USER)


def is_kafka_configured() -> bool:
    """Returns True if Kafka producer should be used instead of inline scoring."""
    return bool(KAFKA_BOOTSTRAP_SERVERS and KAFKA_API_KEY)
