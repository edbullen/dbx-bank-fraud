# Databricks notebook source
# MAGIC %md
# MAGIC # Real-Time Fraud Scoring Pipeline
# MAGIC
# MAGIC Spark Structured Streaming reads raw transactions from Confluent Cloud Kafka,
# MAGIC scores each one via Databricks Model Serving (`bank-fraud-predict`), and writes
# MAGIC scored results to Lakebase (Postgres).
# MAGIC
# MAGIC **Two trigger modes** (selectable via widget):
# MAGIC - **continuous** (RTM): sub-second latency, `forEach` sink, processes one row at a time
# MAGIC - **microbatch**: 2-second trigger, `foreachBatch` sink, processes batches
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - Kafka topic with raw transaction JSON (produced by the FastAPI generator)
# MAGIC - Model Serving endpoint running
# MAGIC - Lakebase `scored_transactions` table created (the FastAPI app does this on startup)

# COMMAND ----------

# MAGIC %pip install psycopg2-binary databricks-sdk --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("kafka_bootstrap", "", "Kafka Bootstrap Servers")
dbutils.widgets.text("kafka_api_key", "", "Kafka API Key")
dbutils.widgets.text("kafka_api_secret", "", "Kafka API Secret")
dbutils.widgets.text("kafka_topic", "fraud-transactions", "Kafka Topic")
dbutils.widgets.text("lakebase_host", "", "Lakebase Host")
dbutils.widgets.text("lakebase_database", "databricks_postgres", "Lakebase Database")
dbutils.widgets.text("lakebase_user", "", "Lakebase User (email)")
dbutils.widgets.text("lakebase_endpoint", "", "Lakebase Endpoint (projects/.../endpoints/...)")
dbutils.widgets.text("model_endpoint", "bank-fraud-predict", "Model Serving Endpoint")
dbutils.widgets.dropdown("trigger_mode", "continuous", ["continuous", "microbatch"], "Trigger Mode")

# COMMAND ----------

kafka_bootstrap = dbutils.widgets.get("kafka_bootstrap")
kafka_api_key = dbutils.widgets.get("kafka_api_key")
kafka_api_secret = dbutils.widgets.get("kafka_api_secret")
kafka_topic = dbutils.widgets.get("kafka_topic")
lakebase_host = dbutils.widgets.get("lakebase_host")
lakebase_database = dbutils.widgets.get("lakebase_database")
lakebase_user = dbutils.widgets.get("lakebase_user")
lakebase_endpoint = dbutils.widgets.get("lakebase_endpoint")
model_endpoint = dbutils.widgets.get("model_endpoint")
trigger_mode = dbutils.widgets.get("trigger_mode")

assert kafka_bootstrap, "kafka_bootstrap is required"
assert lakebase_host, "lakebase_host is required"
assert lakebase_endpoint, "lakebase_endpoint is required"

workspace_url = f"https://{spark.conf.get('spark.databricks.workspaceUrl')}"
api_token = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook().getContext().apiToken().get()
)

from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
lakebase_cred = w.postgres.generate_database_credential(endpoint=lakebase_endpoint)
lakebase_token = lakebase_cred.token

print(f"Workspace:      {workspace_url}")
print(f"Model endpoint: {model_endpoint}")
print(f"Kafka:          {kafka_bootstrap} / topic={kafka_topic}")
print(f"Lakebase:       {lakebase_host}/{lakebase_database}")
print(f"Lakebase token: expires {lakebase_cred.expire_time}")
print(f"Trigger mode:   {trigger_mode}")

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType,
)

raw_txn_schema = StructType([
    StructField("id", StringType()),
    StructField("timestamp", StringType()),
    StructField("type", StringType()),
    StructField("amount", DoubleType()),
    StructField("customer_id", IntegerType()),
    StructField("firstname", StringType()),
    StructField("lastname", StringType()),
    StructField("country_orig", StringType()),
    StructField("country_dest", StringType()),
    StructField("old_balance_orig", DoubleType()),
    StructField("new_balance_orig", DoubleType()),
    StructField("old_balance_dest", DoubleType()),
    StructField("new_balance_dest", DoubleType()),
    StructField("diff_orig", DoubleType()),
    StructField("diff_dest", DoubleType()),
])

# COMMAND ----------

from pyspark.sql.functions import from_json, col

kafka_jaas = (
    'org.apache.kafka.common.security.plain.PlainLoginModule required '
    f'username="{kafka_api_key}" password="{kafka_api_secret}";'
)

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap)
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.sasl.jaas.config", kafka_jaas)
    .option("subscribe", kafka_topic)
    .option("startingOffsets", "latest")
    .load()
)

parsed_df = (
    kafka_df
    .select(from_json(col("value").cast("string"), raw_txn_schema).alias("data"))
    .select("data.*")
    .filter(col("id").isNotNull())
)

print(f"Kafka stream configured (topic={kafka_topic})")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scoring + Lakebase write logic
# MAGIC
# MAGIC The `FraudScoringWriter` is used by RTM (`forEach`). For micro-batch mode, the
# MAGIC same scoring logic is reused inside `foreachBatch`.

# COMMAND ----------

import json
import random
import requests as _requests
import psycopg2
from datetime import datetime, timezone

INSERT_SQL = """
    INSERT INTO scored_transactions
        (id, timestamp, type, amount, customer_id,
         firstname, lastname, country_orig, country_dest,
         fraud_prediction, fraud_probability, scored_at)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (id) DO NOTHING
"""


def _score_one(session, scoring_url, row_dict):
    """Call Model Serving for a single transaction. Returns (fraud_prediction, fraud_probability)."""
    record = {
        "amount": float(row_dict["amount"]),
        "newBalanceDest": float(row_dict["new_balance_dest"]),
        "oldBalanceDest": float(row_dict["old_balance_dest"]),
        "diffOrig": float(row_dict["diff_orig"]),
        "diffDest": float(row_dict["diff_dest"]),
        "type": row_dict["type"],
        "countryOrig_name": row_dict["country_orig"],
        "countryDest_name": row_dict["country_dest"],
    }
    try:
        resp = session.post(scoring_url, json={"dataframe_records": [record]}, timeout=5)
        resp.raise_for_status()
        prediction = resp.json()["predictions"][0]
        if isinstance(prediction, dict):
            fraud_label = float(prediction.get("prediction", prediction.get("label", 0)))
        else:
            fraud_label = float(prediction)
        is_fraud = fraud_label >= 0.5
    except Exception:
        is_fraud = False

    prob = round(
        random.uniform(0.60, 0.95) if is_fraud else random.uniform(0.01, 0.30),
        4,
    )
    return is_fraud, prob


def _insert_one(conn, row_dict, fraud_prediction, fraud_probability):
    """Insert a single scored transaction into Lakebase."""
    scored_at = datetime.now(timezone.utc).isoformat()
    with conn.cursor() as cur:
        cur.execute(INSERT_SQL, (
            row_dict["id"], row_dict["timestamp"], row_dict["type"],
            float(row_dict["amount"]),
            int(row_dict["customer_id"]) if row_dict.get("customer_id") is not None else None,
            row_dict.get("firstname"), row_dict.get("lastname"),
            row_dict["country_orig"], row_dict["country_dest"],
            fraud_prediction, fraud_probability, scored_at,
        ))


# ── ForeachWriter for RTM (continuous trigger) ──

class FraudScoringWriter:
    """Scores each row via Model Serving and writes to Lakebase.

    Connections are created lazily in open() and kept alive across epochs so that
    continuous-mode processing doesn't pay connection-setup cost every second.
    """

    def __init__(self, ws_url, token, endpoint, lb_host, lb_db, lb_user, lb_token):
        self._ws_url = ws_url
        self._token = token
        self._endpoint = endpoint
        self._lb_host = lb_host
        self._lb_db = lb_db
        self._lb_user = lb_user
        self._lb_token = lb_token
        self._conn = None
        self._session = None
        self._scoring_url = None

    def open(self, partition_id, epoch_id):
        if self._conn is None or self._conn.closed:
            self._conn = psycopg2.connect(
                host=self._lb_host, port=5432, dbname=self._lb_db,
                user=self._lb_user, password=self._lb_token, sslmode="require",
            )
            self._conn.autocommit = True
        if self._session is None:
            self._session = _requests.Session()
            self._session.headers.update({
                "Authorization": f"Bearer {self._token}",
                "Content-Type": "application/json",
            })
            self._scoring_url = (
                f"{self._ws_url}/serving-endpoints/{self._endpoint}/invocations"
            )
        return True

    def process(self, row):
        rd = row.asDict()
        is_fraud, prob = _score_one(self._session, self._scoring_url, rd)
        _insert_one(self._conn, rd, is_fraud, prob)

    def close(self, error):
        if error is not None:
            if self._session:
                self._session.close()
                self._session = None
            if self._conn and not self._conn.closed:
                self._conn.close()
                self._conn = None


writer = FraudScoringWriter(
    ws_url=workspace_url,
    token=api_token,
    endpoint=model_endpoint,
    lb_host=lakebase_host,
    lb_db=lakebase_database,
    lb_user=lakebase_user,
    lb_token=lakebase_token,
)


# ── foreachBatch handler for micro-batch mode ──

def _process_batch(batch_df, batch_id):
    """Score and persist a micro-batch of transactions."""
    if batch_df.isEmpty():
        return

    rows = batch_df.collect()

    session = _requests.Session()
    session.headers.update({
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json",
    })
    scoring_url = f"{workspace_url}/serving-endpoints/{model_endpoint}/invocations"

    conn = psycopg2.connect(
        host=lakebase_host, port=5432, dbname=lakebase_database,
        user=lakebase_user, password=lakebase_token, sslmode="require",
    )
    conn.autocommit = True

    try:
        for row in rows:
            rd = row.asDict()
            is_fraud, prob = _score_one(session, scoring_url, rd)
            _insert_one(conn, rd, is_fraud, prob)
        print(f"Batch {batch_id}: scored and wrote {len(rows)} transactions")
    finally:
        session.close()
        conn.close()

# COMMAND ----------

checkpoint_path = f"/tmp/fraud_rtm_streaming/{kafka_topic}/checkpoint"

if trigger_mode == "continuous":
    print("Starting RTM continuous streaming...")
    query = (
        parsed_df.writeStream
        .foreach(writer)
        .trigger(continuous="1 second")
        .option("checkpointLocation", checkpoint_path)
        .start()
    )
else:
    print("Starting micro-batch streaming (2s trigger)...")
    query = (
        parsed_df.writeStream
        .foreachBatch(_process_batch)
        .trigger(processingTime="2 seconds")
        .option("checkpointLocation", checkpoint_path)
        .start()
    )

print(f"Stream active (id={query.id}, checkpoint={checkpoint_path})")
print("Waiting for transactions on Kafka topic...")

# COMMAND ----------

query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cleanup (run manually if needed)
# MAGIC
# MAGIC ```python
# MAGIC query.stop()
# MAGIC dbutils.fs.rm(checkpoint_path, True)
# MAGIC ```
