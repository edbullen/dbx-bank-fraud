# Databricks notebook source
# MAGIC %md
# MAGIC # Create UC function: explain_transaction_risk

# COMMAND ----------

# DBTITLE 1,Configure widgets (same pattern as fraud_model_deploy)
dbutils.widgets.text("unity_catalog", "default_catalog", "Unity Catalog")
dbutils.widgets.text("unity_schema", "default_schema", "Unity Schema")

unity_catalog = dbutils.widgets.get("unity_catalog")
unity_schema = dbutils.widgets.get("unity_schema")

print(f"Unity Catalog: {unity_catalog}, Unity Schema: {unity_schema}")
spark.sql(f"USE `{unity_catalog}`.`{unity_schema}`")

# Fully qualified gold_transactions (backticks for hyphenated catalog/schema names)
_gold = f"`{unity_catalog}`.`{unity_schema}`.gold_transactions"

# COMMAND ----------

# DBTITLE 1,Create the UC function
_create_fn = """
CREATE OR REPLACE FUNCTION explain_transaction_risk(tx_id STRING)
RETURNS TABLE (
  risk_score  INT,
  top_signals ARRAY<STRING>,
  explanation STRING,
  features    MAP<STRING, STRING>
)
RETURN
WITH
tx AS (
  SELECT
    COUNT(*)            AS found_count,
    MAX(id)             AS id,
    MAX(customer_id)    AS customer_id,
    MAX(type)           AS type,
    MAX(amount)         AS amount,
    MAX(oldBalanceOrig) AS oldBalanceOrig,
    MAX(newBalanceOrig) AS newBalanceOrig,
    MAX(oldBalanceDest) AS oldBalanceDest,
    MAX(newBalanceDest) AS newBalanceDest
  FROM """ + _gold + """
  WHERE id = tx_id
),
type_stats AS (
  SELECT
    type,
    AVG(CASE WHEN is_fraud THEN 1.0 ELSE 0.0 END) AS fraud_rate_type,
    percentile_approx(amount, 0.50) AS p50_amount_type,
    percentile_approx(amount, 0.90) AS p90_amount_type,
    percentile_approx(amount, 0.95) AS p95_amount_type,
    percentile_approx(amount, 0.99) AS p99_amount_type
  FROM """ + _gold + """
  GROUP BY type
),
feat AS (
  SELECT
    t.*,
    ts.fraud_rate_type,
    ts.p50_amount_type,
    ts.p90_amount_type,
    ts.p95_amount_type,
    ts.p99_amount_type,
    CASE WHEN COALESCE(ts.fraud_rate_type, 0.0) >= 0.10 THEN 1 ELSE 0 END AS flag_type_prior,
    CASE WHEN t.amount IS NOT NULL AND ts.p95_amount_type IS NOT NULL AND t.amount >= ts.p95_amount_type THEN 1 ELSE 0 END AS flag_large_p95,
    CASE WHEN t.amount IS NOT NULL AND ts.p99_amount_type IS NOT NULL AND t.amount >= ts.p99_amount_type THEN 1 ELSE 0 END AS flag_large_p99,
    CASE
      WHEN t.oldBalanceOrig IS NOT NULL AND t.amount IS NOT NULL AND t.newBalanceOrig IS NOT NULL
       AND t.oldBalanceOrig > 0
       AND t.amount >= 0.90 * t.oldBalanceOrig
       AND t.newBalanceOrig <= 0.10 * t.oldBalanceOrig
      THEN 1 ELSE 0
    END AS flag_drains_origin,
    CASE
      WHEN t.oldBalanceDest IS NOT NULL AND t.amount IS NOT NULL
       AND t.oldBalanceDest = 0
       AND t.amount >= COALESCE(ts.p50_amount_type, 1000)
      THEN 1 ELSE 0
    END AS flag_empty_dest
  FROM tx t
  LEFT JOIN type_stats ts
    ON ts.type = t.type
),
scored AS (
  SELECT
    *,
    CASE
      WHEN found_count = 0 THEN 0
      ELSE LEAST(
        100,
        (flag_type_prior   * 30)
      + (flag_drains_origin * 30)
      + (flag_empty_dest  * 20)
      + (CASE WHEN flag_large_p99 = 1 THEN 20
              WHEN flag_large_p95 = 1 THEN 10
              ELSE 0 END)
      )
    END AS risk_score
  FROM feat
),
signals AS (
  SELECT
    risk_score,
    CASE
      WHEN found_count = 0 THEN ARRAY('Transaction id not found.')
      ELSE FILTER(
        ARRAY(
          CASE WHEN flag_type_prior = 1 THEN
            CONCAT('Transaction type ', CAST(type AS STRING),
                   ' has an elevated fraud rate (', CAST(ROUND(fraud_rate_type * 100, 1) AS STRING), '%).')
          END,
          CASE WHEN flag_drains_origin = 1 THEN
            'This transaction drains most of the origin account balance.'
          END,
          CASE WHEN flag_empty_dest = 1 THEN
            'Funds were sent to a destination account with no prior balance.'
          END,
          CASE WHEN flag_large_p99 = 1 THEN
            CONCAT('Amount is extreme for this type (>= p99 ≈ ', CAST(ROUND(p99_amount_type, 2) AS STRING), ').')
          END,
          CASE WHEN flag_large_p99 = 0 AND flag_large_p95 = 1 THEN
            CONCAT('Amount is unusually large for this type (>= p95 ≈ ', CAST(ROUND(p95_amount_type, 2) AS STRING), ').')
          END
        ),
        x -> x IS NOT NULL
      )
    END AS top_signals,
    MAP(
      'found_count',          CAST(found_count AS STRING),
      'type',                CAST(type AS STRING),
      'amount',              CAST(amount AS STRING),
      'oldBalanceOrig',      CAST(oldBalanceOrig AS STRING),
      'newBalanceOrig',      CAST(newBalanceOrig AS STRING),
      'oldBalanceDest',      CAST(oldBalanceDest AS STRING),
      'newBalanceDest',      CAST(newBalanceDest AS STRING),
      'fraud_rate_type',     CAST(ROUND(COALESCE(fraud_rate_type, 0.0), 4) AS STRING),
      'p50_amount_type',     CAST(p50_amount_type AS STRING),
      'p90_amount_type',     CAST(p90_amount_type AS STRING),
      'p95_amount_type',     CAST(p95_amount_type AS STRING),
      'p99_amount_type',     CAST(p99_amount_type AS STRING),
      'flag_type_prior',     CAST(flag_type_prior AS STRING),
      'flag_drains_origin',  CAST(flag_drains_origin AS STRING),
      'flag_empty_dest',     CAST(flag_empty_dest AS STRING),
      'flag_large_p95',      CAST(flag_large_p95 AS STRING),
      'flag_large_p99',      CAST(flag_large_p99 AS STRING)
    ) AS features
  FROM scored
)
SELECT
  risk_score,
  CASE
    WHEN SIZE(top_signals) > 0 THEN SLICE(top_signals, 1, 3)
    ELSE ARRAY('No strong red flags from basic rules.')
  END AS top_signals,
  CONCAT(
    'Risk score ', CAST(risk_score AS STRING), '/100. Key factors: ',
    CONCAT_WS(
      ' ',
      CASE
        WHEN SIZE(top_signals) > 0 THEN SLICE(top_signals, 1, 3)
        ELSE ARRAY('No strong red flags from basic rules.')
      END
    )
  ) AS explanation,
  features
FROM signals;
"""
spark.sql(_create_fn)


# COMMAND ----------

# DBTITLE 1,Test the function (sample transaction)
_sample_id = spark.sql("SELECT id FROM gold_transactions LIMIT 1").collect()[0][0]
display(spark.sql(f"SELECT * FROM explain_transaction_risk('{_sample_id}')"))

