-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Setup Silver Transactions DLT Table
-- MAGIC
-- MAGIC + `silver_transactions` is created in the Unity Catalog location `<catalog>`.`<schema>`.  This is determined by the Catalog and Schema setting chosen when configuring the DLT pipeline.  Do not set it in the notebook.
-- MAGIC + `<transactions_volume>` (the name is configurable) is sourced from Unity Catalog location `<source_catalog>`.`<source_schema>`  
-- MAGIC + Set the following configuration parameters in the DLT Advanced: Configuration section when creating the pipeline:
-- MAGIC   - `catalog` 
-- MAGIC   -  `schema` 
-- MAGIC   - `transactions_volume`   
-- MAGIC + Data is read from the raw volume into table `transactions`
-- MAGIC + `transactions` is joined with `fraud_reports` which needs to be created in advance in `<catalog>`.`<schema>`   
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### DLT File format options
-- MAGIC https://docs.gcp.databricks.com/en/ingestion/auto-loader/options.html#format-options

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE fraud_reports_stream
USING DELTA
AS SELECT *, current_timestamp() as eventTimeFrd
FROM hsbc.fraud.fraud_reports

-- COMMAND ----------

-- DBTITLE 1,DLT Streaming Table for landing transactions
CREATE OR REFRESH STREAMING TABLE transactions
AS SELECT
  *, current_timestamp() as eventTimeTxn
FROM
  STREAM cloud_files(
    "/Volumes/${catalog}/${schema}/transactions_raw",
    "csv",
    map(
      "header", "true",
      "cloudFiles.inferColumnTypes", "true"
    )
  )

-- COMMAND ----------

CREATE OR REPLACE STREAMING TABLE silver_transactions
AS
SELECT t.* EXCEPT(countryOrig, countryDest, eventTimeTxn),
       f.is_fraud,
       f.eventTimeFrd,
       regexp_replace(countryOrig, "--", "") as countryOrig,
       regexp_replace(countryDest, "--", "") as countryDest,
       newBalanceOrig - oldBalanceOrig as diffOrig,
       newBalanceDest - oldBalanceDest as diffDest
FROM STREAM(LIVE.transactions)
  WATERMARK eventTimeTxn DELAY OF INTERVAL 24 HOURS t
LEFT JOIN STREAM(LIVE.fraud_reports_stream) 
  WATERMARK eventTimeFrd DELAY OF INTERVAL 24 HOURS f
ON t.id = f.id
AND f.eventTimeFrd >= t.eventTimeTxn - interval '24 hours'
AND f.eventTimeFrd <= t.eventTimeTxn
