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

-- DBTITLE 1,DLT Streaming Table for landing transactions
CREATE OR REFRESH STREAMING TABLE transactions
AS SELECT
  *
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

-- DBTITLE 1,DLT Live Table for combined join of batched Delta Fraud Reports and Streamed Transactions
CREATE OR REPLACE STREAMING TABLE silver_transactions
AS
SELECT t.* EXCEPT(countryOrig, countryDest)  , 
       f.is_fraud,
         regexp_replace(countryOrig, "--", "") as countryOrig, 
         regexp_replace(countryDest, "--", "") as countryDest, 
         newBalanceOrig - oldBalanceOrig as diffOrig, 
         newBalanceDest - oldBalanceDest as diffDest
FROM STREAM(LIVE.transactions) t
LEFT JOIN ${catalog}.${schema}.fraud_reports f
    ON t.id = f.id
