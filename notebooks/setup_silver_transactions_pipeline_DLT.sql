-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Setup Silver Transactions DLT Table
-- MAGIC
-- MAGIC + `silver_transactions` is created in the Unity Catalog location `<catalog>`.`<schema>`.  This is determined by the Catalog and Schema setting chosen when configuring the DLT pipeline.  Do not set it in the notebook.
-- MAGIC + `<transactions_volume>` (the name is configurable) is sourced from Unity Catalog location `<source_catalog>`.`<source_schema>`  
-- MAGIC + **Set custom configuration parameters** in the DLT Advanced: Configuration section when creating the pipeline:
-- MAGIC   - `catalog` 
-- MAGIC   -  `schema`  
-- MAGIC + Data is read from the raw volume into table `transactions`
-- MAGIC + `transactions` is joined with `fraud_reports` which is also staged in a raw volume
-- MAGIC
-- MAGIC    
-- MAGIC See [README_DLT.md](../README_DLT.md) for a guide on adding this DLT workflow using the web GUI.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### DLT File format options
-- MAGIC https://docs.gcp.databricks.com/en/ingestion/auto-loader/options.html#format-options

-- COMMAND ----------

-- DBTITLE 1,DLT Streaming Table for landing transactions
CREATE OR REFRESH STREAMING TABLE transactions
AS SELECT
  *,
  _metadata.file_path AS source_file
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

-- DBTITLE 1,DLT Incremental Stream Table for fraud reports
-- Incremental tables are added to but don't update
CREATE INCREMENTAL LIVE TABLE fraud_reports
AS 
  SELECT id, is_fraud, _metadata.file_path AS source_file
  FROM cloud_files("/Volumes/${catalog}/${schema}/fraud_raw", "csv")

-- COMMAND ----------

-- DBTITLE 1,DLT Live Table for combined join of batched Delta Fraud Reports and Streamed Transactions
CREATE OR REPLACE STREAMING TABLE silver_transactions (
  CONSTRAINT correct_data EXPECT (id IS NOT NULL),
  CONSTRAINT correct_customer_id EXPECT (customer_id IS NOT NULL)
  CONSTRAINT correct_amount EXPECT (amount > 0) ON VIOLATION DROP ROW
)
AS
SELECT t.* EXCEPT(countryOrig, countryDest)  , 
       f.is_fraud ,
         regexp_replace(countryOrig, "--", "") as countryOrig, 
         regexp_replace(countryDest, "--", "") as countryDest, 
         newBalanceOrig - oldBalanceOrig as diffOrig, 
         newBalanceDest - oldBalanceDest as diffDest
FROM STREAM(LIVE.transactions) t
--LEFT JOIN ${catalog}.${schema}.fraud_reports f
--  ON t.id = f.id
LEFT JOIN (
  SELECT id, is_fraud 
  FROM live.fraud_reports
) f USING(id)
    

