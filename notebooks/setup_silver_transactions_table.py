# Databricks notebook source
# MAGIC %md
# MAGIC #Setup Silver Transactions Table
# MAGIC

# COMMAND ----------

dbutils.widgets.text(name="catalog", defaultValue='', label='field')
dbutils.widgets.text("schema", defaultValue='', label='field')

table_name = "hsbc.hr.silver_transactions"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE hsbc.hr.silver_transactions 
# MAGIC AS 
# MAGIC   SELECT * EXCEPT(countryOrig, countryDest, t._rescued_data), 
# MAGIC           regexp_replace(countryOrig, "\-\-", "") as countryOrig, 
# MAGIC           regexp_replace(countryDest, "\-\-", "") as countryDest, 
# MAGIC           newBalanceOrig - oldBalanceOrig as diffOrig, 
# MAGIC           newBalanceDest - oldBalanceDest as diffDest
# MAGIC FROM hsbc.hr.transactions t
# MAGIC   LEFT JOIN hsbc.hr.fraud_reports f using(id)
# MAGIC WHERE 1 = 0;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC hsbc.hr.silver_transactions 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM hsbc.hr.silver_transactions;

# COMMAND ----------

# Cleardown
spark.sql(f"DROP TABLE IF EXISTS {table_name}")
