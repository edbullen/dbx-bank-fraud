# Databricks notebook source
# MAGIC %md
# MAGIC #Setup Silver Transactions Table
# MAGIC

# COMMAND ----------

# DBTITLE 1,Specify the source schema for building the Silver table in the Target schema

# Depending on whether this is working on the Azure ADLS source demo or the Big Query demo - set this to transactions or bronze_transactions
dbutils.widgets.text("transactions_table", defaultValue='', label='field')

dbutils.widgets.text(name="catalog", defaultValue='', label='field')
dbutils.widgets.text("schema", defaultValue='', label='field')
dbutils.widgets.text(name="source_catalog", defaultValue='', label='field')
dbutils.widgets.text("source_schema", defaultValue='', label='field')

catalog = dbutils.widgets.get("catalog")
schema = catalog = dbutils.widgets.get("schema")

catalog = dbutils.widgets.get("source_catalog")
schema = catalog = dbutils.widgets.get("source_schema")

transactions_table = dbutils.widgets.get("transactions_table")


# COMMAND ----------

table_name = f"{catalog}.{schema}.{transactions_table}"

# COMMAND ----------

print(table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${catalog}.${schema}.silver_transactions 
# MAGIC AS 
# MAGIC   SELECT * EXCEPT(countryOrig, countryDest, t._rescued_data), 
# MAGIC           regexp_replace(countryOrig, "\-\-", "") as countryOrig, 
# MAGIC           regexp_replace(countryDest, "\-\-", "") as countryDest, 
# MAGIC           newBalanceOrig - oldBalanceOrig as diffOrig, 
# MAGIC           newBalanceDest - oldBalanceDest as diffDest
# MAGIC FROM ${source_catalog}.${source_schema}.${transactions_table} t
# MAGIC   LEFT JOIN ${source_catalog}.${source_schema}.fraud_reports f using(id)
# MAGIC WHERE 1 = 0;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC ${catalog}.${schema}.silver_transactions 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM ${catalog}.${schema}.silver_transactions;

# COMMAND ----------

# Cleardown
spark.sql(f"DROP TABLE IF EXISTS {table_name}")
