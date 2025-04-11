# Databricks notebook source
# MAGIC %md
# MAGIC #Setup Silver Transactions Table
# MAGIC
# MAGIC + `silver_transactions` is created in the Unity Catalog location `<catalog>`.`<schema>`  
# MAGIC + `<transactions_table>` (the name is configurable) is sourced from Unity Catalog location `<source_catalog>`.`<source_schema>`  
# MAGIC + `<transaction_table>` is joined with `fraud_transactions` which needs to be created in advance in `<catalog>`.`<schema>`    

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

source_catalog = dbutils.widgets.get("source_catalog")
source_schema = catalog = dbutils.widgets.get("source_schema")

transactions_table = dbutils.widgets.get("transactions_table")


# COMMAND ----------

catalog="bank"

# COMMAND ----------

table_name = f"{catalog}.{schema}.{transactions_table}"
print(table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${catalog}.${schema}.silver_transactions 
# MAGIC AS 
# MAGIC   SELECT * EXCEPT(countryOrig, countryDest, t._rescued_data), 
# MAGIC           regexp_replace(countryOrig, "\-\-", "") as countryOrig, 
# MAGIC           regexp_replace(countryDest, "\-\-", "") as countryDest, 
# MAGIC           to_number(newBalanceOrig, '999999.99') - to_number(oldBalanceOrig, '999999.99') as diffOrig, 
# MAGIC           to_number(newBalanceDest, '999999.99') - to_number(oldBalanceDest, '999999.99') as diffDest
# MAGIC FROM ${source_catalog}.${source_schema}.${transactions_table} t
# MAGIC   LEFT JOIN ${catalog}.${schema}.fraud_reports f using(id)
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
