# Databricks notebook source
# MAGIC %md
# MAGIC #Setup Gold Transactions View
# MAGIC

# COMMAND ----------

dbutils.widgets.text(name="catalog", defaultValue='', label='field')
dbutils.widgets.text("schema", defaultValue='', label='field')


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW ${catalog}.${schema}.gold_transactions 
# MAGIC AS 
# MAGIC   SELECT t.* EXCEPT(countryOrig, countryDest, is_fraud), c.* EXCEPT(id),
# MAGIC           boolean(coalesce(is_fraud, 0)) as is_fraud,
# MAGIC           o.alpha3_code as countryOrig, o.country as countryOrig_name, o.long_avg as countryLongOrig_long, o.lat_avg as countryLatOrig_lat,
# MAGIC           d.alpha3_code as countryDest, d.country as countryDest_name, d.long_avg as countryLongDest_long, d.lat_avg as countryLatDest_lat
# MAGIC FROM ${catalog}.${schema}.silver_transactions t
# MAGIC   INNER JOIN ${catalog}.${schema}.country_coordinates o ON t.countryOrig=o.alpha3_code 
# MAGIC   INNER JOIN ${catalog}.${schema}.country_coordinates d ON t.countryDest=d.alpha3_code 
# MAGIC   INNER JOIN ${catalog}.${schema}.banking_customers c ON c.id=t.customer_id 

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC ${catalog}.${schema}.gold_transactions 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM ${catalog}.${schema}.gold_transactions;
