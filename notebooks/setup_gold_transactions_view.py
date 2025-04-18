# Databricks notebook source
# MAGIC %md
# MAGIC #Setup Gold Transactions View
# MAGIC
# MAGIC This setup script works with two sets of catalog and schema:
# MAGIC + `silver_catalog`, `silver_schema` : this is where the `silver_transactions` table is located
# MAGIC + `reference_catalog`, `reference_schema` ": this is where the reference tables `banking_customers` and `country_coordinates` are located.
# MAGIC
# MAGIC The `transactions_gold` view is created in the same schema (database) as the `silver_transactions` table.
# MAGIC

# COMMAND ----------

dbutils.widgets.text(name="catalog", defaultValue='', label='field')
dbutils.widgets.text("schema", defaultValue='', label='field')



# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG ${catalog};
# MAGIC USE SCHEMA ${schema};
# MAGIC
# MAGIC CREATE OR REPLACE VIEW gold_transactions 
# MAGIC AS 
# MAGIC   SELECT t.* EXCEPT(amount, countryOrig, countryDest, is_fraud), c.* EXCEPT(id),
# MAGIC           boolean(coalesce(is_fraud, 0)) as is_fraud,
# MAGIC           CAST(t.amount AS DOUBLE) as amount,
# MAGIC           o.alpha3_code as countryOrig, o.country as countryOrig_name, o.long_avg as countryLongOrig_long, o.lat_avg as countryLatOrig_lat,
# MAGIC           d.alpha3_code as countryDest, d.country as countryDest_name, d.long_avg as countryLongDest_long, d.lat_avg as countryLatDest_lat
# MAGIC FROM silver_transactions t
# MAGIC   INNER JOIN country_coordinates o ON t.countryOrig=o.alpha3_code 
# MAGIC   INNER JOIN country_coordinates d ON t.countryDest=d.alpha3_code 
# MAGIC   INNER JOIN banking_customers c ON c.id=t.customer_id

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC ${catalog}.${schema}.gold_transactions 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM ${catalog}.${schema}.gold_transactions;
# MAGIC
# MAGIC
# MAGIC
# MAGIC
