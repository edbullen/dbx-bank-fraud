# Databricks notebook source
# MAGIC %md
# MAGIC ## Setup the Reference Tables
# MAGIC
# MAGIC Static data loaded from CSV and JSON data staged in a cloud storage bucket.
# MAGIC
# MAGIC Tables:
# MAGIC + `country_coordinates`  
# MAGIC + `banking_customers`  
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

dbutils.widgets.text(name="catalog", defaultValue='', label='field')
dbutils.widgets.text("schema", defaultValue='', label='field')
dbutils.widgets.text("refdata_bucket_path", defaultValue='', label='field')

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
#refdata_bucket_path = dbutils.widgets.get("refdata_bucket_path")

# COMMAND ----------

# DBTITLE 1,Example - Query a set of CSV files in Volume
# MAGIC %sql
# MAGIC SELECT * EXCEPT (_rescued_data)
# MAGIC FROM read_files(
# MAGIC   '/Volumes/${catalog}/${schema}/staging/country_coordinates',
# MAGIC   format => 'csv',
# MAGIC   header => true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${catalog}.${schema}.country_coordinates
# MAGIC AS 
# MAGIC SELECT * EXCEPT (_rescued_data)
# MAGIC FROM read_files(
# MAGIC   '/Volumes/${catalog}/${schema}/staging/country_coordinates',
# MAGIC   format => 'csv',
# MAGIC   header => true)
# MAGIC   

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * EXCEPT (_rescued_data)
# MAGIC FROM read_files(
# MAGIC   '/Volumes/${catalog}/${schema}/staging/customers_json',
# MAGIC   format => 'json')
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${catalog}.${schema}.banking_customers
# MAGIC AS 
# MAGIC SELECT * EXCEPT (_rescued_data)
# MAGIC FROM read_files(
# MAGIC   '/Volumes/${catalog}/${schema}/staging/customers_json',
# MAGIC   format => 'json')
