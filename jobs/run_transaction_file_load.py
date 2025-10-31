# Databricks notebook source
# MAGIC %md
# MAGIC # Execute the Transactions file-load job

# COMMAND ----------

# Databricks notebook source

# import the ETL functions
from etl import data_load

# get Job parameters - ** these can be set in the Databricks Job Configuration **
dbutils.widgets.text(name="catalog", defaultValue='', label='field')
dbutils.widgets.text("schema", defaultValue='', label='field')
dbutils.widgets.text("source_folder", defaultValue='', label='field')
dbutils.widgets.text("target_table", defaultValue='', label='field')
dbutils.widgets.text("format_type", defaultValue='', label='field')
dbutils.widgets.text("autoloader", defaultValue='false', label='field')

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
source_folder = dbutils.widgets.get("source_folder")
target_table = dbutils.widgets.get("target_table")
format_type = dbutils.widgets.get("format_type")
autoloader = dbutils.widgets.get("autoloader").lower()

# COMMAND ----------

<<<<<<< Updated upstream
# run autoloader job to check for new files
data_load.auto_loader(spark
                     , catalog=catalog
                     , schema=schema
                     , source_folder=source_folder
                     , target_table=target_table
                     , format_type=format_type)

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC show volumes
=======
if autoloader == 'true':
  # run autoloader job to check for new files
  data_load.auto_loader(spark
                      , catalog=catalog
                      , schema=schema
                      , source_folder=source_folder
                      , target_table=target_table
                      , format_type=format_type)
else:
  # run batch job to load new files
  data_load.transactions_load(spark
                      , catalog=catalog
                      , schema=schema
                      , source_folder=source_folder
                      , target_table=target_table
                      , format_type=format_type)
>>>>>>> Stashed changes
