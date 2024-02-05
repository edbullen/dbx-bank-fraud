# Databricks notebook source
# MAGIC %md
# MAGIC # Execute the Transactions file-load job

# COMMAND ----------

# import the ETL functions
from etl import data_load

# get Job parameters - ** these can be set in the Databricks Job Configuration **
dbutils.widgets.text(name="catalog", defaultValue='', label='field')
dbutils.widgets.text("schema", defaultValue='', label='field')
dbutils.widgets.text("source_folder", defaultValue='', label='field')
dbutils.widgets.text("target_table", defaultValue='', label='field')
dbutils.widgets.text("format_type", defaultValue='', label='field')

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
source_folder = dbutils.widgets.get("source_folder")
target_table = dbutils.widgets.get("target_table")
format_type = dbutils.widgets.get("format_type")


# COMMAND ----------

# run autoloader job to check for new files
data_load.auto_loader(spark
                     , catalog=catalog
                     , schema=schema
                     , source_folder=source_folder
                     , target_table=target_table
                     , format_type=format_type)
