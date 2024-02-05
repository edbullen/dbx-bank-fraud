# Databricks notebook source
# MAGIC %md
# MAGIC # Execute the Silver Transactions table-load job

# COMMAND ----------

# import the ETL functions
from etl import data_load

# get Job parameters - ** these can be set in the Databricks Job Configuration **
dbutils.widgets.text(name="catalog", defaultValue='', label='field')
dbutils.widgets.text("schema", defaultValue='', label='field')

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")



# COMMAND ----------

# run the job to merge new transactions in joined with Fraud reports
data_load.load_silver_transactions(spark, catalog=catalog, schema=schema)
