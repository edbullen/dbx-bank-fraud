# Databricks notebook source
# MAGIC %md
# MAGIC # Execute the Silver Transactions table-load job

# COMMAND ----------

# Databricks notebook source

# import the ETL functions
from etl import data_load

# get Job parameters - ** these can be set in the Databricks Job Configuration **
dbutils.widgets.text(name="catalog", defaultValue='', label='field')
dbutils.widgets.text("schema", defaultValue='', label='field')
dbutils.widgets.text(name="source_catalog", defaultValue='', label='field')
dbutils.widgets.text(name="source_schema", defaultValue='', label='field')
dbutils.widgets.text(name="source_table", defaultValue='transactions', label='field')
dbutils.widgets.text("target_table", defaultValue='', label='field')

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
source_catalog = dbutils.widgets.get("source_catalog")
source_schema = dbutils.widgets.get("source_schema")
target_table = dbutils.widgets.get("target_table")
source_table = dbutils.widgets.get("source_table")


print(f"catalog: {catalog}")
print(f"schema (database): {schema}")
print(f"source_catalog: {source_catalog}")
print(f"source_schema: {source_schema}")
print(f"source_table: {source_table}")
print(f"target_table: {target_table}")


# COMMAND ----------

# run the job to merge new transactions in joined with Fraud reports
data_load.load_silver_transactions(spark, 
                                   catalog=catalog, 
                                   schema=schema,
                                   source_catalog=source_catalog, 
                                   source_schema=source_schema, 
                                   source_table=source_table, 
                                   target_table=target_table)
