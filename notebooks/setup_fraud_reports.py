# Databricks notebook source
# import the ETL functions
from etl import data_load

#this is a test


# COMMAND ----------

# get Job parameters - ** these can be set in the Databricks Job Configuration **
dbutils.widgets.text(name="catalog", defaultValue='', label='field')
dbutils.widgets.text("schema", defaultValue='', label='field')

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

# URL to pull the data from
dbutils.widgets.text("url", defaultValue='https://github.com/edbullen/dbx-bank-fraud/blob/main/data/fraud_reports/fraud_reports_part_a.csv?raw=true', label='field')
url = dbutils.widgets.get("url")

# COMMAND ----------

print(url)

# COMMAND ----------

# run the job to merge new transactions in joined with Fraud reports
data_load.web_url_pull(spark, catalog, schema, url=url, target_table="fraud_reports", format_type="csv", columns=['is_fraud', 'id'])

# COMMAND ----------


