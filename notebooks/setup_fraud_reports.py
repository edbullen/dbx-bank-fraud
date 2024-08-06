# Databricks notebook source
# MAGIC %md
# MAGIC # Fraud reports setup for V1 and V2 demo examples
# MAGIC
# MAGIC Use this notebook to stage fraud report data directly into the `fraud_reports` Delta table.
# MAGIC   
# MAGIC Do NOT use this notebook if running with the V3 demo version, where fraud report data is staged into a GCS bucket and streamed in with DLT.

# COMMAND ----------

# import the ETL functions
from etl import data_load



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


