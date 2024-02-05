# Databricks notebook source
# MAGIC %md
# MAGIC #Load the Transactions Table from UC Storage Volume
# MAGIC
# MAGIC Auto Loader (Cloud Files) loads from the `<catalog>/<schema>/bank_transactions` volume

# COMMAND ----------

dbutils.widgets.text(name="catalog", defaultValue='', label='field')
dbutils.widgets.text("schema", defaultValue='', label='field')

# COMMAND ----------

# Get parameters from job parameters
#job_params = dbutils.notebook.entry_point.getCurrentBindings()
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

# Import functions
from pyspark.sql.functions import col, current_timestamp

# Define variables used in code below
file_path = f"/Volumes/{catalog}/{schema}/bank_transactions/"
table_name = f"{catalog}.{schema}.transactions"

#username = spark.sql("SELECT regexp_replace(current_user(), '[^a-zA-Z0-9]', '_')").first()[0]
checkpoint_path = f"/tmp/{catalog}/{schema}/transactions/_checkpoint/"

# Configure Auto Loader to ingest CSV data to a Delta table
(spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "csv")
  .option("header", "true")
  .option("cloudFiles.schemaLocation", checkpoint_path)
  .load(file_path)
  .select("*", col("_metadata.file_path").alias("source_file"), current_timestamp().alias("processing_time"))
  .writeStream
  .option("checkpointLocation", checkpoint_path)
  .trigger(availableNow=True)
  .toTable(table_name))

# COMMAND ----------


