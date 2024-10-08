# Databricks notebook source
# MAGIC %md
# MAGIC # Clear Down MLmodel, Predictions, Gold and Silver

# COMMAND ----------

dbutils.widgets.text("unity_catalog", "default_catalog", "Unity Catalog")
dbutils.widgets.text("unity_schema", "default_schema", "Unity Schema")
unity_catalog = dbutils.widgets.get("unity_catalog")
unity_schema = dbutils.widgets.get("unity_schema")

print(f"Unity Catalog: {unity_catalog}, Unity Schema: {unity_schema} ")
spark.sql(f"USE {unity_catalog}.{unity_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## START MAIN
# MAGIC - fraud_predictions table
# MAGIC - bank_fraud_predict model and versions
# MAGIC - gold_transactions view

# COMMAND ----------

# DBTITLE 1,Drop the Fraud Predictons Table
# MAGIC %sql
# MAGIC DROP TABLE ${unity_catalog}.${unity_schema}.fraud_predictions;

# COMMAND ----------

# DBTITLE 1,Drop the MLflow Model
import mlflow
from mlflow.tracking import MlflowClient

# work with Unity Catalog registered MLflow models
mlflow.set_registry_uri('databricks-uc') 

# Set the model name
model_name = f"{unity_catalog}.{unity_schema}.bank_fraud_predict"

# Get all versions of the model
client = MlflowClient()
model_versions = client.search_model_versions(f"name = '{model_name}'")

# Delete each model version
for version in model_versions:
    version_num = str(version.version)  # Convert the version number to string
    client.delete_model_version(name=model_name, version=version_num)

# Delete the model
client.delete_registered_model(model_name)

# COMMAND ----------

# DBTITLE 1,Drop the Gold View
# MAGIC %sql
# MAGIC DROP VIEW ${unity_catalog}.${unity_schema}.gold_transactions;

# COMMAND ----------

# MAGIC %md
# MAGIC ## END MAIN ##
# MAGIC
# MAGIC - **DELETE THE ML MODEL EXPERIMENTS MANUALLY** 
# MAGIC - Separate cleardown for DLT

# COMMAND ----------

# DBTITLE 1,ONLY FOR NON DLT SETUP - Drop the Silver Table
# MAGIC %sql
# MAGIC DROP TABLE silver_transactions;
