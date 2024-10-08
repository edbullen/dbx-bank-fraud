# Databricks notebook source
dbutils.widgets.text("unity_catalog", "default_catalog", "Unity Catalog")
dbutils.widgets.text("unity_schema", "default_schema", "Unity Schema")
unity_catalog = dbutils.widgets.get("unity_catalog")
unity_schema = dbutils.widgets.get("unity_schema")

print(f"Unity Catalog: {unity_catalog}, Unity Schema: {unity_schema} ")
spark.sql(f"USE {unity_catalog}.{unity_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## DLT Pipeline Cleardown
# MAGIC
# MAGIC + `silver_transactions` 
# MAGIC + `fraud_reports`  
# MAGIC + `transactions`  
# MAGIC For DLT, just delete the FSI Fraud DLT workflow in the *Workflows* -> *Delta Live Tables* GUI.  
# MAGIC
# MAGIC This will clear down all the associated streaming tables, data, and cloud-files checkpoint 
# MAGIC
# MAGIC state.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE silver_transactions;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE transactions;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE fraud_reports;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleardown staged data
# MAGIC
# MAGIC Remove all the `transactions/bronze_transactions_*.csv` files EXCEPT the first txn file.
# MAGIC   
# MAGIC Remove `fraud_reports_part_b.csv`
