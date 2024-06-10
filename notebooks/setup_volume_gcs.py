# Databricks notebook source
# MAGIC %md
# MAGIC # Setup a Unity Catalog Volume to source Transaction Files from Google GCS
# MAGIC The bank transactions are staged in Cloud object store bucket (i.e. ADLS / S3 / GCS)
# MAGIC   
# MAGIC Create a Unity Catalog volume that is authorised to access these files so they can be referenced in a CloudFiles Auto Loader job.
# MAGIC
# MAGIC This example shows how to setup access to load files from **GCP**
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Unity Catalog Storage Credential
# MAGIC Before Creating an External Location, a Unity Catalog Storage Credential needs to be set up.
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC 1. *In Databricks* Unity Catalog Data Explorer, **Create an External Data Storage Credential** 
# MAGIC
# MAGIC + Create a named Storage Credential and note the Service Account identity that we need to grant access to in Step 2
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/edbullen/dbx-bank-fraud/main/notebooks/images/UC_GCS_Volume_Credential_Create.png" width="1200">
# MAGIC
# MAGIC
# MAGIC 2. *In the GCP Project*, in Cloud Storage console, **grant access on the storage bucket** that the UC volume will use to by granting role-privileges to the service account identity noted in step 1:
# MAGIC + "Storage Legacy Bucket Reader"  
# MAGIC + "Storage Object Admin"  
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/edbullen/dbx-bank-fraud/main/notebooks/images/UC_GCS_Volume_SA_Grant.png" width="1200">
# MAGIC
# MAGIC
# MAGIC
# MAGIC 3. *In Databricks* Unity Catalog Data Explorer, **Create a new External Location**
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/edbullen/dbx-bank-fraud/main/notebooks/images/UC_GCS_Volume_Create_External_Location.png" width="1000">
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Parameters for creating a Volume mapped to the Google Storage Bucket with Raw TXNs
# Unity Catalog catalog and schema to work in
dbutils.widgets.text(name="catalog", defaultValue='', label='field')
dbutils.widgets.text("schema", defaultValue='', label='field')

# Google Storage GS URL location - gs://<bucket>/<path>
dbutils.widgets.text("txn_bucket_path", defaultValue='', label='field')
dbutils.widgets.text("staging_bucket_path", defaultValue='', label='field')
dbutils.widgets.text("refdata_bucket_path", defaultValue='', label='field')

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
txn_bucket_path = dbutils.widgets.get("txn_bucket_path")
staging_bucket_path = dbutils.widgets.get("staging_bucket_path")
refdata_bucket_path = dbutils.widgets.get("refdata_bucket_path")

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# DBTITLE 1,Create a Unity Catalog External Volume - path is set up as UC External Location
# MAGIC %sql
# MAGIC CREATE EXTERNAL VOLUME ${catalog}.${schema}.transactions_raw
# MAGIC LOCATION '${txn_bucket_path}';

# COMMAND ----------

# MAGIC %md
# MAGIC Path for listing files in the Volume: `/Volumes/<catalog_identifier>/<schema_identifier>/<volume_identifier>/<path>/<file_name>`

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL VOLUME ${catalog}.${schema}.staging
# MAGIC LOCATION '${staging_bucket_path}';
