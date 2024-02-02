# Databricks notebook source
# MAGIC %md
# MAGIC # Setup a Unity Catalog Volume to source Transaction Files from
# MAGIC The bank transactions are staged in Cloud object store bucket (i.e. ADLS / S3 / GCS)
# MAGIC   
# MAGIC Create a Unity Catalog volume that is authorised to access these files so they can be referenced in a CloudFiles Auto Loader job.
# MAGIC
# MAGIC This example shows how to setup access to load files from Azure using an Azure Service Principle and a key stored in dbutils.secrets

# COMMAND ----------

# MAGIC %md
# MAGIC ### Unity Catalog Storage Credential
# MAGIC Before Creating an External Location, a Unity Catalog Storage Credential needs to be set up.
# MAGIC
# MAGIC 1. *In Azure*, **register an Identity App** (see "App Registrations" in the console)
# MAGIC <img src="https://raw.githubusercontent.com/edbullen/dbx-notebooks/main/ADLS_unity/Azure_Application_oneenv-adls.png" width="1200">
# MAGIC
# MAGIC
# MAGIC
# MAGIC 2. *In Databricks* Unity Catalog Data Explorer, **Create an External Data Storage Credential** that links to the App ID and stores a Key associated with the the registered Azure Indentity App. 
# MAGIC <img src="https://raw.githubusercontent.com/edbullen/dbx-notebooks/main/ADLS_unity/Unity_Catalog_Storage_Credential.png" width="1200">
# MAGIC

# COMMAND ----------

# DBTITLE 1,Set Parameters
# Unity Catalog catalog and schema to work in
dbutils.widgets.text(name="catalog", defaultValue='', label='field')
dbutils.widgets.text("schema", defaultValue='', label='field')

# Azure Service Principal Application Id - ie. "########-####-####-####-############"
dbutils.widgets.text("azure_app_id", defaultValue='', label='field')

# Azure application Directory ID - format "########-####-####-####-############"
dbutils.widgets.text("azure_dir_id", defaultValue='', label='field')

# params for getting Secret Key from dbutils - EG APP_KEY = dbutils.secrets.get(scope = "oetrta", key = "oneenv-adls-secret")
dbutils.widgets.text("key_scope", defaultValue='', label='field')
dbutils.widgets.text("key_id", defaultValue='', label='field')

# External Credential Name
dbutils.widgets.text("credential_name", defaultValue='', label='field')

# URL for where the files are located - i.e. 'abfss://<storage_container>@<storage_account>.dfs.core.windows.net/transactions'
dbutils.widgets.text(name="url", defaultValue='', label='field')

# COMMAND ----------

APPLICATION_ID = dbutils.widgets.get("azure_app_id")
DIRECTORY_ID = dbutils.widgets.get("azure_dir_id")

KEY_SCOPE = dbutils.widgets.get("key_scope")
KEY_ID = dbutils.widgets.get("key_id")

APP_KEY = APP_KEY = dbutils.secrets.get(scope = KEY_SCOPE, key = KEY_ID)

URL = dbutils.widgets.get("url")
UC_CREDENTIAL_NAME = dbutils.widgets.get("credential_name")

# COMMAND ----------

# DBTITLE 1,Set up Connectivity
spark.conf.set("fs.azure.account.auth.type", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id", APPLICATION_ID)
spark.conf.set("fs.azure.account.oauth2.client.secret", APP_KEY)
spark.conf.set("fs.azure.account.oauth2.client.endpoint", "https://login.microsoftonline.com/"+DIRECTORY_ID+"/oauth2/token")

# COMMAND ----------

# DBTITLE 1,Create a Unity Catalog External Location - ref's a UC Storage Credential
# MAGIC %sql
# MAGIC CREATE EXTERNAL LOCATION bank_transactions
# MAGIC URL 'abfss://deltalake@oneenvadls.dfs.core.windows.net/hsbc/hr/transactions'
# MAGIC WITH (STORAGE CREDENTIAL field_demos_credential);

# COMMAND ----------

# DBTITLE 1,Create a Unity Catalog External Volume - path is set up as UC External Location
# MAGIC %sql
# MAGIC CREATE EXTERNAL VOLUME hsbc.hr.bank_transactions
# MAGIC     LOCATION 'abfss://deltalake@oneenvadls.dfs.core.windows.net/hsbc/hr/transactions'
# MAGIC     COMMENT 'Unity External Volume for Bank Transactions in Azure ADLS';

# COMMAND ----------


