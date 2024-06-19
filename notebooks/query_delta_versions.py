# Databricks notebook source
# DBTITLE 1,Set parameters for catalog and schema
dbutils.widgets.text(name="catalog", defaultValue='', label='field')
dbutils.widgets.text("schema", defaultValue='', label='field')

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT(alpha3_code)
# MAGIC FROM ${catalog}.${schema}.country_coordinates
# MAGIC WHERE alpha3_code LIKE 'R%'

# COMMAND ----------

# DBTITLE 1,Delete some data from country_coordinates
# MAGIC %sql
# MAGIC DELETE FROM ${catalog}.${schema}.country_coordinates
# MAGIC WHERE alpha3_code = 'RUS'
# MAGIC

# COMMAND ----------

# DBTITLE 1,Show the table physical details
# MAGIC %sql
# MAGIC DESCRIBE DETAIL ${catalog}.${schema}.country_coordinates

# COMMAND ----------

# DBTITLE 1,Show the versions
# MAGIC %sql
# MAGIC DESC HISTORY ${catalog}.${schema}.country_coordinates

# COMMAND ----------

# MAGIC %md
# MAGIC Now - check the impact in the dashboard (queries the gold view with a join on `country_coordinates`).
# MAGIC
# MAGIC The Russian data has gone.
# MAGIC
# MAGIC The audit trail can also be seen in the Unity catalog UI as well.

# COMMAND ----------

# DBTITLE 1,Use Delta Time Travel to Reset the Changes
# MAGIC %sql
# MAGIC RESTORE TABLE ${catalog}.${schema}.country_coordinates TO VERSION AS OF 0

# COMMAND ----------

# DBTITLE 1,Check the table history
# MAGIC %sql
# MAGIC DESC HISTORY ${catalog}.${schema}.country_coordinates

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT(alpha3_code) 
# MAGIC FROM ${catalog}.${schema}.country_coordinates
# MAGIC VERSION AS OF 2
# MAGIC WHERE alpha3_code LIKE 'R%'
# MAGIC ;

# COMMAND ----------

# MAGIC %md
# MAGIC The dashboard should now be back to normal.
