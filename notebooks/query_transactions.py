# Databricks notebook source
# MAGIC %md
# MAGIC # Explore the transactions table

# COMMAND ----------

dbutils.widgets.text(name="catalog", defaultValue='', label='field')
dbutils.widgets.text("schema", defaultValue='', label='field')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) 
# MAGIC FROM ${catalog}.${schema}.transactions

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE ${catalog}.${schema}.transactions

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ${catalog}.${schema}.transactions LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ### View Transactions loaded by File

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT source_file, count(*) 
# MAGIC FROM ${catalog}.${schema}.transactions
# MAGIC GROUP BY source_file;

# COMMAND ----------

# MAGIC %md
# MAGIC # View Transaction History

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY hsbc.hr.transactions

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT source_file, count(*) 
# MAGIC FROM 
# MAGIC (SELECT * FROM ${catalog}.${schema}.transactions VERSION AS OF 1)
# MAGIC GROUP BY source_file;
