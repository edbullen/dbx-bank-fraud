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
# MAGIC (SELECT * FROM ${catalog}.${schema}.transactions VERSION AS OF 3)
# MAGIC GROUP BY source_file;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hsbc.hr.gold_transactions limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hsbc.hr.silver_transactions LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC # Query the Gold Transactions View

# COMMAND ----------

# MAGIC %sql
# MAGIC select countryOrig , countryDest , type, value from (
# MAGIC     select countryOrig , countryDest , type, sum(amount ) as value 
# MAGIC     from ${catalog}.${schema}.gold_transactions
# MAGIC     where is_fraud and amount > 350000
# MAGIC     group by countryOrig , countryDest ,type
# MAGIC ) order by value desc
# MAGIC ;

# COMMAND ----------


