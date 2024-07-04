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
# MAGIC -- this won't work unless Auto Loader has been set up to record the file name on load
# MAGIC --SELECT source_file, count(*) 
# MAGIC --FROM ${catalog}.${schema}.transactions
# MAGIC --GROUP BY source_file;

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

txn_df = spark.read.table("hsbc.fraud.transactions")

# COMMAND ----------

txn_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import count

agg_df = txn_df.groupBy("countryOrig").agg(count("*").alias("count_by_countryOrig"))
agg_df.show()
