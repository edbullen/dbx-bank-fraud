# Databricks notebook source
# MAGIC %md
# MAGIC # Execute the test_module Job
# MAGIC

# COMMAND ----------

from etl import test_module

dbutils.widgets.text(name="catalog", defaultValue='', label='field')
dbutils.widgets.text("schema", defaultValue='', label='field')

# COMMAND ----------

status = test_module.return_true()

if status:
    print("Success")
else:
    print("Failure")


