# Databricks notebook source
# MAGIC %md
# MAGIC # Batch Inference with an MLflow Model in Unity Catalog

# COMMAND ----------

# MAGIC %pip install --force-reinstall mlflow[databricks]
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("unity_catalog", "default_catalog", "Unity Catalog")
dbutils.widgets.text("unity_schema", "default_schema", "Unity Schema")

# COMMAND ----------

# DBTITLE 1,Setup which schema we are working in

unity_catalog = dbutils.widgets.get("unity_catalog")
unity_schema = dbutils.widgets.get("unity_schema")

print(f"Unity Catalog: {unity_catalog}, Unity Schema: {unity_schema} ")
spark.sql(f"USE {unity_catalog}.{unity_schema}")

# COMMAND ----------

# DBTITLE 1,Load the Model
import mlflow.sklearn
mlflow.set_registry_uri('databricks-uc') 

# reference to the model in Unity Catalog, pick the version that has been labeled in Unity Catalog with an alias "production"
model_version_uri = f"models:/{unity_catalog}.{unity_schema}.bank_fraud_predict@production"

# load the model
model = mlflow.sklearn.load_model(model_uri=model_version_uri)

# COMMAND ----------

# DBTITLE 1,Create a PySpark UDF and use it for batch inference
# Create the PySpark UDF
import mlflow.pyfunc
pyfunc_udf = mlflow.pyfunc.spark_udf(spark, model_uri=model_version_uri)

# For the purposes of this example, create a small Spark DataFrame. 
transactions_df = spark.sql("""SELECT id,
                            CAST(amount as FLOAT),
                            CAST(newBalanceDest as FLOAT),
                            CAST(oldBalanceDest as FLOAT),
                            CAST(diffOrig as FLOAT),
                            CAST(diffDest as FLOAT),
                            countryOrig_name,
                            countryDest_name,
                            type
                            FROM gold_transactions LIMIT 100000
                            """)

# COMMAND ----------

# DBTITLE 1,Create a predicted data frame with is_fraud col
# Use the Spark function withColumn() to apply the PySpark UDF to the DataFrame and return a new DataFrame with a prediction column.
from pyspark.sql.functions import struct, col

# Predict on a Spark DataFrame.
predicted_df = transactions_df.withColumn('is_fraud', pyfunc_udf(struct(*map(col, transactions_df.columns))).getItem(0))

display(predicted_df)


# COMMAND ----------

display(predicted_df.groupBy(['is_fraud']).count())

# COMMAND ----------

# create a Spark Temp View so we can query it
predicted_df.createOrReplaceTempView("predictions_view")

#Create a managed Delta table in the catalog
#spark.sql(f"""CREATE OR REPLACE TABLE {unity_catalog}.{unity_schema}.fraud_predictions 
#          AS SELECT * FROM  predictions_view """)
# Update the target table lineage with MLflow information
#spark.sql(f"""ALTER TABLE {unity_catalog}.{unity_schema}.fraud_predictions  
#          SET TBLPROPERTIES ('mlflow_experiment_name'='{experiment_name}', 'mlflow_model_uri'='{model_version_uri}')""")


# COMMAND ----------

# DBTITLE 1,Write the Data out to CSV in a Unity Catalog volume called output
predicted_df.coalesce(1).write.mode('overwrite').option('header', 'true').csv(f"/Volumes/{unity_catalog}/{unity_schema}/bank-fraud/retail/fraud_predictions")
