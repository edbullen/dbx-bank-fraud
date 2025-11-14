# Databricks notebook source
# MAGIC %md
# MAGIC # MLFlow Model Deployment to Unity Catalog

# COMMAND ----------

# DBTITLE 1,Configure Widgets for Unity Catalog schema
dbutils.widgets.text("unity_catalog", "default_catalog", "Unity Catalog")
dbutils.widgets.text("unity_schema", "default_schema", "Unity Schema")

# COMMAND ----------

# DBTITLE 1,Set the Default Catalog and Schema

unity_catalog = dbutils.widgets.get("unity_catalog")
unity_schema = dbutils.widgets.get("unity_schema")

print(f"Unity Catalog: {unity_catalog}, Unity Schema: {unity_schema} ")
spark.sql(f"USE {unity_catalog}.{unity_schema}")

# COMMAND ----------

# DBTITLE 1,Local Vars to pick up MLflow Experiment location
# set some vars to generate names for where we store our experiments and feature data for demonstration purposes
import re
current_user = spark.sql("SELECT current_user()").collect()[0][0]
if current_user.rfind('@') > 0:
  current_user_no_at = current_user[:current_user.rfind('@')]
else:
  current_user_no_at = current_user
current_user_no_at = re.sub(r'\W+', '_', current_user_no_at)
print(f"current_user var set to {current_user}")

# for demo purposes, work with an experiment in the demo-users path
experiment_name = f"/Users/{current_user}/bank_fraud_experiment"

print(f"MLFlow Experiment name is {experiment_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set MLflow to Use Unity Catalog
# MAGIC Specify the model-versions location in a Unity Catalog `catalog`.`schema` location.

# COMMAND ----------

# DBTITLE 1,Set Model Registry to use Unity Catalog
# Set MLflow to use Unity Catalog
import mlflow
mlflow.set_registry_uri("databricks-uc")
model_registry_name = f"{unity_catalog}.{unity_schema}.bank_fraud_predict"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Search MLflow Experiments and Register Model Version in Unity Catalog

# COMMAND ----------

# DBTITLE 1,Search for the best run based on a specified metric
# Get the MLflow Experiment ID to search in
current_experiment=dict(mlflow.get_experiment_by_name(experiment_name))
experiment_id=current_experiment['experiment_id']

# Get the best model from the runs in our experiment
best_model = mlflow.search_runs([experiment_id], 
                                filter_string='tags.project = "bank_fraud_model"'
                                , order_by = ['metrics.f1_score DESC']).iloc[0]

# COMMAND ----------

mlflow.set_registry_uri("databricks-uc")
model_registered = mlflow.register_model("runs:/"+best_model.run_id+"/model", model_registry_name)

# COMMAND ----------

# DBTITLE 1,Assign an Alias to a Model Version to reference it in testing or production
# MAGIC %md
# MAGIC ## Set the Model Alias
# MAGIC
# MAGIC Models in Unity Catalog support aliases for model deployment. 
# MAGIC Aliases provide mutable, named references (for example, “Champion” or “Challenger”) to a particular version of a registered model. 
# MAGIC You can reference and target model versions using these aliases in downstream inference workflows.
# MAGIC
# MAGIC Set the Alias in the Unity Catalog browser.
