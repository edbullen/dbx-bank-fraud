# Databricks notebook source
# MAGIC %md
# MAGIC # MLflow Model Training
# MAGIC
# MAGIC ![Train-Test-Diag](https://raw.githubusercontent.com/edbullen/dbx-bank-fraud/main/notebooks/images/ml-diagram-model-development-deployment.png)
# MAGIC

# COMMAND ----------

# DBTITLE 1,Set the default schema and catalog
dbutils.widgets.text("unity_catalog", "default_catalog", "Unity Catalog")
dbutils.widgets.text("unity_schema", "default_schema", "Unity Schema")
unity_catalog = dbutils.widgets.get("unity_catalog")
unity_schema = dbutils.widgets.get("unity_schema")

print(f"Unity Catalog: {unity_catalog}, Unity Schema: {unity_schema} ")
spark.sql(f"USE {unity_catalog}.{unity_schema}")

# COMMAND ----------

# DBTITLE 1,Set a user-name to identify this experiment with
# set some vars to generate names for where we store our experiments and feature data for demonstration purposes
import re
current_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
if current_user.rfind('@') > 0:
  current_user_no_at = current_user[:current_user.rfind('@')]
else:
  current_user_no_at = current_user
current_user_no_at = re.sub(r'\W+', '_', current_user_no_at)
print(f"current_user var set to {current_user}")

# COMMAND ----------

# DBTITLE 1,Model registry in Unity Catalog
# Set MLflow to use Unity Catalog
import mlflow
mlflow.set_registry_uri("databricks-uc")

model_registry_name = f"{unity_catalog}.{unity_schema}.bank_fraud_predict"

# COMMAND ----------

# MAGIC %md
# MAGIC # Load Data for Training

# COMMAND ----------

# DBTITLE 1,Load Data
# load from the gold transactions view - only pick a subset of columns for a simple demonstration
# we want numeric data for our ML model - can't do this directly in the query for categorical features (do this later with OHE)

# One of the requirements for a Feature table is a Primary Key - id in this example.
transactions_df = spark.sql("""SELECT id,
                            CAST(amount as FLOAT),
                            CAST(isUnauthorizedOverdraft as FLOAT),
                            CAST(newBalanceDest as FLOAT),
                            CAST(oldBalanceDest as FLOAT),
                            CAST(diffOrig as FLOAT),
                            CAST(diffDest as FLOAT),
                            countryOrig_name,
                            countryDest_name,
                            CASE 
                              WHEN is_fraud = 'false' THEN 0
                              WHEN is_fraud = "true" THEN 1
                              ELSE 'NaN'
                            END as is_fraud
                            FROM hsbc.fraud_features.fraud_training_1""")
#FROM gold_transactions LIMIT 100000""")

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Cast Label to Float
# cast is_fraud to a float; if there was any bad data, we will get an error
from pyspark.sql.types import IntegerType, FloatType
transactions_df = transactions_df.select(transactions_df['*']).withColumn("label", transactions_df["is_fraud"].cast(FloatType())).drop('is_fraud')

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# DBTITLE 1,Convert Pyspark DF to Pandas DF
# work in Pandas and Scikit learn for a simple example - about 30 MB data gets pulled into the Driver node
import pandas as pd
import sys

transactions_pd = transactions_df.toPandas()
print(sys.getsizeof(transactions_pd))

# COMMAND ----------

mlflow_data = mlflow.data.from_spark(transactions_df)

# COMMAND ----------

transactions_pd.head()

# COMMAND ----------

transactions_pd.groupby(['label']).count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Split Into Test / Train Datasets
# MAGIC
# MAGIC ![Train-Test-Diag](https://raw.githubusercontent.com/edbullen/dbx-bank-fraud/main/notebooks/images/train_test_split-procedure.jpg)

# COMMAND ----------

# DBTITLE 1,Split to Train and Test dataframes, select Feature cols
# util for splitting into training and test data
from sklearn.model_selection import train_test_split

# Vertical Split into X (features) and y (labels)
y = transactions_pd.label
X = transactions_pd.drop(['id', 'label'], axis =1 )

# Divide data into training and validation subsets
X_train_full, X_test_full, y_train, y_test = train_test_split(X, y, train_size=0.8, test_size=0.2, random_state=0)

# list of feature cols - categerical need one hot encoding, numeric need to have missing vals filled
categorical_cols = ["isUnauthorizedOverdraft", "countryOrig_name", "countryDest_name" ]
numeric_cols = ["amount", "newBalanceDest", "oldBalanceDest", "diffOrig", "diffDest"]  

# Keep selected columns only
feature_cols =  numeric_cols + categorical_cols
#feature_cols =  categorical_cols
X_train = X_train_full[feature_cols].copy()
X_test = X_test_full[feature_cols].copy()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preprocessing Pipeline

# COMMAND ----------

# DBTITLE 1,Imports and pre-processing pipeline
# import utils for pre-processing and transforming the training data
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import OneHotEncoder
#from sklearn.preprocessing import FunctionTransformer, StandardScaler

# import ML model algorithm
from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier

# import utils for determining model performance / accuracy
from sklearn.metrics import mean_absolute_error
from sklearn.metrics import confusion_matrix, accuracy_score, f1_score, precision_score, recall_score

# Preprocessing for categorical data
categorical_transformer = Pipeline(steps=[
    # ('keep_columns', 'passthrough', columns_to_keep),
    ('imputer', SimpleImputer(strategy='most_frequent')),
    ('onehot', OneHotEncoder(handle_unknown='ignore'))
])

# Preprocessing for numerical data
numerical_transformer = SimpleImputer(strategy='median')

# Bundle preprocessing for numerical and categorical data
preprocessor = ColumnTransformer(
    transformers=[
        ('num', numerical_transformer, numeric_cols),
        ('cat', categorical_transformer, categorical_cols)
    ])



# COMMAND ----------

# MAGIC %md
# MAGIC ## Model Training 
# MAGIC + Grid-Search in MLflow for multiple hyperparameters. 
# MAGIC + Log metrics and models in an MLflow Experiment.

# COMMAND ----------

# DBTITLE 1,MLFlow training run
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import time

# for demo purposes, just store the experiment in the demo-users path
mlflow.set_experiment(f"/Users/{current_user}/bank_fraud_experiment")
# see documentation for shared workspace experiment options: https://docs.databricks.com/en/mlflow/experiments.html 

estimators_list = [10, 20, 100]
depth_list = [10, 20, 50]

run_timestamp = int(time.time())

# add nested loops here to loop through hyper-paramter grid-search and log results for multiple mlflow runs.
for estimators in estimators_list:
  for depth in depth_list:

    with mlflow.start_run(run_name=f'bank_fraud_{estimators}_{run_timestamp}'):

      mlflow.sklearn.autolog()
      mlflow.log_input(mlflow_data, "training")

      # Set a tag so we can find this group of experiment runs
      mlflow.set_tag("project", "bank_fraud_model")

      # machine learning model instance with hyperparaemters specified
      model = RandomForestClassifier(n_estimators=estimators, random_state=0, max_depth=depth, n_jobs=16)

      # link preprocessing and modeling code into a pipeline
      my_pipeline = Pipeline(steps=[('preprocessor', preprocessor),
                                    ('model', model)
                                  ])

      # Process training data and fit model 
      my_pipeline.fit(X_train, y_train)

      # Preprocessing of validation data, get predictions
      y_pred = my_pipeline.predict(X_test)

      # Evaluate the model
      #score = mean_absolute_error(y_test, preds)
      #print('MAE:', score)
      accuracy = accuracy_score(y_test, y_pred)*100
      prec = precision_score(y_test, y_pred)*100
      rec = recall_score(y_test, y_pred)*100
      f1 = f1_score(y_test, y_pred)*100

      #print(f"Accuracy: {accuracy} Precision: {prec} Recall: {rec} F1 Score: {f1}")

      # explicitly log metrics in MLflow (many metrics are auto-logged anyway)
      mlflow.log_metric('accuracy', accuracy)
      mlflow.log_metric('f1_score', f1)
      mlflow.log_param('max_depth', depth)
      mlflow.log_param('n_estimators', estimators)

      # Record a confusion matrix plot for the model test results
      fig, ax = plt.subplots(figsize=(6, 6))  # set dimensions of the plot
      CM = confusion_matrix(y_test, y_pred)
      ax = sns.heatmap(CM, annot=True, cmap='Blues', fmt='.8g')
      mlflow.log_figure(plt.gcf(), "test_confusion_matrix.png")

      # Get the model feature importances - this is after the OHE transform has been applied
      all_features_importance = model.feature_importances_.round(3)

      # get the model features factoring in OHE 
      ohe_features = my_pipeline.named_steps["preprocessor"].named_transformers_["cat"].named_steps["onehot"].get_feature_names_out(categorical_cols).tolist()
      all_features = numeric_cols + ohe_features

      feature_importance = pd.DataFrame({'feature':all_features, 'importance':all_features_importance})



# COMMAND ----------



# COMMAND ----------


