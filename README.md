# Databricks Demo with Retail Bank Fraud Data

Ingest multiple data-sources and join them to analyze simulated bank fraud transactions and build a machine learning model to predict fraud.

The dataset is based on the [PaySim](https://www.msc-les.org/proceedings/emss/2016/EMSS2016_249.pdf),  dataset (GitHub [repo](https://github.com/EdgarLopezPhD/PaySim) and [LICENSE](https://github.com/EdgarLopezPhD/PaySim/blob/master/LICENSE) for more details). The Databricks demo version augmented this dataset to introduce simulated geo-data as well.  The Databricks version of this data has a [LICENCE](https://github.com/databricks-demos/dbdemos-dataset/blob/main/LICENSE) for use.



![Dashboard](./doc/dashboard_example.png)  

## Data Schema

Files are staged in Unity Catalog volumes and then ingested into Delta Tables.  

![Schema](./doc/retail_fraud_schema.png)  

# IDE Connect to the workspace environment
Most steps described can be executed from a local IDE running with a local clone of this Git repo.   Use the Databricks SDK and CLI to authenticate to a remote workspace and it's services.
  
The Databricks CLI profiles are configured in `~/.databrickscfg`.  

+ Check the available CLI profiles to connect to: `databricks auth profiles`
+ Authenticate to a configured profile using [U2M OAuth](https://docs.databricks.com/aws/en/dev-tools/cli/authentication#oauth-user-to-machine-u2m-authentication): `databricks auth login -p my_profile_name`.  



# Setup - Base Data, Tables and Views

File-based data from `./data` folder in this repo needs to be loaded to a Unity Catalog Volume.  This can be a UC *Managed Volume* (storage and setup managed within Databricks) or an *External Volume* (files stored in an external cloud storage bucket mapped to this volume).  

## Copy the CSV and JSON data to a Unity Catalog Volume
1. Identify the Databricks Catalog, Schema, Volume name to load the data to.
2. export local env vars:
+ `export UNITY_CATALOG=<catalog>`
+ `export UNITY_SCHEMA=<schema>`
+ `export UNITY_VOLUME=<volume>`

Manually, in the Workspace GUI, create the volume directory structure:
```
<volume>  
   ├── retail/
        ├── transactions/
        │── fraud_reports/ 
        ├── customers/
        ├── country_code/            
```

From the root of this repo:

```
databricks fs cp ./data/transactions/ dbfs:/Volumes/$UNITY_CATALOG/$UNITY_SCHEMA/$UNITY_VOLUME/retail/transactions/ --overwrite --recursive

databricks fs cp ./data/fraud_reports/ dbfs:/Volumes/$UNITY_CATALOG/$UNITY_SCHEMA/$UNITY_VOLUME/retail/fraud_reports/ --overwrite --recursive  

databricks fs cp ./data/customers_json/ dbfs:/Volumes/$UNITY_CATALOG/$UNITY_SCHEMA/$UNITY_VOLUME/retail/customers/ --overwrite --recursive

databricks fs cp ./data/country_coordinates/country_coordinates.csv dbfs:/Volumes/$UNITY_CATALOG/$UNITY_SCHEMA/$UNITY_VOLUME/retail/country_code/ --overwrite
```

## Create Base Tables and populate.

These tables can be setup from a remote command prompt session, running in the root of this repo.  

Connect to a remote workspace as per the instructions above in *IDE Connect to the workspace environment*

### 1. Create Bronze transactions `bronze_transactions`

Use the `etl/create.py` script to create the bronze transactions table by specifying the table name `-t` as `bronze_transactions`

+ The `create.py` script runs differently for the `bronze_transactions` table and runs an Autoloader incremental load (not a simple CTAS)

Specify the catalog `-c` the schema `-s` to work in.  Specify the volume `-v` in which the source data is staged in.  

Example:  
```
python etl/create.py -c users -s ed_bullen -t bronze_transactions -v bank-fraud
```

### 2. Create the Fraud Reports table `fraud_reports`

This is just a CTAS operation.  

Example:
```
python etl/create.py -c users -s ed_bullen -t fraud_reports -v bank-fraud 
```

### 3. Create the Customer Details table `banking_customers`

This is just a CTAS operation.  Note the option `--format json` for JSON format data.  

Also, if the source folder name is different from the table name, specify the folder with the `-f` option.

Example:
```
python etl/create.py -c users -s ed_bullen -t banking_customers -v bank-fraud -f customers --format json
```

### 4. Create the Country Details table `country_coordinates` 

This is just a CTAS operation. 
The source folder name is different from the table name, so specify the folder with the `-f` option.

Example:
```
python etl/create.py -c users -s ed_bullen -t country_coordinates -v bank-fraud -f country_code 
```

### 5. Run the Silver Transactions Merge-Load to Create the `silver_transactions` table

```
python etl/create.py -c users -s ed_bullen -t silver_transactions -v bank-fraud
``` 

### 6. Create the Gold Transactions View

```
python etl/create.py -c users -s ed_bullen -t gold_transactions -v bank-fraud
```

# Setup - ML Model Train and Deploy

## Machine Learning and MLflow

1. Train a model with multiple training runs in an MLflow experiment using the `./notebooks/fraud_model_training.py` notebook
2. Deploy the best performing model from the MLFlow experiments to Unity Catalog using the  `./notebooks/fraud_model_deploy.py` notebook.
3. Run a batch set of predictions using the model registered in Unity Catalog and store the results using the `./notebooks/fraud_model_run.py` notebook



# Job Configuration

Use the two notebooks 
+ `./jobs/run_transaction_file_load.py`
+ `./jobs/run_silver_table_load.py`

to call the ETL code in `./etl` and run the ETL pipeline.  These read the parameters configured in the Databricks Job run-time configuration and pass them to the code execution.




