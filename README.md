# Databricks Demo with Retail Bank Fraud Data

### Pipeline overview
+ Ingest files from Azure ADLS storage container - table `transactions`
  + A Unity Volume needs to be set up in advance which references the location where the files will be staged. See `./notebooks/setup_volume.py` for an example.
  + Copy the `./data/transactions/*.csv` files in the ADLS storage bucket linked to the Unity Volume ref.
  + The Azure ADLS bucket can be replaced with AWS s3 or GCP GCS bucket and the same approach followed.
+ Ingest data from REST API - table `fraud_reports`
+ Join Transactions and Fraud data in Databricks to create table `silver_transactions`
+ Databricks Delta tables `banking_customers` and `country_coordinates` are joined with `silver_transactions` to produce table `gold_transactions`

![ETL_Flow](./notebooks/images/data_flow.png)     


### Sample Data

Sample data for running the pipeline is in `./data/`

+ `./data/country_coordinates/*` - static data; just manually load the CSV into a delta table.
+ `./data/customers/*` - static data; just manually load the CSV into a delta table.
+ `./data/fraud_reports/*` - `./etl/data_load.web_url_pull()` pulls data directly from here and stages in the `fraud_reports` table.
+ `./data/transactions/*` - use these g-zipped CSV files to load into ADLS (or an S3 / GCS bucket) and simulate streaming files into the pipeline.

### Job Configuration

Use the two notebooks 
+ `./jobs/run_transaction_file_load.py`
+ `./jobs/run_silver_table_load.py`

to call the ETL code in ./etl and run the ETL pipeline.  These read the parameters configured in the Databricks Job run-time configuration and pass them to the code execution.


### Development Environment

Databricks DB-Connect V2 allows IDE development, Git integration and Unit Tests

See function `sdk_connect_config()` in `./etl/common.py` for how this is set up.  
`main.py` shows an example of how this can be used to run the ETL code against a remote test cluster and data-set from code in the local Dev IDE environment.

### System Environment

Developed against Databricks `LTS 13.3 ML` cluster

Local Env: Python `3.10.12`

Python Libraries: `requirements.txt`   (note DB-Connect version aligns with Databricks cluster version )

### Analytics

Create the `gold_transactions` view using notebook `./notebooks/setup_gold_transactions_view.py`  

The view can be used for doing analytics, building dashboards or training ML models.  

![Dashboard](./notebooks/images/dashboard_example.png)     

