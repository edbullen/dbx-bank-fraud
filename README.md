# Databricks Demo with Retail Bank Fraud Data

Pipeline overview: 
+ Ingest files from Azure ADLS storage container - table `bronze_transactions`
  + A Unity Volume needs to be set up in advance which references the location where the files will be staged.
  + Copy the `./data/transactions/*.csv` files in the ADLS storage bucket linked to the Unity Volume ref.
+ Ingest data from REST API - table `fraud_reports`
+ Join Transactions and Fraud data in Databricks to create table `silver_transactions`
+ Databricks Delta tables `banking_customers` and `country_coordinates` are joined with `silver_transactions` to produce table `gold_transactions`

Job Trigger:
+ Incremental transaction data arriving in the ADLS storage container triggers a job
+ This Job has an Auto Loader cloudFiles configuration to load newly arriving data into the bronze transactions table
+ The Job also pulls data from the 

DB-Connect V2 allows IDE development, Git integration and Unit Tests




### Environment

Developed against Databricks `LTS 13.3 ML` cluster

Local Env: Python `3.10.12`

Python Libraries: `requirements.txt`   (note DB-Connect version aligns with Databricks cluster version )


