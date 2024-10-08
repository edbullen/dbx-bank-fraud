import argparse

from databricks.connect.session import DatabricksSession as SparkSession

# DB-Connect configuration - the config determines which workspace and cluster the spark session is for
from etl.common import sdk_connect_config
from etl import test_module
from etl import data_load
sdk_config = sdk_connect_config()

# DB-connect spark session on the Databricks Cluster connected to by DB-Connect
spark = SparkSession.builder.sdkConfig(sdk_config).getOrCreate()


def run_file_loader(catalog, schema, source_folder, target_table, format_type):
    print(f"Running {format_type} file loader for {catalog}.{schema}.{target_table} from {source_folder}/")

    data_load.auto_loader(spark
                          , catalog=catalog
                          , schema=schema
                          , source_folder=source_folder
                          , target_table=target_table
                          , format_type=format_type)
    print("file loader complete")


def run_fraud_loader(catalog, schema, url, target_table, format_type, columns):
    print(f"Running {format_type} URL loader for {catalog}.{schema}.{target_table} from {url}/")

    data_load.web_url_pull(spark
                           , catalog=catalog
                           , schema=schema
                           , url=url
                           , target_table=target_table
                           , format_type=format_type
                           , columns=columns)

    print("fraud loader pull from remote URL complete")


def run_silver_load(catalog, schema):
    print(f"Running silver transactions table loader for {catalog}.{schema}.silver_transactions")
    data_load.load_silver_transactions(spark, catalog=catalog, schema=schema)
    print("Silver Transactions table load complete")



if __name__ == '__main__':
    # DB-Connect Example
    # df = spark.table("samples.nyctaxi.trips")
    # print(df.count())

    status = test_module.return_true()
    if status:
        print("Test Connect Module ran with: Success")
    else:
        print("Test Connect Module ran with: Failure")

    # process command-line arguments to set the Unity Catalog and Schema
    parser = argparse.ArgumentParser(description="specify the Unity catalog and schema",
                                     formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-c', dest='catalog', action='store', help='Unity Catalog Name', required=True)
    parser.add_argument('-s', dest='schema', action='store', help='Unity Schema NAme', required=True)
    args = vars(parser.parse_args())

    # get the gold_transactions data in a data-frame and count it.
    gold_df = data_load.gold_transactions(spark, args['catalog'], args['schema'], 'RUS')
    print(gold_df.count(), 'gold transactions count')




    # old examples
    """
    run_file_loader("catalog", "schema", source_folder="bank_transactions", target_table="transactions", format_type="csv")

    run_fraud_loader("catalog", "schema"
                     , url="https://raw.githubusercontent.com/edbullen/dbx-bank-fraud/main/data/fraud_reports/fraud_reports_part_b.csv"
                     , target_table="fraud_reports"
                     , format_type="csv"
                     , columns=["is_fraud", "id"])

    run_silver_load("", "")
    """

