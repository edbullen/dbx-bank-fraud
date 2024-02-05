

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

if __name__ == '__main__':
    # DB-Connect Example
    # df = spark.table("samples.nyctaxi.trips")
    # print(df.count())

    status = test_module.return_true()
    if status:
        print("Test Module ran with: Success")
    else:
        print("Test Module ran with: Failure")

    run_file_loader("hsbc", "hr", source_folder="bank_transactions", target_table="transactions", format_type="csv")




