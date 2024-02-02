

from databricks.connect.session import DatabricksSession as SparkSession

# DB-Connect configuration - the config determines which workspace and cluster the spark session is for
from etl.common import sdk_connect_config
sdk_config = sdk_connect_config()

# DB-connect spark session on the Databricks Cluster connected to by DB-Connect
spark = SparkSession.builder.sdkConfig(sdk_config).getOrCreate()


if __name__ == '__main__':
    # DB-Connect Example
    # df = spark.table("samples.nyctaxi.trips")
    # print(df.count())

    pass


