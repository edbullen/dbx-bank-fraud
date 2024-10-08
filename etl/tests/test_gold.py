from databricks.connect.session import DatabricksSession as SparkSession

# DB-Connect configuration - the config determines which workspace and cluster the spark session is for
from etl.common import sdk_connect_config
from etl import test_module
from etl import data_load
sdk_config = sdk_connect_config()


def test_gold_df():
    #session = SparkSession.builder.serverless().getOrCreate()
    session = SparkSession.builder.sdkConfig(sdk_config).getOrCreate()

    assert 1 == 1

