import argparse
import os
import sys
from pathlib import Path

# Ensure repo root is on sys.path when running this file directly.
ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from databricks.connect import DatabricksSession

from etl import test_module


# If DATABRICKS_CLUSTER_ID is set connect to a specific cluster, else use serverless
databricks_cluster_id = os.getenv("DATABRICKS_CLUSTER_ID")
if databricks_cluster_id:
    spark = DatabricksSession.builder.clusterId(databricks_cluster_id).getOrCreate()
else:
    spark = DatabricksSession.builder.serverless().getOrCreate()


def drop_gold(spark, catalog, schema):
    spark.sql(f"DROP VIEW IF EXISTS `{catalog}`.`{schema}`.`gold_transactions`")


def drop_silver(spark, catalog, schema):
    spark.sql(f"DROP TABLE IF EXISTS `{catalog}`.`{schema}`.`silver_transactions`")


def drop_table(spark, catalog, schema, table):
    spark.sql(f"DROP TABLE IF EXISTS `{catalog}`.`{schema}`.`{table}`")


if __name__ == '__main__':
    print("\nConnected to", test_module.get_workspace_host(spark))

    parser = argparse.ArgumentParser(description="specify the Unity catalog and schema",
                                     formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-c', dest='catalog', action='store', help='Unity Catalog Name', required=True)
    parser.add_argument('-s', dest='schema', action='store', help='Unity Schema Name', required=True)
    parser.add_argument('-t', dest='table', action='store', help='Table or view to drop', required=True)
    args = vars(parser.parse_args())

    table = args['table']
    catalog = args['catalog']
    schema = args['schema']

    if table == 'gold_transactions':
        print("Dropping gold_transactions view")
        drop_gold(spark, catalog, schema)
    elif table == 'silver_transactions':
        print("Dropping silver_transactions table")
        drop_silver(spark, catalog, schema)
    else:
        print(f"Dropping table {table}")
        drop_table(spark, catalog, schema, table)
