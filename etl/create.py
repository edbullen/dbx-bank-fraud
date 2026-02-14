import argparse
import os
import sys
from pathlib import Path

# Ensure repo root is on sys.path when running this file directly.
ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient

from etl import test_module
from etl import data_load

# If DATABRICKS_CLUSTER_ID is set connect to a specific cluster, else use serverless
databricks_cluster_id = os.getenv("DATABRICKS_CLUSTER_ID")
if databricks_cluster_id:
    spark = DatabricksSession.builder.clusterId(databricks_cluster_id).getOrCreate()
else:
    spark = DatabricksSession.builder.serverless().getOrCreate()

#def run_silver_load(catalog, schema):
#    print(f"Running silver transactions table loader for {catalog}.{schema}.silver_transactions")
#    data_load.load_silver_transactions(spark, catalog=catalog, schema=schema)
#    print("Silver Transactions table load complete")

# CTAS to create and load fraud_reports table and data
def create_table_ctas(spark
                      , catalog
                      , schema
                      , source_folder
                      , target_table
                      , format_type="csv"
                      , header=True,
                      ):

    source_path = f"/Volumes/{catalog}/{schema}/{source_folder}/"

    try:
        spark.sql(
            f"""
            CREATE TABLE `{catalog}`.`{schema}`.`{target_table}` AS
            SELECT *
            FROM read_files(
                '{source_path}',
                format => '{format_type}',
                header => {str(header).lower()}
            )
            """
        )
        return True
    except Exception as e:
        print(f"Failed to create table {catalog}.{schema}.{target_table}: {e}")
        return False
    

# Create the Gold Transactions view
def create_gold_view(spark, catalog, schema):
    spark.sql(
        f"""
        CREATE OR REPLACE VIEW `{catalog}`.`{schema}`.`gold_transactions`
        AS
        SELECT t.* EXCEPT(pos, _metadata, source_file, amount, countryOrig, countryDest, is_fraud), c.* EXCEPT(id),
               o.alpha3_code as countryOrig, o.country as countryOrig_name, o.long_avg as countryLongOrig_long, o.lat_avg as countryLatOrig_lat,
               d.alpha3_code as countryDest, d.country as countryDest_name, d.long_avg as countryLongDest_long, d.lat_avg as countryLatDest_lat,
               CAST(t.amount AS DOUBLE) as amount,
               boolean(coalesce(is_fraud, 0)) as is_fraud
        FROM `{catalog}`.`{schema}`.`silver_transactions` t
            INNER JOIN `{catalog}`.`{schema}`.`country_coordinates` o ON t.countryOrig=o.alpha3_code
            INNER JOIN `{catalog}`.`{schema}`.`country_coordinates` d ON t.countryDest=d.alpha3_code
            INNER JOIN `{catalog}`.`{schema}`.`banking_customers` c ON c.id=t.customer_id
        """
    )

if __name__ == '__main__':

    #if test_module.return_true():
    #    print("Test Connect Module ran with: Success")
    #else:
    #    print("Test Connect Module ran with: Failure")

    #print(test_module.return_taxi_count(spark))

    print("\nConnected to", test_module.get_workspace_host(spark))
    
    # process command-line arguments to set the Unity Catalog and Schema
    parser = argparse.ArgumentParser(description="specify the Unity catalog and schema",
                                     formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-c', dest='catalog', action='store', help='Unity Catalog Name', required=True)
    parser.add_argument('-s', dest='schema', action='store', help='Unity Schema Name', required=True)
    parser.add_argument('-t', dest='table', action='store', help='Table to create', required=True)
    parser.add_argument('-v', dest='volume', action='store', help='Volume with source data', required=True)
    parser.add_argument('-f', dest='folder', action='store', help='source Folder Volume/retail/<folder> (defaults to table_name)', required=False)
    parser.add_argument('--format', dest='format_type', choices=['csv', 'json'], default='csv',
                        help='Source data format (default: csv)')
    args = vars(parser.parse_args())
    volume = args['volume']
    table = args['table']

    if args['folder']:
        folder = args['folder']
    else:
        folder = args['table']

    if table == 'bronze_transactions':
        
        print(f"Running the bronze_transactions AutoLoader table creation, source_folder={volume}/retail/transactions")
        data_load.transactions_load(spark
                                    , args['catalog']
                                    , args['schema']
                                    , source_folder=f'{volume}/retail/transactions'
                                    , target_table='bronze_transactions'
                                    , format_type=args['format_type'])
    elif table == 'silver_transactions':
        print(f"Running the silver_transactions Creation and Merge/Join operation")
        data_load.load_silver_transactions(spark
                                    , catalog=args['catalog']
                                    , schema=args['schema']
                                    , source_catalog=args['catalog']
                                    , source_schema=args['schema']
                                    , source_table='bronze_transactions'
                                    , target_table=table)
    elif table == 'gold_transactions':
        print(f"Running the gold_transactions View Create")
        create_gold_view(spark, args['catalog'], args['schema'])

    else:
        print(f"Running the {args['table']} CTAS table creation, Volume source_folder={volume}/retail/{folder}")
        create_table_ctas(spark
                         , args['catalog']
                         , args['schema']
                         , source_folder=f"{volume}/retail/{folder}"
                         , target_table=table
                         , format_type=args['format_type'])
