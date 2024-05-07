# Import functions
from pyspark.sql.functions import col, current_timestamp
from delta.tables import *

import pandas as pd


def auto_loader(spark, catalog, schema, source_folder, target_table, format_type="csv"):
    """ load raw file-data from a Unity Catalog Volume location and write to Delta Table

    documentation: https://docs.databricks.com/en/ingestion/auto-loader/options.html

    :param spark     SparkSession
    :param catalog   String - Unity catalog location for input file and output table
    :param schema    String - Unity schema location for input file and output table
    :param source_folder String - folder / directory name in which input file is located
    :param target_table  String - name of output tablr
    :param format_type   String - input file format (CSV or JSON)

    """

    # Define path to source files and output table
    file_path = f"/Volumes/{catalog}/{schema}/{source_folder}/"
    table_name = f"{catalog}.{schema}.{target_table}"

    # username = spark.sql("SELECT regexp_replace(current_user(), '[^a-zA-Z0-9]', '_')").first()[0]
    checkpoint_path = f"/tmp/{catalog}/{schema}/{source_folder}/_checkpoint/"

    # Configure Auto Loader to ingest CSV data to a Delta table
    (spark.readStream
     .format("cloudFiles")
     .option("cloudFiles.format", f"{format_type}")
     .option("header", "true")
     .option("cloudFiles.schemaLocation", checkpoint_path)
     .load(file_path)
     .select("*", col("_metadata.file_path").alias("source_file"), current_timestamp().alias("processing_time"))
     .writeStream
     .option("checkpointLocation", checkpoint_path)
     .trigger(availableNow=True)
     .toTable(table_name))


def web_url_pull(spark, catalog, schema, url, target_table, format_type="csv", columns=None):
    """ get data from a web-url and merge write to the target_table

    :param spark     SparkSession
    :param catalog   String - Unity catalog location for input file and output table
    :param schema    String - Unity schema location for input file and output table
    :param source_folder String - folder / directory name in which input file is located
    :param target_table  String - name of output tablr
    :param format_type   String - input file format (CSV or JSON)
    :param columns   List - optional list of columns to filter before writing out

    """

    if format_type == 'csv':
        pandas_df = pd.read_csv(url)
        if columns:
            pandas_df = pandas_df[columns]

        df = spark.createDataFrame(pandas_df)

        # Write PySpark DataFrame to Delta table
        df.write.format("delta").mode("overwrite")\
            .saveAsTable(f"{catalog}.{schema}.{target_table}")

    else:
        raise ValueError(f"Unhandled file format type: {format_type}")


def load_silver_transactions(spark, catalog, schema,
                             source_catalog,
                             source_schema,
                             source_table="transactions",
                             target_table="silver_transactions"):
    """ Load the Silver Transactions Table based on bronze txns joined with fraud reports"""

    if source_catalog is None:
        source_catalog = catalog
    if source_schema is None:
        source_schema = schema

    spark.sql(f"""
    MERGE INTO {catalog}.{schema}.{target_table} as target
    USING (SELECT f.is_fraud, t.* EXCEPT(countryOrig, countryDest, t._rescued_data), 
         regexp_replace(countryOrig, "--", "") as countryOrig, 
         regexp_replace(countryDest, "--", "") as countryDest, 
         newBalanceOrig - oldBalanceOrig as diffOrig, 
         newBalanceDest - oldBalanceDest as diffDest
         FROM {source_catalog}.{source_schema}.{source_table} t
        LEFT JOIN  {catalog}.{schema}.fraud_reports f USING(id)) as source
    ON source.id = target.id
    WHEN NOT MATCHED
      THEN INSERT *
    """
    )

