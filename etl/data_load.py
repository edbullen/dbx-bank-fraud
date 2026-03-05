# Import functions
from pyspark.sql.functions import col, current_timestamp
from delta.tables import *

import pandas as pd


def transactions_load(
    spark,
    catalog,
    schema,
    source_folder,
    target_table,
    format_type="csv",
    include_existing_files=True,
    header=True,
):
    """ load raw file-data from a Unity Catalog Volume location and write to Delta Table.  
    Job needs to read new data only.

    :param spark     SparkSession
    :param catalog   String - Unity catalog location for input file and output table
    :param schema    String - Unity schema location for input file and output table
    :param source_folder String - folder / directory name in which input file is located
    :param target_table  String - name of output table
    :param format_type   String - input file format (CSV or JSON)
    :param include_existing_files Boolean - if true, load existing files in source path
    :param header    Boolean - if true and CSV, treat first row as headers

    """
    source_path = f"/Volumes/{catalog}/{schema}/{source_folder}/"
    checkpoint_path = f"/Volumes/{catalog}/{schema}/{source_folder}/_checkpoint/"

    reader = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", format_type)
        .option("cloudFiles.schemaLocation", checkpoint_path)
    )
    if include_existing_files:
        reader = reader.option("cloudFiles.includeExistingFiles", "true")
    if header and format_type == "csv":
        reader = reader.option("header", "true")

    (reader
     .load(source_path)
     .select("*", "_metadata")  # Explicitly include _metadata
     .select("*", col("_metadata.file_path").alias("source_file"), current_timestamp().alias("processing_time"))
     .writeStream
     .option("checkpointLocation", checkpoint_path)
     .trigger(availableNow=True)
     .toTable(f"`{catalog}`.`{schema}`.`{target_table}`")
     )


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
            .saveAsTable(f"`{catalog}`.`{schema}`.`{target_table}`")

    else:
        raise ValueError(f"Unhandled file format type: {format_type}")


def load_silver_transactions(spark, catalog, schema,
                             source_catalog,
                             source_schema,
                             source_table="bronze_transactions",
                             target_table="silver_transactions"):
    """ Load the Silver Transactions Table based on bronze txns joined with fraud reports"""

    if source_catalog is None:
        source_catalog = catalog
    if source_schema is None:
        source_schema = schema

    source_query = f"""
        SELECT
            t.* EXCEPT(countryOrig, countryDest, t._rescued_data, t._metadata, t.source_file, newBalanceOrig, oldBalanceOrig, newBalanceDest, oldBalanceDest),
            regexp_replace(countryOrig, "--", "") as countryOrig,
            regexp_replace(countryDest, "--", "") as countryDest,
            CAST(newBalanceOrig AS DOUBLE) as newBalanceOrig,
            CAST(oldBalanceOrig AS DOUBLE) as oldBalanceOrig,
            CAST(newBalanceDest AS DOUBLE) as newBalanceDest,
            CAST(oldBalanceDest AS DOUBLE) as oldBalanceDest,
            CAST(newBalanceOrig AS DOUBLE) - CAST(oldBalanceOrig AS DOUBLE) as diffOrig,
            CAST(newBalanceDest AS DOUBLE) - CAST(oldBalanceDest AS DOUBLE) as diffDest,
            f.is_fraud
        FROM `{source_catalog}`.`{source_schema}`.`{source_table}` t
        LEFT JOIN `{catalog}`.`{schema}`.fraud_reports f USING(id)
    """

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS `{catalog}`.`{schema}`.`{target_table}` AS
        SELECT * FROM ({source_query}) WHERE 1=0
    """)

    spark.sql(f"""
        MERGE INTO `{catalog}`.`{schema}`.`{target_table}` as target
        USING ({source_query}) as source
        ON source.id = target.id
        WHEN NOT MATCHED
        THEN INSERT *
    """)


def gold_transactions(spark: SparkSession, source_catalog: str, source_schema: str, country: str = None) -> DataFrame:
    """ return a spark dataframe from the gold transactions table, optionally filter by country
    :param: SparkSession
    :param: catalog
    :param: schema
    :param: country - optional filter by three-letter country code EG RUS, USA etc
    :returns: Spark DataFrame
    """

    result = spark.table(f"`{source_catalog}`.`{source_schema}`.gold_transactions")
    if country:
        return result.filter(result["country"] == country)
    else:
        return result
