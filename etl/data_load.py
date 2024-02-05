# Import functions
from pyspark.sql.functions import col, current_timestamp

from databricks.connect import DatabricksSession
from databricks.connect.session import DatabricksSession as SparkSession


def auto_loader(spark, catalog, schema, source_folder, target_table, format_type="csv"):
    """ load raw file-data from a Unity Catalog Volume location and write to Delta Table

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
