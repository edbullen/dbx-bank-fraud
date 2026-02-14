# COMMAND ------------------------

# just a test to see if the module has been loaded at all
def return_true():
    return True

# count the Databricks samples taxi trips table
def get_taxi_count(spark):
    df = spark.table("samples.nyctaxi.trips")
    return df.count()

# check the workspace host that we are connected to
def get_workspace_host(spark):
    host = spark.conf.get("spark.databricks.workspaceUrl", "Not set")
    return host

