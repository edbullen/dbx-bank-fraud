import os

from databricks.sdk.core import Config


def sdk_connect_config():
    """Config for local dbconnect-2 connect and execute - export DATABRICKS_TOKEN to use PAT to authenticate"""

    # test env vars are set
    required_vars = {"DATABRICKS_TOKEN", "DATABRICKS_HOST", "DATABRICKS_CLUSTER"}
    diff = required_vars.difference(os.environ)
    if len(diff) > 0:
        raise EnvironmentError(f'Failed because {diff} are not set')

    # do some checks on the DATABRICKS_HOST (workspace-url) format
    databricks_host = os.environ.get('DATABRICKS_HOST')
    if "https://" not in databricks_host:
        raise EnvironmentError(f"DATABRICKS_HOST is {databricks_host}; prefix should be https://")
    databricks_host = databricks_host.rstrip("/")

    databricks_cluster = os.environ.get('DATABRICKS_CLUSTER')

    config = Config(
        host=databricks_host,
        cluster_id=databricks_cluster,
        # token=""
    )
    return config


