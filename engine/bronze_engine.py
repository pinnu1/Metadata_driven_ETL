import dlt
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


# -------------------------------------------------
# Spark session (SAFE for DLT + modules)
# -------------------------------------------------
spark = SparkSession.builder.getOrCreate()


# -------------------------------------------------
# JDBC READ (FULL / INCREMENTAL)
# -------------------------------------------------
def read_from_azure_sql_jdbc(
    server_name: str,
    database_name: str,
    table_name: str,
    username: str,
    password: str,
    incremental_column: str = None,
    last_loaded_value=None
):
    jdbc_url = (
        f"jdbc:sqlserver://{server_name}.database.windows.net:1433;"
        f"database={database_name};"
        "encrypt=true;"
        "trustServerCertificate=false;"
        "loginTimeout=30;"
    )

    if incremental_column and last_loaded_value:
        query = (
            f"(SELECT * FROM {table_name} "
            f"WHERE {incremental_column} > '{last_loaded_value}') AS src"
        )
    else:
        query = table_name

    return (
        spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", query)
        .option("user", username)
        .option("password", password)
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .load()
    )


# -------------------------------------------------
# BRONZE DLT TABLE CREATOR
# -------------------------------------------------
def create_bronze_table(table_meta: dict, global_conf: dict):

    bronze_conf = table_meta["layers"]["bronze"]
    source = bronze_conf["source"]

    server = global_conf["azure_sql"]["server"]
    username = global_conf["azure_sql"]["username"]
    password = global_conf["azure_sql"]["password"]

    source_table = f"{source['schema']}.{source['table']}"
    bronze_table_name = bronze_conf["target"]["table"]

    incremental_column = None
    if bronze_conf.get("incremental", {}).get("enabled"):
        incremental_column = bronze_conf["incremental"]["column"]

    @dlt.table(
        name=bronze_table_name,
        comment=f"Bronze table for {source_table}"
    )
    def bronze_table():

        # NOTE: Incremental watermark handled later in Silver
        df = read_from_azure_sql_jdbc(
            server_name=server,
            database_name=source["database"],
            table_name=source_table,
            username=username,
            password=password,
            incremental_column=incremental_column,
            last_loaded_value=None  # Bronze is stateless
        )

        return df
