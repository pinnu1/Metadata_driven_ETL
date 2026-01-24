import dlt
from pyspark.sql import SparkSession

def create_gold_table(table_meta: dict):

    gold = table_meta["layers"]["gold"]
    kpi_type = gold["kpi_type"]
    output_table = gold["output"]["table"]

    if kpi_type == "sql":
        sql_query = gold["sql"]

    elif kpi_type == "template":
        from engine.gold_kpi_templates import TEMPLATE_REGISTRY

        template_name = gold["template_name"]
        template_fn = TEMPLATE_REGISTRY.get(template_name)

        if not template_fn:
            raise ValueError(f"Unknown KPI template: {template_name}")

        sql_query = template_fn(gold)

    else:
        raise ValueError(f"Unsupported kpi_type: {kpi_type}")

    @dlt.table(
        name=output_table,
        comment=f"Gold KPI table: {table_meta['table_name']}"
    )
    def gold_table():
        spark = SparkSession.getActiveSession()
        return spark.sql(sql_query)
