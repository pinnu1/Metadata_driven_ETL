def customer_sales_template(meta: dict):
    dims = ", ".join(meta["dimensions"])

    measures_sql = []
    for alias, expr in meta["measures"].items():
        measures_sql.append(f"{expr} AS {alias}")

    measures = ", ".join(measures_sql)

    return f"""
        SELECT
            {dims},
            {measures}
        FROM {meta["input_table"]}
        GROUP BY {dims}
    """
