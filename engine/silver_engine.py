import dlt
from pyspark.sql import SparkSession

from utils.silver_common_utils import (
    generic_silver_cleaning,
    lowercase_columns,
    mask_pii,
    cast_columns,
    handle_nulls,
    apply_row_filters,
    add_surrogate_key_from_natural_key,
    add_surrogate_key_from_composite_keys,
    apply_business_rules,
    add_freshness_flag,
    drop_columns
)
from utils.silver_common_utils import *


spark = SparkSession.builder.getOrCreate()


# -------------------------------------------------
# STEP DISPATCHER
# -------------------------------------------------
def apply_silver_step(df, step: dict):
    step_type = step["step_type"]
    options = step.get("options", {})

    if step_type == "generic_cleaning":
        return generic_silver_cleaning(df)

    if step_type == "lowercase_columns":
        return lowercase_columns(df)

    if step_type == "mask_pii":
        return mask_pii(df, options["columns"])

    if step_type == "cast_columns":
        return cast_columns(df, options)

    if step_type == "handle_nulls":
        return handle_nulls(df, options.get("fill_map"))

    if step_type == "row_filter":
        return apply_row_filters(df, options["condition"])

    if step_type == "add_surrogate_key":
        return add_surrogate_key_from_natural_key(
            df,
            natural_key=options["natural_key"],
            sk_name=options["sk_name"]
        )

    if step_type == "add_composite_surrogate_key":
        return add_surrogate_key_from_composite_keys(
            df,
            natural_keys=options["natural_keys"],
            sk_name=options["sk_name"]
        )

    if step_type == "business_rules":
        return apply_business_rules(df, options["rules"])

    if step_type == "freshness_flag":
        return add_freshness_flag(
            df,
            date_col=options["date_col"],
            threshold_days=options.get("threshold_days", 7)
        )
    if step_type == "apply_row_filters":
        return apply_row_filters(df, options["condition"])
    raise ValueError(f"Unsupported silver step: {step_type}")


# -------------------------------------------------
# SILVER TABLE CREATOR
# -------------------------------------------------
def create_silver_table(table_meta: dict):

    silver_conf = table_meta["layers"]["silver"]
    input_table = silver_conf["input"]
    output_table = silver_conf["output"]["table"]
    steps = silver_conf["steps"]

    @dlt.table(
        name=output_table,
        comment=f"Silver table for {table_meta['table_name']}"
    )
    def silver_table():


        # Always read Bronze as stream
        if silver_conf.get("table_type") == "dimension":
            df = dlt.read(input_table)
        else:
            df = dlt.read_stream(input_table)
        # Apply steps in order
        for step in steps:
            df = apply_silver_step(df, step)

        return df
    # -------------------------------------------------
# SILVER JOIN TABLE CREATOR
# -------------------------------------------------
def create_silver_join_table(table_meta: dict):

    silver_conf = table_meta["layers"]["silver"]
    input_table = silver_conf["input"]
    joins = silver_conf["joins"]
    output_table = silver_conf["output"]["table"]

    @dlt.table(
        name=output_table,
        comment=f"Silver join table for {table_meta['table_name']}"
    )
    def silver_join_table():

        df = dlt.read_stream(input_table)

        for join_conf in joins:
            right_df = dlt.read(join_conf["table"])

            # 🔽 DROP COLUMNS IF DEFINED IN METADATA
            if "drop_columns" in join_conf:
                right_df = drop_columns(
                    right_df,
                    join_conf["drop_columns"]
                )

            df = df.join(
                right_df,
                on=join_conf["on"],
                how=join_conf.get("join_type", "left")
            )

        return df
    




