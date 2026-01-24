from pyspark.sql.functions import *
from pyspark.sql.types import DateType
from pyspark.sql.functions import sha2, col
import dlt
def generic_silver_cleaning(df):

    # Drop exact duplicate rows
    df = df.dropDuplicates()

    # # 2️ Trim all string columns
    # for col_name, dtype in df.dtypes:
    #     if dtype == "string":
    #         df = df.withColumn(col_name, trim(col(col_name)))

    # Replace empty strings with NULL
    df = df.replace("", None)

    # 4 Convert date-like columns to DATE type
    for col_name, dtype in df.dtypes:
        if "date" in col_name.lower() and dtype == "string":
            df = df.withColumn(col_name, to_date(col(col_name)))

    # 5 Add audit column
    df = df.withColumn("silver_processed_ts", current_timestamp())

    return df
# make all column name to lower case 
def lowercase_columns(df):
    for col_name in df.columns:
        df = df.withColumnRenamed(col_name, col_name.lower())
    return df
# mask personal indentification information
def mask_pii(df, pii_columns: list):
    for col_name in pii_columns:
        df = df.withColumn(col_name, sha2(col(col_name), 256))
    return df

# cast one column to another with dictionary basic 
def cast_columns(df, cast_map: dict):
    for col_name, dtype in cast_map.items():
        df = df.withColumn(col_name, col(col_name).cast(dtype))
    return df
#fill value with specific mention value 
def handle_nulls(df, fill_map=None):
    return df.fillna(fill_map) if fill_map else df

#row level filterin on the basic of filter condition 
def apply_row_filters(df, filter_condition: str):
    return df.filter(filter_condition)

# surrogate_key addition 
def add_surrogate_key_from_natural_key(df, natural_key: str, sk_name: str):
    """
    Create a deterministic surrogate key from a single natural key
    (Streaming & SCD safe)
    """
    return df.withColumn(
        sk_name,
        sha2(col(natural_key).cast("string"), 256)
    )


# primary key check validation 
def enforce_fk(df, fk_column: str):
    return df.filter(col(fk_column).isNotNull())

#genric join 
def join_tables(
    left_df,
    right_df,
    join_condition,
    join_type="inner"
):
    return left_df.join(right_df, join_condition, join_type)
# slow chaning dimension but currently does't work due to unspport of dlt need some change in parameter 
def apply_scd_type2(
    target_table: str,
    source_stream: str,
    keys: list,
    sequence_col: str
):
    dlt.apply_changes(
        target=target_table,
        source=source_stream,
        keys=keys,
        sequence_by=col(sequence_col),
        stored_as_scd_type=2
    )
#apply mutliple rule to apply business rule
# rules _ example  = [
#     "quantity > 0",
#     "totaldue >= 0"
# ]
    
def apply_business_rules(df, rules: list):
    for rule in rules:
        df = df.filter(rule)
    return df

    
#data filterr by default last 7 days 
def add_freshness_flag(df, date_col: str, threshold_days=7):
    return df.withColumn(
        "is_fresh",
        datediff(current_date(), col(date_col)) <= threshold_days
    )
def add_surrogate_key_from_composite_keys(df, natural_keys: list, sk_name: str):
    """
    Create surrogate key from multiple natural keys
    """
    return df.withColumn(
        sk_name,
        sha2(
            concat_ws("||", *[col(k).cast("string") for k in natural_keys]),
            256
        )
    )
def drop_columns(df, columns: list):
    """
    Drop unwanted / duplicate columns from dataframe
    """
    for c in columns:
        if c in df.columns:
            df = df.drop(c)
    return df









