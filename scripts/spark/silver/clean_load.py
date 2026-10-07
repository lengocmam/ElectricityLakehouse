from pyspark.sql import DataFrame
from pyspark.sql.functions import col, date_add, lit, lower, to_date, to_timestamp

from silver.silver_utils import add_dq, normalize_text


def clean_load(
    df: DataFrame,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
) -> tuple[DataFrame, DataFrame]:
    """Execute clean load logic."""
    prepared = (
        df.withColumn("region", normalize_text(lower("region")))
        .withColumn("data_date", to_date("observation_timestamp"))
    )

    range_conditions = [
        (col("load_mw") < 0, "LOAD_MW_NEGATIVE"),
    ]

    if "source_data_date" in df.columns:
        start_ts = to_timestamp(col("source_data_date"))
        end_ts = to_timestamp(date_add(to_date(col("source_data_date")), 1))
        range_conditions.append((
            (col("observation_timestamp") < start_ts) | (col("observation_timestamp") >= end_ts),
            "EVENT_TIME_OUT_OF_RANGE",
        ))
    if start_date is not None:
        range_conditions.append((
            col("observation_timestamp") < to_timestamp(lit(start_date)),
            "EVENT_TIME_OUT_OF_RANGE",
        ))
    if end_date is not None:
        range_conditions.append((
            col("observation_timestamp") >= to_timestamp(lit(end_date)),
            "EVENT_TIME_OUT_OF_RANGE",
        ))

    return add_dq(
        prepared, key_name="load_key", key_columns=["observation_timestamp", "region"],
        hash_columns=["observation_timestamp", "region", "load_mw"],
        required_columns={
            "bronze_key": "BRONZE_KEY_NULL",
            "observation_timestamp": "EVENT_TIME_NULL",
            "region": "REGION_NULL",
            "load_mw": "SYSTEM_LOAD_NULL",
        },
        range_conditions=range_conditions,
        business_conditions=[
            (~col("region").isin("north", "central", "south", "national"), "REGION_INVALID"),
        ],
        dedup_order_by=[col("bronze_key").desc()],
    )
