from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lower

from silver.silver_utils import add_dq, normalize_text


def clean_power(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Execute clean power logic."""
    prepared = (
        df.withColumn("period", normalize_text(lower("period")))
        .withColumn("generation_source", normalize_text("generation_source"))
    )
    return add_dq(
        prepared, key_name="evn_power_key", key_columns=["data_date", "generation_source", "period"],
        hash_columns=["data_date", "generation_source", "period", "dispatched_power_mw"],
        required_columns={
            "bronze_key": "BRONZE_KEY_NULL",
            "data_date": "DATA_DATE_NULL",
            "generation_source": "SOURCE_TYPE_NULL",
            "period": "LOAD_PERIOD_NULL",
            "dispatched_power_mw": "DISPATCHED_POWER_NULL",
        },
        range_conditions=[
            (col("dispatched_power_mw") < 0, "DISPATCHED_POWER_NEGATIVE"),
        ],
        business_conditions=[
            (~col("period").isin("midday_off_peak", "evening_peak"), "LOAD_PERIOD_INVALID"),
        ],
        dedup_order_by=[col("bronze_key").desc()],
    )
