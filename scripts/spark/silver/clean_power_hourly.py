"""Clean and reject NSMO power hourly without changing source values."""

from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from silver.silver_utils import add_dq, normalize_text, stable_hash


def clean_power_hourly(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    prepared = (
        df.withColumn("dataset_name", F.lit("nsmo_power"))
        .withColumn("generation_source_identity", normalize_text(F.col("generation_source_name")))
        .withColumn("raw_source_key", stable_hash(["dataset_name", "generation_source_identity"]))
        .withColumn("data_date", F.to_date("observation_timestamp_local"))
        .withColumn("unit_status", F.lit("ASSUMED_MW"))
        .withColumn("contract_version", F.lit("NSMO_MIX_V1"))
        .withColumn("report_version", F.col("source_payload_hash"))
    )
    valid, reject = add_dq(
        prepared,
        key_name="generation_mix_key",
        key_columns=["dataset_name", "generation_source_identity", "source_timestamp_raw"],
        hash_columns=["report_date", "generation_source_name", "generation_source_identity",
                      "source_timestamp_raw", "generation_power_mw", "unit_status"],
        required_columns={
            "bronze_key": "BRONZE_KEY_NULL",
            "report_date": "REPORT_DATE_NULL",
            "generation_source_name": "SOURCE_NULL",
            "source_timestamp_raw": "TIMESTAMP_NULL",
            "observation_timestamp_local": "EVENT_TIME_NULL",
            "generation_power_mw": "POWER_NULL",
        },
        business_conditions=[
            (F.col("validation_error").isNotNull(), "REPORT_INVALID"),
            (F.col("generation_source_identity") == "", "SOURCE_EMPTY"),
        ],
        range_conditions=[
            (F.col("generation_power_mw") < 0, "POWER_NEGATIVE"),
        ],
    )
    return valid, reject
