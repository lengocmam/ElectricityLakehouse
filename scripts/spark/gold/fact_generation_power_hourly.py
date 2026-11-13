"""NSMO generation power at exact HH:00 readings, by original source."""

from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from silver.silver_utils import stable_hash
from gold.dim_energy_source import source_key_from_name


MAPPING_VERSION = "SOURCE_NAME_KEY_V3"


def transform_fact_generation_power_hourly(source_df: DataFrame) -> DataFrame:
    timestamp = F.col("observation_timestamp_local")
    hourly = source_df.filter(
        (F.minute(timestamp) == 0) & (F.second(timestamp) == 0)
    )
    energy_key = source_key_from_name(F.col("generation_source_name"))
    prepared = (
        hourly.withColumn("energy_source_key", energy_key)
        .withColumn("mapping_status", F.when(energy_key.isNull(), "UNMAPPED").otherwise("SOURCE_NAME_KEY"))
        .withColumn("mapping_version", F.lit(MAPPING_VERSION))
        .withColumn("sampling_policy", F.lit("EXACT_HOUR_V1"))
        .withColumn("date_key", F.date_format("data_date", "yyyyMMdd").cast("int"))
        .withColumn("time_key", F.hour(timestamp).cast("int"))
        .withColumn("report_date_key", F.date_format("report_date", "yyyyMMdd").cast("int"))
    )
    prepared = prepared.withColumn(
        "gold_row_hash", stable_hash([
            "generation_mix_key", "row_hash", "mapping_version",
            "energy_source_key", "sampling_policy",
        ])
    )
    return prepared.select(
        "generation_mix_key", "report_date", "report_date_key", "data_date",
        "date_key", "time_key", "observation_timestamp_local",
        "raw_source_key", "generation_source_name", "energy_source_key",
        "mapping_status", "mapping_version", "generation_power_mw", "unit_status",
        "sampling_policy", "bronze_key", "source_payload_hash",
        "report_version", "row_hash", "gold_row_hash", "contract_version",
        "dq_warning_code",
    )

