"""NSMO generation power at exact HH:00 readings, by original source."""

from pyspark.sql import DataFrame
from pyspark.sql.window import Window

from gold.dim_energy_source import source_key_from_name
from pyspark.sql.functions import (
    abs, col, date_format, dense_rank, expr, hash, hour,
    lag, minute, round, second, sum, when,
)

FACT_COLUMNS = [
    "generation_power_key", "date_key", "time_key", "energy_source_key",
    "generation_power_mw", "generation_power_share_pct", "previous_generation_power_mw",
    "generation_power_change_mw", "generation_power_growth_pct", "generation_power_rank_in_hour",
]


def transform_fact_generation_power_hourly(source_df: DataFrame) -> DataFrame:
    timestamp = col("observation_timestamp_local")
    hourly = source_df.filter(
        (minute(timestamp) == 0) & (second(timestamp) == 0)
    )
    energy_key = source_key_from_name(col("generation_source_name"), "NSMO")
    surrogate_key = abs(hash(col("date_key"), col("time_key"), col("energy_source_key"))).cast("int")
    prepared = (
        hourly.withColumn("energy_source_key", energy_key)
        .withColumn("date_key", date_format("data_date", "yyyyMMdd").cast("int"))
        .withColumn("time_key", hour(timestamp).cast("int"))
        .withColumn("generation_power_key", surrogate_key)
    )
    hour_window = Window.partitionBy("date_key", "time_key")
    source_window = Window.partitionBy("energy_source_key").orderBy(timestamp)
    total_power = sum("generation_power_mw").over(hour_window)
    return (
        prepared.withColumn("_previous_timestamp", lag(timestamp).over(source_window))
        .withColumn(
            "previous_generation_power_mw",
            when(timestamp == col("_previous_timestamp") + expr("INTERVAL 1 HOUR"),
                 lag("generation_power_mw").over(source_window)),
        )
        .withColumn("generation_power_share_pct", when(
            total_power > 0, round(col("generation_power_mw") / total_power * 100.0, 2),
        ).otherwise(0.0).cast("float"))
        .withColumn("generation_power_change_mw", round(
            col("generation_power_mw") - col("previous_generation_power_mw"), 2,
        ).cast("float"))
        .withColumn("generation_power_growth_pct", when(
            col("previous_generation_power_mw") != 0,
            round(col("generation_power_change_mw") / col("previous_generation_power_mw") * 100.0, 2),
        ).cast("float"))
        .withColumn("previous_generation_power_mw", col("previous_generation_power_mw").cast("float"))
        .withColumn("generation_power_rank_in_hour", dense_rank().over(
            hour_window.orderBy(col("generation_power_mw").desc()),
        ).cast("int"))
        .select(*FACT_COLUMNS)
    )

