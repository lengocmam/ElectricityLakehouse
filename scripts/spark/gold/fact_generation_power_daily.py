from pyspark.sql import DataFrame
from pyspark.sql.window import Window

from gold.gold_utils import build_date_key
from gold.dim_energy_source import source_key_from_name
from pyspark.sql.functions import (
    Column, abs, col, dense_rank, hash, lag,
    lower, round, sum, trim, when,
)


def map_period_key(col: Column) -> Column:
    """Map period name to Dim_period key."""
    return (
        when(lower(trim(col)) == "midday_off_peak", 1)
        .when(lower(trim(col)) == "evening_peak", 2)
        .otherwise(1)
    ).cast("int")


def transform_fact_generation_power_daily(
    power_df: DataFrame,
) -> DataFrame:
    """Dispatched generation power in MW, by day, period, and source."""
    enriched = (
        power_df
        .withColumn("date_key", build_date_key(col("data_date")))
        .withColumn("energy_source_key", source_key_from_name(col("generation_source"), "EVN"))
        .withColumn("period_key", map_period_key(col("period")))
        .withColumn("generation_power_mw", col("dispatched_power_mw").cast("float"))
    )

    day_period_window = Window.partitionBy("date_key", "period_key")
    source_period_window = Window.partitionBy("energy_source_key", "period_key").orderBy("date_key")
    # Deterministic surrogate key: parallel O(1), eliminates single-partition shuffle bottleneck
    surrogate_key = abs(hash(col("date_key"), col("period_key"), col("energy_source_key"))).cast("int")

    period_total_power = sum("generation_power_mw").over(day_period_window)

    result = (
        enriched
        .withColumn(
            "generation_power_share_pct",
            when(period_total_power > 0, round(col("generation_power_mw") / period_total_power * 100.0, 2)).otherwise(0.0).cast("float"),
        )
        .withColumn(
            "previous_generation_power_mw",
            lag("generation_power_mw", 1).over(source_period_window).cast("float"),
        )
        .withColumn(
            "generation_power_change_mw",
            round(col("generation_power_mw") - col("previous_generation_power_mw"), 2).cast("float"),
        )
        .withColumn(
            "generation_power_growth_pct",
            when(
                (col("previous_generation_power_mw").isNotNull()) & (col("previous_generation_power_mw") != 0),
                round(col("generation_power_change_mw") / col("previous_generation_power_mw") * 100.0, 2),
            ).otherwise(None).cast("float"),
        )
        .withColumn(
            "generation_power_rank_in_period",
            dense_rank().over(day_period_window.orderBy(col("generation_power_mw").desc())).cast("int"),
        )
        .withColumn(
            "generation_power_key",
            surrogate_key,
        )
        .select(
            "generation_power_key",
            "date_key",
            "energy_source_key",
            "period_key",
            "generation_power_mw",
            "generation_power_share_pct",
            "previous_generation_power_mw",
            "generation_power_change_mw",
            "generation_power_growth_pct",
            "generation_power_rank_in_period",
        )
    )

    return result
