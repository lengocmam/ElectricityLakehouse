from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

from gold.gold_utils import build_date_key
from gold.fact_generation_daily import map_energy_source_key


def map_period_key(col: F.Column) -> F.Column:
    """Map period name to Dim_period key."""
    return (
        F.when(F.lower(F.trim(col)) == "midday_off_peak", 1)
        .when(F.lower(F.trim(col)) == "evening_peak", 2)
        .otherwise(1)
    ).cast("int")


def transform_fact_generation_capacity_daily(
    power_df: DataFrame,
) -> DataFrame:
    """Transform silver dispatched power data into fact_generation_capacity_daily."""
    enriched = (
        power_df
        .withColumn("date_key", build_date_key(F.col("data_date")))
        .withColumn("energy_source_key", map_energy_source_key(F.col("generation_source")))
        .withColumn("period_key", map_period_key(F.col("period")))
        .withColumn("capacity_mw", F.col("dispatched_power_mw").cast("float"))
    )

    day_period_window = Window.partitionBy("date_key", "period_key")
    source_period_window = Window.partitionBy("energy_source_key", "period_key").orderBy("date_key")
    # Deterministic surrogate key: parallel O(1), eliminates single-partition shuffle bottleneck
    surrogate_key = F.abs(F.hash(F.col("date_key"), F.col("period_key"), F.col("energy_source_key"))).cast("int")

    total_period_cap = F.sum("capacity_mw").over(day_period_window)

    result = (
        enriched
        .withColumn(
            "capacity_share_pct",
            F.when(total_period_cap > 0, F.round(F.col("capacity_mw") / total_period_cap * 100.0, 2)).otherwise(0.0).cast("float"),
        )
        .withColumn(
            "previous_capacity_mw",
            F.lag("capacity_mw", 1).over(source_period_window).cast("float"),
        )
        .withColumn(
            "capacity_change_mw",
            F.round(F.col("capacity_mw") - F.col("previous_capacity_mw"), 2).cast("float"),
        )
        .withColumn(
            "capacity_growth_pct",
            F.when(
                (F.col("previous_capacity_mw").isNotNull()) & (F.col("previous_capacity_mw") != 0),
                F.round(F.col("capacity_change_mw") / F.col("previous_capacity_mw") * 100.0, 2),
            ).otherwise(None).cast("float"),
        )
        .withColumn(
            "source_capacity_rank",
            F.dense_rank().over(day_period_window.orderBy(F.col("capacity_mw").desc())).cast("int"),
        )
        .withColumn(
            "capacity_key",
            surrogate_key,
        )
        .select(
            "capacity_key",
            "date_key",
            "energy_source_key",
            "period_key",
            "capacity_mw",
            "capacity_share_pct",
            "previous_capacity_mw",
            "capacity_change_mw",
            "capacity_growth_pct",
            "source_capacity_rank",
        )
    )

    return result
