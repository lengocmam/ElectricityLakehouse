from pyspark.sql import DataFrame
from pyspark.sql.window import Window

from gold.gold_utils import build_date_key
from gold.dim_energy_source import source_key_from_name
from pyspark.sql.functions import (
    abs, col, dense_rank, hash,
    lag, round, sum, when,
)


def transform_fact_generation_daily(
    generation_df: DataFrame,
) -> DataFrame:
    """Daily generation energy in MWh, by source."""
    enriched = (
        generation_df
        .withColumn("date_key", build_date_key(col("data_date")))
        .withColumn("energy_source_key", source_key_from_name(col("energy_source"), "EVN"))
        .withColumn("generation_mwh", round(col("generation_million_kwh") * 1000.0, 2).cast("float"))
    )

    day_window = Window.partitionBy("date_key")
    source_window = Window.partitionBy("energy_source_key").orderBy("date_key")
    # Deterministic surrogate key: parallel O(1), eliminates single-partition shuffle bottleneck
    surrogate_key = abs(hash(col("date_key"), col("energy_source_key"))).cast("int")

    day_total = sum("generation_mwh").over(day_window)

    result = (
        enriched
        .withColumn(
            "generation_share_pct",
            when(day_total > 0, round(col("generation_mwh") / day_total * 100.0, 2)).otherwise(0.0).cast("float"),
        )
        .withColumn(
            "previous_generation_mwh",
            lag("generation_mwh", 1).over(source_window).cast("float"),
        )
        .withColumn(
            "generation_change_mwh",
            round(col("generation_mwh") - col("previous_generation_mwh"), 2).cast("float"),
        )
        .withColumn(
            "generation_growth_pct",
            when(
                (col("previous_generation_mwh").isNotNull()) & (col("previous_generation_mwh") != 0),
                round(col("generation_change_mwh") / col("previous_generation_mwh") * 100.0, 2),
            ).otherwise(None).cast("float"),
        )
        .withColumn(
            "generation_rank_in_day",
            dense_rank().over(day_window.orderBy(col("generation_mwh").desc())).cast("int"),
        )
        .withColumn(
            "generation_key",
            surrogate_key,
        )
        .select(
            "generation_key",
            "date_key",
            "energy_source_key",
            "generation_mwh",
            "generation_share_pct",
            "previous_generation_mwh",
            "generation_change_mwh",
            "generation_growth_pct",
            "generation_rank_in_day",
        )
    )

    return result
