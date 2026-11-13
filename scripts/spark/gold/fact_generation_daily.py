from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

from gold.gold_utils import build_date_key
from gold.dim_energy_source import source_key_from_name


def transform_fact_generation_daily(
    generation_df: DataFrame,
) -> DataFrame:
    """Transform silver generation data into fact_generation_daily."""
    enriched = (
        generation_df
        .withColumn("date_key", build_date_key(F.col("data_date")))
        .withColumn("energy_source_key", source_key_from_name(F.col("energy_source")))
        .withColumn("generation_mwh", F.round(F.col("generation_million_kwh") * 1000.0, 2).cast("float"))
    )

    day_window = Window.partitionBy("date_key")
    source_window = Window.partitionBy("energy_source_key").orderBy("date_key")
    # Deterministic surrogate key: parallel O(1), eliminates single-partition shuffle bottleneck
    surrogate_key = F.abs(F.hash(F.col("date_key"), F.col("energy_source_key"))).cast("int")

    day_total = F.sum("generation_mwh").over(day_window)

    result = (
        enriched
        .withColumn(
            "generation_share_pct",
            F.when(day_total > 0, F.round(F.col("generation_mwh") / day_total * 100.0, 2)).otherwise(0.0).cast("float"),
        )
        .withColumn(
            "previous_day_generation_mwh",
            F.lag("generation_mwh", 1).over(source_window).cast("float"),
        )
        .withColumn(
            "generation_change_mwh",
            F.round(F.col("generation_mwh") - F.col("previous_day_generation_mwh"), 2).cast("float"),
        )
        .withColumn(
            "generation_growth_pct",
            F.when(
                (F.col("previous_day_generation_mwh").isNotNull()) & (F.col("previous_day_generation_mwh") != 0),
                F.round(F.col("generation_change_mwh") / F.col("previous_day_generation_mwh") * 100.0, 2),
            ).otherwise(None).cast("float"),
        )
        .withColumn(
            "source_rank_in_day",
            F.dense_rank().over(day_window.orderBy(F.col("generation_mwh").desc())).cast("int"),
        )
        .withColumn(
            "generation_fact_key",
            surrogate_key,
        )
        .select(
            "generation_fact_key",
            "date_key",
            "energy_source_key",
            "generation_mwh",
            "generation_share_pct",
            "previous_day_generation_mwh",
            "generation_change_mwh",
            "generation_growth_pct",
            "source_rank_in_day",
        )
    )

    return result
