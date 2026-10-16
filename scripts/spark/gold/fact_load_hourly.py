from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

from gold.gold_utils import build_date_key, build_time_key


def transform_fact_load_hourly(
    load_df: DataFrame,
    dim_region_df: DataFrame,
) -> DataFrame:
    """Transform silver load data into fact_load_hourly."""
    enriched = (
        load_df
        .join(
            F.broadcast(dim_region_df),
            F.trim(F.lower(load_df.region)) == F.trim(F.lower(dim_region_df.region_name)),
            "inner",
        )
        .withColumn("date_key", build_date_key(F.col("observation_timestamp")))
        .withColumn("time_key", build_time_key(F.col("observation_timestamp")))
    )

    day_region_window = Window.partitionBy("date_key", "region_key")
    ts_window = Window.partitionBy("region_key").orderBy("observation_timestamp")
    # Deterministic surrogate key: parallel O(1), eliminates single-partition shuffle bottleneck
    surrogate_key = F.abs(F.hash(F.col("date_key"), F.col("time_key"), F.col("region_key"))).cast("int")

    result = (
        enriched
        .withColumn("load_mw", F.col("load_mw").cast("float"))
        .withColumn(
            "daily_avg_load_mw",
            F.round(F.avg("load_mw").over(day_region_window), 2).cast("float"),
        )
        .withColumn(
            "daily_min_load_mw",
            F.min("load_mw").over(day_region_window).cast("float"),
        )
        .withColumn(
            "daily_peak_load_mw",
            F.max("load_mw").over(day_region_window).cast("float"),
        )
        .withColumn(
            "previous_hour_load_mw",
            F.lag("load_mw", 1).over(ts_window).cast("float"),
        )
        .withColumn(
            "load_change_mw",
            F.round(F.col("load_mw") - F.col("previous_hour_load_mw"), 2).cast("float"),
        )
        .withColumn(
            "load_rank_in_day",
            F.dense_rank().over(day_region_window.orderBy(F.col("load_mw").desc())).cast("int"),
        )
        .withColumn(
            "load_key",
            surrogate_key,
        )
        .select(
            "load_key",
            "date_key",
            "time_key",
            "region_key",
            "load_mw",
            "daily_avg_load_mw",
            "daily_min_load_mw",
            "daily_peak_load_mw",
            "previous_hour_load_mw",
            "load_change_mw",
            "load_rank_in_day",
        )
    )

    return result
