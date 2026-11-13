from pyspark.sql import DataFrame, SparkSession

from gold.gold_utils import build_date_key
from pyspark.sql.functions import (
    col, date_format, dayofmonth, dayofweek, explode, expr, lit,
    month, quarter, sequence, to_date, weekofyear, when,
)


def generate_dim_date(
    spark: SparkSession,
    start_date: str = "2020-01-01",
    end_date: str = "2030-12-31",
) -> DataFrame:
    """Generate calendar dimension table (dim_date)."""
    date_df = spark.range(1).select(
        explode(
            sequence(
                to_date(lit(start_date)),
                to_date(lit(end_date)),
                expr("interval 1 day"),
            )
        ).alias("full_date")
    )

    # In Spark dayofweek: 1=Sunday, 2=Monday, ..., 7=Saturday
    # Transform day_of_week to ISO standard: 1=Monday .. 7=Sunday
    iso_day_of_week = (
        when(dayofweek("full_date") == 1, 7)
        .otherwise(dayofweek("full_date") - 1)
        .cast("int")
    )

    # Season mapping: 1 (Spring: 3-5), 2 (Summer: 6-8), 3 (Autumn: 9-11), 4 (Winter: 12, 1, 2)
    season = (
        when(month("full_date").isin(3, 4, 5), 1)
        .when(month("full_date").isin(6, 7, 8), 2)
        .when(month("full_date").isin(9, 10, 11), 3)
        .otherwise(4)
        .cast("int")
    )

    return date_df.select(
        build_date_key(col("full_date")).alias("date_key"),
        col("full_date"),
        dayofmonth("full_date").cast("int").alias("day"),
        month("full_date").cast("int").alias("month"),
        date_format("full_date", "MMMM").alias("month_name"),
        quarter("full_date").cast("int").alias("quarter"),
        iso_day_of_week.alias("day_of_week"),
        date_format("full_date", "EEEE").alias("day_name"),
        weekofyear("full_date").cast("int").alias("week_of_year"),
        dayofweek("full_date").isin(1, 7).alias("is_weekend"),
        season.alias("season"),
    )
