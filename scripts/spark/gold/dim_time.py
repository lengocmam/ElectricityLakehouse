from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F


def generate_dim_time(spark: SparkSession) -> DataFrame:
    """Generate time dimension table for 24 hours (dim_time)."""
    hours_df = spark.range(0, 24).select(F.col("id").cast("int").alias("hour"))

    time_period = (
        F.when(F.col("hour").between(6, 11), "morning")
        .when(F.col("hour").between(12, 17), "afternoon")
        .when(F.col("hour").between(18, 21), "evening")
        .otherwise("night")
    )

    return hours_df.select(
        F.col("hour").alias("time_key"),
        F.col("hour"),
        time_period.alias("time_period"),
    )
