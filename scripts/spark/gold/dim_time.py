from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, when


def generate_dim_time(spark: SparkSession) -> DataFrame:
    """Generate time dimension table for 24 hours (dim_time)."""
    hours_df = spark.range(0, 24).select(col("id").cast("int").alias("hour"))

    time_period = (
        when(col("hour").between(6, 11), "morning")
        .when(col("hour").between(12, 17), "afternoon")
        .when(col("hour").between(18, 21), "evening")
        .otherwise("night")
    )

    return hours_df.select(
        col("hour").alias("time_key"),
        col("hour"),
        time_period.alias("time_period"),
    )
