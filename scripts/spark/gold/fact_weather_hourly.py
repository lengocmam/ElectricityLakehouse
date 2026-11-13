from pyspark.sql import DataFrame
from pyspark.sql.window import Window

from gold.gold_utils import build_date_key, build_time_key
from pyspark.sql.functions import (
    abs, broadcast, col,
    hash, lower, trim,
)


def transform_fact_weather_hourly(
    weather_hourly_df: DataFrame,
    dim_location_df: DataFrame,
) -> DataFrame:
    """Transform silver hourly weather data into fact_weather_hourly."""
    enriched = (
        weather_hourly_df
        .join(
            broadcast(dim_location_df),
            trim(lower(weather_hourly_df.location_name)) == trim(lower(dim_location_df.province)),
            "inner",
        )
        .withColumn("date_key", build_date_key(col("data_date")))
        .withColumn("time_key", build_time_key(col("observation_timestamp")))
    )

    # Deterministic surrogate key: parallel O(1), eliminates single-partition shuffle bottleneck
    surrogate_key = abs(hash(col("date_key"), col("time_key"), col("location_key"))).cast("int")

    result = (
        enriched
        .withColumn("weather_hourly_key", surrogate_key)
        .withColumn("temperature_2m", col("temperature_2m").cast("float"))
        .withColumn("apparent_temperature", col("apparent_temperature").cast("float"))
        .withColumn("relative_humidity_2m", col("relative_humidity_2m").cast("float"))
        .withColumn("precipitation", col("precipitation").cast("float"))
        .withColumn("cloud_cover", col("cloud_cover").cast("float"))
        .withColumn("shortwave_radiation", col("shortwave_radiation").cast("float"))
        .withColumn("wind_speed_10m", col("wind_speed_10m").cast("float"))
        .withColumn("weather_code", col("weather_code").cast("int"))
        .select(
            "weather_hourly_key",
            "date_key",
            "time_key",
            "location_key",
            "temperature_2m",
            "apparent_temperature",
            "relative_humidity_2m",
            "precipitation",
            "cloud_cover",
            "shortwave_radiation",
            "wind_speed_10m",
            "weather_code",
        )
    )

    return result
