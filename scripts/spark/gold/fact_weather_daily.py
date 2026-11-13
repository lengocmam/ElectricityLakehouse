from pyspark.sql import DataFrame
from pyspark.sql.window import Window

from gold.gold_utils import build_date_key
from pyspark.sql.functions import (
    abs, broadcast, col,
    hash, lower, trim,
)


def transform_fact_weather_daily(
    weather_daily_df: DataFrame,
    dim_location_df: DataFrame,
) -> DataFrame:
    """Transform silver daily weather data into fact_weather_daily."""
    enriched = (
        weather_daily_df
        .join(
            broadcast(dim_location_df),
            trim(lower(weather_daily_df.location_name)) == trim(lower(dim_location_df.province)),
            "inner",
        )
        .withColumn("date_key", build_date_key(col("data_date")))
    )

    # Deterministic surrogate key: parallel O(1), eliminates single-partition shuffle bottleneck
    surrogate_key = abs(hash(col("date_key"), col("location_key"))).cast("int")

    result = (
        enriched
        .withColumn("weather_daily_key", surrogate_key)
        .withColumn("temperature_2m_mean", col("temperature_2m_mean").cast("float"))
        .withColumn("temperature_2m_max", col("temperature_2m_max").cast("float"))
        .withColumn("temperature_2m_min", col("temperature_2m_min").cast("float"))
        .withColumn("precipitation_sum", col("precipitation_sum").cast("float"))
        .withColumn("precipitation_hours", col("precipitation_hours").cast("float"))
        .withColumn("sunshine_duration", col("sunshine_duration").cast("float"))
        .withColumn("cloud_cover_mean", col("cloud_cover_mean").cast("float"))
        .withColumn("shortwave_radiation_sum", col("shortwave_radiation_sum").cast("float"))
        .withColumn("wind_speed_10m_max", col("wind_speed_10m_max").cast("float"))
        .select(
            "weather_daily_key",
            "date_key",
            "location_key",
            "temperature_2m_mean",
            "temperature_2m_max",
            "temperature_2m_min",
            "precipitation_sum",
            "precipitation_hours",
            "sunshine_duration",
            "cloud_cover_mean",
            "shortwave_radiation_sum",
            "wind_speed_10m_max",
        )
    )

    return result
