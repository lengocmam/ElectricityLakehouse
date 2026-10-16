from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

from gold.gold_utils import build_date_key


def transform_fact_weather_daily(
    weather_daily_df: DataFrame,
    dim_location_df: DataFrame,
) -> DataFrame:
    """Transform silver daily weather data into fact_weather_daily."""
    enriched = (
        weather_daily_df
        .join(
            F.broadcast(dim_location_df),
            F.trim(F.lower(weather_daily_df.location_name)) == F.trim(F.lower(dim_location_df.province)),
            "inner",
        )
        .withColumn("date_key", build_date_key(F.col("data_date")))
    )

    # Deterministic surrogate key: parallel O(1), eliminates single-partition shuffle bottleneck
    surrogate_key = F.abs(F.hash(F.col("date_key"), F.col("location_key"))).cast("int")

    result = (
        enriched
        .withColumn("weather_daily_key", surrogate_key)
        .withColumn("temperature_2m_mean", F.col("temperature_2m_mean").cast("float"))
        .withColumn("temperature_2m_max", F.col("temperature_2m_max").cast("float"))
        .withColumn("temperature_2m_min", F.col("temperature_2m_min").cast("float"))
        .withColumn("precipitation_sum", F.col("precipitation_sum").cast("float"))
        .withColumn("precipitation_hours", F.col("precipitation_hours").cast("float"))
        .withColumn("sunshine_duration", F.col("sunshine_duration").cast("float"))
        .withColumn("cloud_cover_mean", F.col("cloud_cover_mean").cast("float"))
        .withColumn("shortwave_radiation_sum", F.col("shortwave_radiation_sum").cast("float"))
        .withColumn("wind_speed_10m_max", F.col("wind_speed_10m_max").cast("float"))
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
