from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

from gold.gold_utils import build_date_key, build_time_key


def transform_fact_weather_hourly(
    weather_hourly_df: DataFrame,
    dim_location_df: DataFrame,
) -> DataFrame:
    """Transform silver hourly weather data into fact_weather_hourly."""
    enriched = (
        weather_hourly_df
        .join(
            F.broadcast(dim_location_df),
            F.trim(F.lower(weather_hourly_df.location_name)) == F.trim(F.lower(dim_location_df.province)),
            "inner",
        )
        .withColumn("date_key", build_date_key(F.col("observation_timestamp")))
        .withColumn("time_key", build_time_key(F.col("observation_timestamp")))
    )

    # Deterministic surrogate key: parallel O(1), eliminates single-partition shuffle bottleneck
    surrogate_key = F.abs(F.hash(F.col("date_key"), F.col("time_key"), F.col("location_key"))).cast("int")

    result = (
        enriched
        .withColumn("weather_hourly_key", surrogate_key)
        .withColumn("temperature_2m", F.col("temperature_2m").cast("float"))
        .withColumn("apparent_temperature", F.col("apparent_temperature").cast("float"))
        .withColumn("relative_humidity_2m", F.col("relative_humidity_2m").cast("float"))
        .withColumn("precipitation", F.col("precipitation").cast("float"))
        .withColumn("cloud_cover", F.col("cloud_cover").cast("float"))
        .withColumn("shortwave_radiation", F.col("shortwave_radiation").cast("float"))
        .withColumn("wind_speed_10m", F.col("wind_speed_10m").cast("float"))
        .withColumn("weather_code", F.col("weather_code").cast("int"))
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
