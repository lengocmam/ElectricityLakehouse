from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from silver.silver_utils import add_dq, normalize_text


def clean_weather_daily(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Execute clean weather daily logic."""
    prepared = df.withColumn("timezone", normalize_text("timezone"))
    ranges = [
        ((col("latitude") < -90) | (col("latitude") > 90), "LATITUDE_INVALID"),
        ((col("longitude") < -180) | (col("longitude") > 180), "LONGITUDE_INVALID"),
        (col("cloud_cover_mean").isNotNull() & ((col("cloud_cover_mean") < 0) | (col("cloud_cover_mean") > 100)), "DAILY_CLOUD_COVER_INVALID"),
        (col("precipitation_hours").isNotNull() & ((col("precipitation_hours") < 0) | (col("precipitation_hours") > 24)), "PRECIPITATION_HOURS_INVALID"),
        (col("precipitation_sum").isNotNull() & (col("precipitation_sum") < 0), "DAILY_PRECIPITATION_NEGATIVE"),
        (col("sunshine_duration").isNotNull() & (col("sunshine_duration") < 0), "SUNSHINE_DURATION_NEGATIVE"),
        (col("shortwave_radiation_sum").isNotNull() & (col("shortwave_radiation_sum") < 0), "DAILY_RADIATION_NEGATIVE"),
        (col("wind_speed_10m_max").isNotNull() & (col("wind_speed_10m_max") < 0), "DAILY_WIND_MAX_NEGATIVE"),
    ]
    return add_dq(
        prepared, key_name="daily_weather_key", key_columns=["latitude", "longitude", "data_date"],
        hash_columns=[
            "latitude", "longitude", "timezone", "utc_offset_seconds", "data_date",
            "weather_code", "temperature_2m_mean", "temperature_2m_max", "temperature_2m_min",
            "precipitation_sum", "precipitation_hours", "sunshine_duration",
            "cloud_cover_mean", "shortwave_radiation_sum", "wind_speed_10m_max",
        ],
        required_columns={
            "bronze_key": "BRONZE_KEY_NULL",
            "latitude": "LATITUDE_NULL",
            "longitude": "LONGITUDE_NULL",
            "data_date": "DATE_NULL",
            "weather_code": "DAILY_WEATHER_CODE_NULL",
            "temperature_2m_mean": "DAILY_TEMP_MEAN_NULL",
            "temperature_2m_max": "DAILY_TEMP_MAX_NULL",
            "temperature_2m_min": "DAILY_TEMP_MIN_NULL",
            "precipitation_sum": "DAILY_PRECIPITATION_NULL",
            "precipitation_hours": "PRECIPITATION_HOURS_NULL",
            "sunshine_duration": "SUNSHINE_DURATION_NULL",
            "cloud_cover_mean": "DAILY_CLOUD_COVER_NULL",
            "shortwave_radiation_sum": "DAILY_RADIATION_NULL",
            "wind_speed_10m_max": "DAILY_WIND_MAX_NULL",
        },
        range_conditions=ranges,
        dedup_order_by=[col("bronze_key").desc()],
    )
