from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_date

from silver.silver_utils import add_dq, normalize_text


def clean_weather_hourly(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Execute clean weather hourly logic."""
    prepared = (
        df.withColumn("timezone", normalize_text("timezone"))
        .withColumn("data_date", to_date("observation_timestamp"))
    )
    ranges = [
        ((col("latitude") < -90) | (col("latitude") > 90), "LATITUDE_INVALID"),
        ((col("longitude") < -180) | (col("longitude") > 180), "LONGITUDE_INVALID"),
        (col("relative_humidity_2m").isNotNull() & ((col("relative_humidity_2m") < 0) | (col("relative_humidity_2m") > 100)), "HOURLY_HUMIDITY_INVALID"),
        (col("cloud_cover").isNotNull() & ((col("cloud_cover") < 0) | (col("cloud_cover") > 100)), "HOURLY_CLOUD_COVER_INVALID"),
        (col("precipitation").isNotNull() & (col("precipitation") < 0), "HOURLY_PRECIPITATION_NEGATIVE"),
        (col("shortwave_radiation").isNotNull() & (col("shortwave_radiation") < 0), "HOURLY_RADIATION_NEGATIVE"),
        (col("wind_speed_10m").isNotNull() & (col("wind_speed_10m") < 0), "HOURLY_WIND_SPEED_NEGATIVE"),
    ]
    return add_dq(
        prepared,
        key_name="hourly_weather_key",
        key_columns=["latitude", "longitude", "observation_timestamp"],
        hash_columns=[
            "latitude", "longitude", "timezone", "utc_offset_seconds", "observation_timestamp",
            "temperature_2m", "apparent_temperature", "relative_humidity_2m",
            "precipitation", "cloud_cover", "shortwave_radiation", "wind_speed_10m", "weather_code",
        ],
        required_columns={
            "bronze_key": "BRONZE_KEY_NULL",
            "latitude": "LATITUDE_NULL",
            "longitude": "LONGITUDE_NULL",
            "timezone": "TIMEZONE_NULL",
            "utc_offset_seconds": "UTC_OFFSET_NULL",
            "observation_timestamp": "OBSERVATION_TIME_NULL",
            "temperature_2m": "HOURLY_TEMP_NULL",
            "apparent_temperature": "HOURLY_APPARENT_TEMP_NULL",
            "relative_humidity_2m": "HOURLY_HUMIDITY_NULL",
            "precipitation": "HOURLY_PRECIPITATION_NULL",
            "cloud_cover": "HOURLY_CLOUD_COVER_NULL",
            "shortwave_radiation": "HOURLY_RADIATION_NULL",
            "wind_speed_10m": "HOURLY_WIND_SPEED_NULL",
            "weather_code": "HOURLY_WEATHER_CODE_NULL",
        },
        range_conditions=ranges,
        dedup_order_by=[col("bronze_key").desc()],
    )
