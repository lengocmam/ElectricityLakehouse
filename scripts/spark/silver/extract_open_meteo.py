from pyspark.sql import DataFrame
from pyspark.sql.functions import arrays_zip, col, explode, from_json, inline, to_date, to_timestamp
from pyspark.sql.types import ArrayType, DoubleType, IntegerType, StringType, StructField, StructType


def _arrays(names, type_):
    """Execute arrays logic."""
    return [StructField(name, ArrayType(type_)) for name in names]


HOURLY_FIELDS = ["time", "temperature_2m", "apparent_temperature", "relative_humidity_2m", "precipitation", "cloud_cover", "shortwave_radiation", "wind_speed_10m", "weather_code"]
DAILY_FIELDS = ["time", "weather_code", "temperature_2m_mean", "temperature_2m_max", "temperature_2m_min", "precipitation_sum", "precipitation_hours", "sunshine_duration", "cloud_cover_mean", "shortwave_radiation_sum", "wind_speed_10m_max"]
LOCATION_SCHEMA = ArrayType(StructType([
    StructField("latitude", DoubleType()), StructField("longitude", DoubleType()), StructField("timezone", StringType()), StructField("utc_offset_seconds", IntegerType()),
    StructField("hourly", StructType(_arrays(HOURLY_FIELDS, StringType())[:1] + _arrays(HOURLY_FIELDS[1:], DoubleType()))),
    StructField("daily", StructType(_arrays(DAILY_FIELDS, StringType())[:1] + _arrays(DAILY_FIELDS[1:], DoubleType()))),
]))


def _locations(df: DataFrame) -> DataFrame:
    """Execute locations logic."""
    return df.select("bronze_key", explode(from_json("raw", LOCATION_SCHEMA)).alias("location"))


def _inline(prefix, fields):
    """Execute inline logic."""
    return inline(arrays_zip(*[col(f"location.{prefix}.{field}").alias(field) for field in fields]))


def extract_open_meteo_daily(df: DataFrame) -> DataFrame:
    """Return one record per raw coordinate and day."""
    return (_locations(df).select("bronze_key", "location.*", _inline("daily", DAILY_FIELDS))
        .withColumn("data_date", to_date("time"))
        .select("bronze_key", "latitude", "longitude", "timezone", "utc_offset_seconds", "data_date", *DAILY_FIELDS[1:]))


def extract_open_meteo_hourly(df: DataFrame) -> DataFrame:
    """Return one record per raw coordinate and hour."""
    return (_locations(df).select("bronze_key", "location.*", _inline("hourly", HOURLY_FIELDS))
        .withColumn("observation_timestamp", to_timestamp("time"))
        .select("bronze_key", "latitude", "longitude", "timezone", "utc_offset_seconds", "observation_timestamp", *HOURLY_FIELDS[1:]))