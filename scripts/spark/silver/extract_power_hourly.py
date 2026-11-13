"""Extract all 30-minute NSMO generation readings from Bronze reports."""

import hashlib
from datetime import date, datetime

from pyspark.sql import DataFrame
from pyspark.sql.types import (DateType, DoubleType, StringType, StructField,
                               StructType, TimestampNTZType)

from bronze.nsmo_power import parse_report_payload


EXTRACT_SCHEMA = StructType([
    StructField("bronze_key", StringType(), False),
    StructField("report_date", DateType(), False),
    StructField("generation_source_name", StringType(), True),
    StructField("source_timestamp_raw", StringType(), True),
    StructField("observation_timestamp_local", TimestampNTZType(), True),
    StructField("generation_power_mw", DoubleType(), True),
    StructField("source_payload_hash", StringType(), False),
    StructField("validation_error", StringType(), True),
])


def _extract_record(record):
    report_date = date.fromisoformat(record.source_data_date)
    digest = hashlib.sha256(record.raw.encode("utf-8")).hexdigest()
    rows, issue = parse_report_payload(record.raw)
    if issue:
        return [(record.bronze_key, report_date, None, None, None, None, digest, issue)]
    return [
        (record.bronze_key, report_date, source, stamp,
         datetime.fromisoformat(stamp), power, digest, None)
        for source, stamp, power in rows
    ]


def extract_power_hourly(bronze_df: DataFrame) -> DataFrame:
    """Extract all 48 readings/source from NSMO Power reports."""
    return bronze_df.sparkSession.createDataFrame(
        bronze_df.rdd.flatMap(_extract_record), EXTRACT_SCHEMA
    )
