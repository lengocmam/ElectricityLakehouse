from pyspark.sql import Column
from pyspark.sql.functions import date_format, hour


def build_date_key(col: Column) -> Column:
    """Convert date or timestamp column to integer date_key (YYYYMMDD)."""
    return date_format(col, "yyyyMMdd").cast("int")


def build_time_key(col: Column) -> Column:
    """Extract hour from timestamp as integer time_key (0-23)."""
    return hour(col).cast("int")
