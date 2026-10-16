from pyspark.sql import Column
import pyspark.sql.functions as F


def build_date_key(col: Column) -> Column:
    """Convert date or timestamp column to integer date_key (YYYYMMDD)."""
    return F.date_format(col, "yyyyMMdd").cast("int")


def build_time_key(col: Column) -> Column:
    """Extract hour from timestamp as integer time_key (0-23)."""
    return F.hour(col).cast("int")
