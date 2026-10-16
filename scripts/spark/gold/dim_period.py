from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


def generate_dim_period(spark: SparkSession) -> DataFrame:
    """Generate load period dimension table (dim_period)."""
    schema = StructType([
        StructField("period_key", IntegerType(), False),
        StructField("period_name", StringType(), False),
    ])

    data = [
        (1, "midday_off_peak"),
        (2, "evening_peak"),
    ]

    return spark.createDataFrame(data, schema)
