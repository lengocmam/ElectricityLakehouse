from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


def generate_dim_region(spark: SparkSession) -> DataFrame:
    """Generate region dimension table (dim_region)."""
    schema = StructType([
        StructField("region_key", IntegerType(), False),
        StructField("region_name", StringType(), False),
    ])

    data = [
        (1, "north"),
        (2, "central"),
        (3, "south"),
        (4, "national"),
    ]

    return spark.createDataFrame(data, schema)
