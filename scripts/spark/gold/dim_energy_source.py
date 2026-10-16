from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import BooleanType, IntegerType, StringType, StructField, StructType


def generate_dim_energy_source(spark: SparkSession) -> DataFrame:
    """Generate energy source dimension table (dim_energy_source)."""
    schema = StructType([
        StructField("energy_source_key", IntegerType(), False),
        StructField("source_code", StringType(), False),
        StructField("source_name", StringType(), False),
        StructField("renewable_flag", BooleanType(), False),
    ])

    data = [
        (1, "hydro", "Thủy điện", True),
        (2, "coal", "Nhiệt điện than", False),
        (3, "gas", "Tua bin khí", False),
        (4, "solar", "Điện mặt trời", True),
        (5, "wind", "Điện gió", True),
        (6, "biomass", "Điện sinh khối", True),
        (7, "import", "Nhập khẩu", False),
        (8, "oil", "Nhiệt điện dầu", False),
        (9, "other_renewable", "Năng lượng tái tạo khác", True),
        (10, "other", "Khác", False),
    ]

    return spark.createDataFrame(data, schema)
