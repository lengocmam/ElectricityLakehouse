from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import FloatType, IntegerType, StringType, StructField, StructType

from utils.locations import LOCATIONS


NORTH_PROVINCES = {
    "Ha Noi", "Cao Bang", "Tuyen Quang", "Dien Bien", "Lai Chau", "Son La",
    "Lao Cai", "Thai Nguyen", "Lang Son", "Quang Ninh", "Bac Ninh", "Phu Tho",
    "Hung Yen", "Hai Phong", "Ninh Binh", "Thanh Hoa", "Nghe An", "Ha Tinh",
}

CENTRAL_PROVINCES = {
    "Quang Tri", "Hue", "Da Nang", "Quang Ngai", "Gia Lai", "Khanh Hoa",
    "Lam Dong", "Dak Lak",
}


def generate_dim_location(spark: SparkSession) -> DataFrame:
    """Generate location dimension table for 34 provinces/cities (dim_location)."""
    schema = StructType([
        StructField("location_key", IntegerType(), False),
        StructField("latitude", FloatType(), False),
        StructField("longitude", FloatType(), False),
        StructField("province", StringType(), False),
        StructField("region_key", IntegerType(), False),
    ])

    data = []
    for idx, loc in enumerate(LOCATIONS, start=1):
        name = loc["location_name"]
        if name in NORTH_PROVINCES:
            region_key = 1
        elif name in CENTRAL_PROVINCES:
            region_key = 2
        else:
            region_key = 3

        data.append((
            idx,
            float(loc["latitude"]),
            float(loc["longitude"]),
            name,
            region_key,
        ))

    return spark.createDataFrame(data, schema)
