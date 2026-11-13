from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import FloatType, IntegerType, StringType, StructField, StructType
from pyspark.errors import AnalysisException

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

    try:
        has_existing = spark.catalog.tableExists("nessie.gold.dim_location")
    except AnalysisException as exc:
        missing_catalog = ("CATALOG_NOT_FOUND", "SCHEMA_NOT_FOUND",
                           "REQUIRES_SINGLE_PART_NAMESPACE")
        if not any(code in str(exc) for code in missing_catalog):
            raise
        has_existing = False
    existing_keys = (
        {row.province.strip().casefold(): int(row.location_key)
         for row in spark.table("nessie.gold.dim_location").select("province", "location_key").collect()}
        if has_existing else {}
    )
    next_key = max(existing_keys.values(), default=0)

    data = []
    for loc in LOCATIONS:
        name = loc["location_name"]
        location_key = existing_keys.get(name.strip().casefold())
        if location_key is None:
            next_key += 1
            location_key = next_key
        if name in NORTH_PROVINCES:
            region_key = 1
        elif name in CENTRAL_PROVINCES:
            region_key = 2
        else:
            region_key = 3

        data.append((
            location_key,
            float(loc["latitude"]),
            float(loc["longitude"]),
            name,
            region_key,
        ))

    return spark.createDataFrame(data, schema)
