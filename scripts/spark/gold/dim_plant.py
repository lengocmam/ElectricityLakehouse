from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    broadcast, coalesce, col, dense_rank, lit,
    lower, max, regexp_replace, trim, when,
)
from pyspark.sql.window import Window
from pyspark.errors import AnalysisException


RIVER_BASIN_RULES = [
    (["hòa bình", "sơn la", "lai châu", "bản chát", "huội quảng", "nậm chiến"], "Sông Đà"),
    (["tuyên quang", "thác bà", "chiêm hóa"], "Sông Lô - Gâm"),
    (["trung sơn", "cửa đạt", "hủa na", "bản vẽ"], "Sông Mã - Cả"),
    (["a vương", "sông tranh", "đak mi", "sông bung"], "Sông Vu Gia - Thu Bồn"),
    (["ialy", "sê san", "pleikrông"], "Sông Sê San"),
    (["buôn kuốp", "buôn tua srah", "srêpôk"], "Sông Srêpôk"),
    (["sông ba hạ", "sông hinh", "an khê"], "Sông Ba"),
    (["trị an", "thác mơ", "cần đơn", "đa nhim", "đại ninh", "đồng nai", "hàm thuận"], "Sông Đồng Nai"),
]


def generate_dim_plant(spark: SparkSession, hydro_df: DataFrame | None = None) -> DataFrame:
    """Generate hydropower reservoir dimension table (dim_plant) directly from Silver."""
    if hydro_df is None:
        if spark.catalog.tableExists("nessie.silver.hydro"):
            hydro_df = spark.table("nessie.silver.hydro")
        else:
            from pyspark.sql.types import IntegerType, StringType, StructField, StructType
            schema = StructType([
                StructField("plant_key", IntegerType(), False),
                StructField("plant_name", StringType(), False),
                StructField("river_basin", StringType(), False),
            ])
            return spark.createDataFrame([], schema)

    distinct_plants = (
        hydro_df
        .select(
            trim(
                regexp_replace(col("plant_name"), r"(?iu)\s*[đĐ]\u1ed3ng\s*b\u1ed9\s*l\u00fac.*$", "")
            ).alias("plant_name")
        )
        .filter(col("plant_name").isNotNull() & (col("plant_name") != ""))
        .withColumn("norm_name", lower(col("plant_name")))
        .dropDuplicates(["norm_name"])
    )

    try:
        has_existing = spark.catalog.tableExists("nessie.gold.dim_plant")
    except AnalysisException as exc:
        missing_catalog = ("CATALOG_NOT_FOUND", "SCHEMA_NOT_FOUND",
                           "REQUIRES_SINGLE_PART_NAMESPACE")
        if not any(code in str(exc) for code in missing_catalog):
            raise
        has_existing = False
    if has_existing:
        existing = spark.table("nessie.gold.dim_plant")
        max_key = existing.agg(max("plant_key")).first()[0] or 0
        existing_keys = existing.select(
            lower(trim("plant_name")).alias("norm_name"), "plant_key"
        )
    else:
        max_key = 0
        existing_keys = (distinct_plants.select("norm_name").limit(0)
                         .withColumn("plant_key", lit(None).cast("int")))

    # Build basin column using regex / matching rules
    basin_col = lit("Lưu vực khác")
    lower_plant = col("plant_name").cast("string")

    for keywords, basin_name in RIVER_BASIN_RULES:
        cond = lit(False)
        for kw in keywords:
            cond = cond | lower_plant.rlike(f"(?i){kw}")
        basin_col = when(cond, lit(basin_name)).otherwise(basin_col)

    window = Window.orderBy("norm_name")
    return (
        distinct_plants
        .join(broadcast(existing_keys), "norm_name", "left")
        .withColumn("plant_key", coalesce(
            col("plant_key"),
            (lit(max_key) + dense_rank().over(window)).cast("int"),
        ))
        .withColumn("river_basin", basin_col)
        .select("plant_key", "plant_name", "river_basin")
    )
