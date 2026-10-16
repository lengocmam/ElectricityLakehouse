from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import col, dense_rank, lit, when
from pyspark.sql.window import Window


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
        .select(F.trim(col("plant_name")).alias("plant_name"))
        .filter(col("plant_name").isNotNull() & (col("plant_name") != ""))
        .withColumn("norm_name", F.lower(col("plant_name")))
        .dropDuplicates(["norm_name"])
        .drop("norm_name")
    )

    # Build basin column using regex / matching rules
    basin_col = lit("Lưu vực khác")
    lower_plant = col("plant_name").cast("string")

    for keywords, basin_name in RIVER_BASIN_RULES:
        cond = lit(False)
        for kw in keywords:
            cond = cond | lower_plant.rlike(f"(?i){kw}")
        basin_col = when(cond, lit(basin_name)).otherwise(basin_col)

    window = Window.orderBy("plant_name")
    return (
        distinct_plants
        .withColumn("plant_key", dense_rank().over(window).cast("int"))
        .withColumn("river_basin", basin_col)
        .select("plant_key", "plant_name", "river_basin")
    )
