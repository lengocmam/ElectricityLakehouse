from pyspark.sql import DataFrame
from pyspark.sql.functions import array, col, explode, from_json, lit, minute, struct, to_timestamp
from pyspark.sql.types import ArrayType, DoubleType, StringType, StructField, StructType

LOAD_SCHEMA = StructType([StructField("result", StructType([StructField("data", StructType([StructField("phuTais", ArrayType(StructType([
    StructField("thoiGian", StringType()), StructField("congSuatMB", DoubleType()), StructField("congSuatMT", DoubleType()), StructField("congSuatMN", DoubleType()), StructField("congSuatHT", DoubleType()),
])))]))]))])


def extract_nsmo_load(df: DataFrame) -> DataFrame:
    """Select minute-zero source readings to meet the required region x hour grain."""
    observation = explode(from_json("raw", LOAD_SCHEMA)["result"]["data"]["phuTais"])

    return (df.select("bronze_key", "source_data_date", observation.alias("observation"))
        .withColumn("observation_timestamp", to_timestamp("observation.thoiGian"))
        .where(minute("observation_timestamp") == lit(0))
        .select("bronze_key", "source_data_date", "observation_timestamp", explode(array(
            struct(lit("north").alias("region"), col("observation.congSuatMB").alias("load_mw")), struct(lit("central").alias("region"), col("observation.congSuatMT").alias("load_mw")),
            struct(lit("south").alias("region"), col("observation.congSuatMN").alias("load_mw")), struct(lit("national").alias("region"), col("observation.congSuatHT").alias("load_mw")),
        )).alias("regional_load"))
        .select("bronze_key", "source_data_date", "observation_timestamp", "regional_load.*"))