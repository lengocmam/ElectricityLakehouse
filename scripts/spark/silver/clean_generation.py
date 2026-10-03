from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from silver.silver_utils import add_dq, normalize_text


def clean_generation(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    prepared = df.withColumn("energy_source", normalize_text("energy_source"))
    return add_dq(
        prepared, key_name="evn_generation_key", key_columns=["data_date", "energy_source"],
        hash_columns=["data_date", "energy_source", "generation_million_kwh"],
        required_columns={
            "bronze_key": "BRONZE_KEY_NULL",
            "data_date": "DATA_DATE_NULL",
            "energy_source": "SOURCE_TYPE_NULL",
            "generation_million_kwh": "GENERATION_NULL",
        },
        range_conditions=[
            (col("generation_million_kwh") < 0, "GENERATION_NEGATIVE"),
        ],
        dedup_order_by=[col("bronze_key").desc()],
    )
