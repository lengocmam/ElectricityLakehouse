from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from datetime import date

from bronze.watermark import update_watermark


BRONZE_RAW_SCHEMA = StructType([
    StructField("bronze_key", StringType(), False),
    StructField("source_name", StringType(), False),
    StructField("source_url", StringType(), False),
    StructField("source_data_date", StringType(), True),
    StructField("source_data_start_date", StringType(), True),
    StructField("source_data_end_date", StringType(), True),
    StructField("batch_id", StringType(), False),
    StructField("ingestion_timestamp", StringType(), False),
    StructField("ingest_date", StringType(), False),
    StructField("raw", StringType(), False),
])


def write_raw_bronze(
    spark: SparkSession,
    records: list[dict],
    table_name: str,
    ingest_date: str,
    last_successful_data_date: date | None,
    *,
    namespace: str | None = None,
    fail_on_empty: bool = False,
    dataset_name: str,
    update_watermark_after_write: bool = True,
) -> bool:
    if not records:
        if fail_on_empty:
            raise RuntimeError("No records were successfully crawled.")
        print("No records to write.")
        return False

    df = spark.createDataFrame(records, schema=BRONZE_RAW_SCHEMA)
    print(f"Records to write: {len(records)}")
    print(f"Ingest date: {ingest_date}")

    if namespace:
        print(f"Creating namespace if needed: {namespace}")
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")

    if spark.catalog.tableExists(table_name):
        print("Table exists, overwriting partitions...")
        df.writeTo(table_name).overwritePartitions()
    else:
        print("Table does not exist, creating and writing...")
        df.writeTo(table_name).using("iceberg").partitionedBy("ingest_date").create()
        
    if update_watermark_after_write:
        if last_successful_data_date is None:
            raise ValueError(
                "last_successful_data_date is required "
                "when update_watermark_after_write=True."
            )

        update_watermark(
            dataset_name=dataset_name,
            data_date=last_successful_data_date,
        )

    print("Bronze ingestion completed successfully.")
    return True