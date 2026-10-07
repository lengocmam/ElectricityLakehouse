from utils.logging import create_logger
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType
from bronze.control import update_watermark

logger = create_logger(__name__)

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
    """Write raw records to the Bronze Iceberg table and update watermark."""
    if not records:
        if fail_on_empty:
            logger.error("No records were successfully crawled.")
            raise RuntimeError("No records were successfully crawled.")
        logger.info("No records to write to Bronze.")
        return False

    df = spark.createDataFrame(records, schema=BRONZE_RAW_SCHEMA)

    if namespace:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")

    if spark.catalog.tableExists(table_name):
        logger.info(f"Appending {len(records)} records to {table_name}.")
        df.writeTo(table_name).append()
    else:
        logger.info(f"Creating {table_name} and writing {len(records)} records.")
        df.writeTo(table_name).using("iceberg").partitionedBy("ingest_date").create()
        
    if update_watermark_after_write:
        if last_successful_data_date is None:
            raise ValueError(
                "last_successful_data_date is required "
                "when update_watermark_after_write=True."
            )

        update_watermark(dataset_name=dataset_name, data_date=last_successful_data_date,)
        logger.info(f"Watermark updated to {last_successful_data_date} for {dataset_name}.")

    logger.info("Bronze ingestion completed successfully.")
    return True