import argparse
from datetime import datetime
from zoneinfo import ZoneInfo
import pyspark.sql.functions as F

from utils.spark import create_spark_session
from utils.logging import create_logger
from silver import silver_pipeline
from silver.quality_control import (
    enforce_quality_circuit_breaker,
    detect_volume_skew,
    QualityCircuitBreakerError,
)

logger = create_logger(__name__)

DATASETS = {
    "power": ("nessie.bronze.evn", silver_pipeline.power),
    "generation": ("nessie.bronze.evn", silver_pipeline.generation),
    "hydro": ("nessie.bronze.evn_hydro", silver_pipeline.hydro),
    "load": ("nessie.bronze.nsmo", silver_pipeline.load),
    "weather_daily": ("nessie.bronze.open_meteo", silver_pipeline.weather_daily),
    "weather_hourly": ("nessie.bronze.open_meteo", silver_pipeline.weather_hourly),
}


def _write_iceberg_partitioned(df, table_name, spark, partition_col="data_date"):
    """Write DataFrame to Iceberg table partitioned by partition_col with dynamic partition overwrite."""
    df_sorted = df.repartition(partition_col).sortWithinPartitions(partition_col)
    if spark.catalog.tableExists(table_name):
        df_sorted.writeTo(table_name) \
            .option("fanout-enabled", "false") \
            .overwritePartitions()
    else:
        df_sorted.writeTo(table_name) \
            .tableProperty("write.format.default", "parquet") \
            .tableProperty("format-version", "2") \
            .tableProperty("write.distribution-mode", "hash") \
            .tableProperty("write.spark.fanout.enabled", "false") \
            .partitionedBy(partition_col) \
            .using("iceberg") \
            .create()


def write_silver_tables(spark, dataset_name, bronze_table, pipeline_func, run_date=None):
    """Execute write silver tables logic with Quality Circuit Breaker and Volume Skew Detection."""
    logger.info(f"Processing {dataset_name}...")
    
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver")
    
    bronze_df = spark.table(bronze_table)
    if run_date:
        logger.info(f"Filtering bronze_df for ingest_date = {run_date}")
        bronze_df = bronze_df.filter(F.col("ingest_date") == run_date)
        
    valid_df, reject_df = pipeline_func(bronze_df)
    
    valid_table = f"nessie.silver.{dataset_name}"
    reject_table = f"nessie.silver.{dataset_name}_reject"
    
    # Persist DataFrames to avoid multiple Spark DAG re-evaluations
    valid_df = valid_df.persist()
    reject_df = reject_df.persist()

    try:
        valid_count = valid_df.count()
        reject_count = reject_df.count()
        total_count = valid_count + reject_count

        logger.info(f"  -> Pipeline produced {valid_count} valid, {reject_count} rejected records.")

        # 1. Volume Skew / Drop Detection (Alert if volume drops > 30% vs 7-day average)
        detect_volume_skew(spark, dataset_name, valid_table, total_count, run_date=run_date, max_drop_ratio=0.30)

        # 2. Quality Circuit Breaker: Halt pipeline if reject rate > 10%
        try:
            enforce_quality_circuit_breaker(dataset_name, valid_count, reject_count, max_reject_ratio=0.10)
        except QualityCircuitBreakerError:
            # If rejected rows exist, preserve them in reject table for data engineering diagnostics
            if reject_count > 0:
                logger.info(f"Writing {reject_count} rejected records to {reject_table} for debugging...")
                _write_iceberg_partitioned(reject_df, reject_table, spark)
            raise

        # 3. Write Valid Records
        if valid_count > 0:
            _write_iceberg_partitioned(valid_df, valid_table, spark)
            logger.info(f"  -> Wrote {valid_count} records to {valid_table}")
        else:
            logger.warning(f"  -> Valid record count is 0 for {dataset_name}. Skipped write.")

        # 4. Write Reject Records (if any)
        if reject_count > 0:
            _write_iceberg_partitioned(reject_df, reject_table, spark)
            logger.info(f"  -> Wrote {reject_count} records to {reject_table}")

    finally:
        valid_df.unpersist()
        reject_df.unpersist()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", choices=["all", *DATASETS], default="all")
    parser.add_argument("--run-date", type=str, help="The ingest_date partition to read from bronze layer. Defaults to current date.")
    args = parser.parse_args()
    
    if not args.run_date:
        args.run_date = datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).date().isoformat()
        
    spark = create_spark_session("write_silver")
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        selected = DATASETS.items() if args.dataset == "all" else [(args.dataset, DATASETS[args.dataset])]
        for name, (bronze_table, pipeline) in selected:
            write_silver_tables(spark, name, bronze_table, pipeline, run_date=args.run_date)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
