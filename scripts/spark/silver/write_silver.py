import argparse
from datetime import datetime
from zoneinfo import ZoneInfo
from pyspark.sql.functions import col, to_timestamp, row_number
from pyspark.sql.window import Window

from utils.spark import create_spark_session
from utils.logging import create_logger
from silver import silver_pipeline
from silver.quality_control import (
    enforce_quality_circuit_breaker,
    detect_volume_skew,
    QualityCircuitBreakerError,
)
from utils.audit import record_task_metrics

logger = create_logger(__name__)

DATASETS = {
    "power_daily": ("nessie.bronze.evn", silver_pipeline.power_daily),
    "generation": ("nessie.bronze.evn", silver_pipeline.generation),
    "hydro": ("nessie.bronze.evn_hydro", silver_pipeline.hydro),
    "load": ("nessie.bronze.nsmo_load", silver_pipeline.load),
    "power_hourly": ("nessie.bronze.nsmo_power", silver_pipeline.power_hourly),
    "weather_daily": ("nessie.bronze.open_meteo", silver_pipeline.weather_daily),
    "weather_hourly": ("nessie.bronze.open_meteo", silver_pipeline.weather_hourly),
}


def _write_iceberg_partitioned(df, table_name, spark, partition_col="report_date"):
    """Write DataFrame to Iceberg table partitioned by partition_col with dynamic partition overwrite."""
    df_sorted = df.repartition(partition_col).sortWithinPartitions(partition_col)
    if spark.catalog.tableExists(table_name):
        target_columns = spark.table(table_name).columns
        if set(df_sorted.columns) != set(target_columns):
            raise ValueError(f"Silver schema mismatch for {table_name}")
        df_sorted.select(*target_columns).writeTo(table_name) \
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

    bronze_df = spark.table(bronze_table).where(col("ingest_date") == run_date)
    report_order = Window.partitionBy("source_url").orderBy(
        to_timestamp("ingestion_timestamp").desc(), col("bronze_key").desc()
    )
    bronze_df = (
        bronze_df.withColumn("_report_rank", row_number().over(report_order))
        .where(col("_report_rank") == 1).drop("_report_rank")
    )

    valid_df, reject_df = pipeline_func(bronze_df)
    valid_table = f"nessie.silver.{dataset_name}"
    reject_table = f"nessie.silver.{dataset_name}_reject"
    valid_df = valid_df.persist()
    reject_df = reject_df.persist()
    try:
        if "report_date" not in valid_df.columns or "report_date" not in reject_df.columns:
            raise ValueError(f"{dataset_name} pipeline must produce report_date")
        valid_count = valid_df.count()
        reject_count = reject_df.count()
        total_count = valid_count + reject_count
        if valid_df.where(col("report_date").isNull()).limit(1).count():
            raise ValueError(f"{dataset_name} has valid rows without report_date")
        logger.info("Pipeline produced %d valid, %d rejected records", valid_count, reject_count)

        detect_volume_skew(
            spark,
            dataset_name,
            valid_table,
            total_count,
            run_date=run_date,
            max_drop_ratio=0.30
        )
        try:
            enforce_quality_circuit_breaker(dataset_name, valid_count, reject_count, max_reject_ratio=0.10)
        except QualityCircuitBreakerError:
            if reject_count:
                try:
                    _write_iceberg_partitioned(reject_df, reject_table, spark, "report_date")
                except Exception:
                    logger.exception(
                        "Could not write %d rejected rows to %s; preserving the quality circuit breaker error",
                        reject_count, reject_table,
                    )
            record_task_metrics(
                dataset_name=dataset_name,
                records_read=total_count,
                records_written=0,
                records_rejected=reject_count,
                start_date=run_date,
                end_date=run_date,
                status="FAILED",
                error_message="Quality Circuit Breaker tripped"
            )
            raise

        if valid_count > 0:
            _write_iceberg_partitioned(valid_df, valid_table, spark, "report_date")
            logger.info("Wrote %d records to %s", valid_count, valid_table)
        else:
            logger.info("No valid rows for %s on ingest_date %s", dataset_name, run_date)
        if reject_count > 0:
            _write_iceberg_partitioned(reject_df, reject_table, spark, "report_date")
        record_task_metrics(
            dataset_name=dataset_name,
            records_read=total_count,
            records_written=valid_count,
            records_rejected=reject_count,
            start_date=run_date,
            end_date=run_date,
        )
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
