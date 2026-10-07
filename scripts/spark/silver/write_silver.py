import argparse

from utils.spark import create_spark_session
from utils.logging import create_logger
from silver import silver_pipeline

logger = create_logger(__name__)

DATASETS = {
    "power": ("nessie.bronze.evn", silver_pipeline.power),
    "generation": ("nessie.bronze.evn", silver_pipeline.generation),
    "hydro": ("nessie.bronze.evn_hydro", silver_pipeline.hydro),
    "load": ("nessie.bronze.nsmo", silver_pipeline.load),
    "weather_daily": ("nessie.bronze.open_meteo", silver_pipeline.weather_daily),
    "weather_hourly": ("nessie.bronze.open_meteo", silver_pipeline.weather_hourly),
}

def write_silver_tables(spark, dataset_name, bronze_table, pipeline_func):
    """Execute write silver tables logic."""
    logger.info(f"Processing {dataset_name}...")
    
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver")
    
    bronze_df = spark.table(bronze_table)
    valid_df, reject_df = pipeline_func(bronze_df)
    
    valid_table = f"nessie.silver.{dataset_name}"
    reject_table = f"nessie.silver.{dataset_name}_reject"
    
    valid_df = valid_df.repartition("data_date").sortWithinPartitions("data_date")
    reject_df = reject_df.repartition("data_date").sortWithinPartitions("data_date")
    
    if spark.catalog.tableExists(valid_table):
        valid_df.writeTo(valid_table) \
            .option("fanout-enabled", "false") \
            .overwritePartitions()
    else:
        valid_df.writeTo(valid_table) \
            .tableProperty("write.format.default", "parquet") \
            .tableProperty("format-version", "2") \
            .tableProperty("write.distribution-mode", "hash") \
            .tableProperty("write.spark.fanout.enabled", "false") \
            .partitionedBy("data_date") \
            .using("iceberg") \
            .create()
    logger.info(f"  -> Wrote {valid_df.count()} records to {valid_table}")
    
    if spark.catalog.tableExists(reject_table):
        reject_df.writeTo(reject_table) \
            .option("fanout-enabled", "false") \
            .overwritePartitions()
    else:
        reject_df.writeTo(reject_table) \
            .tableProperty("write.format.default", "parquet") \
            .tableProperty("format-version", "2") \
            .tableProperty("write.distribution-mode", "hash") \
            .tableProperty("write.spark.fanout.enabled", "false") \
            .partitionedBy("data_date") \
            .using("iceberg") \
            .create()
    logger.info(f"  -> Wrote {reject_df.count()} records to {reject_table}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", choices=["all", *DATASETS], default="all")
    args = parser.parse_args()
    
    spark = create_spark_session("write_silver")
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        selected = DATASETS.items() if args.dataset == "all" else [(args.dataset, DATASETS[args.dataset])]
        for name, (bronze_table, pipeline) in selected:
            write_silver_tables(spark, name, bronze_table, pipeline)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
