import argparse
from datetime import datetime
from zoneinfo import ZoneInfo
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from utils.spark import create_spark_session
from utils.logging import create_logger

from gold.dim_date import generate_dim_date
from gold.dim_time import generate_dim_time
from gold.dim_region import generate_dim_region
from gold.dim_location import generate_dim_location
from gold.dim_plant import generate_dim_plant
from gold.dim_energy_source import generate_dim_energy_source
from gold.dim_period import generate_dim_period
from gold import gold_pipeline

logger = create_logger(__name__)

DIMENSIONS = {
    "dim_date": generate_dim_date,
    "dim_time": generate_dim_time,
    "dim_region": generate_dim_region,
    "dim_location": generate_dim_location,
    "dim_plant": generate_dim_plant,
    "dim_energy_source": generate_dim_energy_source,
    "dim_period": generate_dim_period,
}

FACTS = {
    "fact_load_hourly": ("nessie.silver.load", gold_pipeline.load_hourly),
    "fact_weather_hourly": ("nessie.silver.weather_hourly", gold_pipeline.weather_hourly),
    "fact_weather_daily": ("nessie.silver.weather_daily", gold_pipeline.weather_daily),
    "fact_hydro_status": ("nessie.silver.hydro", gold_pipeline.hydro_status),
    "fact_generation_daily": ("nessie.silver.generation", gold_pipeline.generation_daily),
    "fact_generation_capacity_daily": ("nessie.silver.power", gold_pipeline.generation_capacity_daily),
}


def write_dimension_table(spark, dim_name: str, generator_func) -> None:
    """Generate and write a dimension table to nessie.gold."""
    table_name = f"nessie.gold.{dim_name}"
    logger.info(f"Generating dimension {dim_name} -> {table_name}...")

    dim_df = generator_func(spark)
    dim_count = dim_df.count()
    if dim_count == 0:
        logger.warning(f"Dimension {dim_name} has 0 records. Skipping write to avoid blank tables.")
        return

    if spark.catalog.tableExists(table_name):
        dim_df.writeTo(table_name).overwrite(F.lit(True))
    else:
        dim_df.writeTo(table_name) \
            .tableProperty("write.format.default", "parquet") \
            .tableProperty("format-version", "2") \
            .using("iceberg") \
            .create()

    logger.info(f"  -> Wrote {dim_count} records to {table_name}")


from gold.gold_control import (
    get_changed_partitions,
    init_gold_control_table,
    update_gold_watermarks,
)


def validate_fact_invariants(fact_name: str, fact_df: DataFrame) -> None:
    """Pre-write Business & Mathematical Invariant Assertions."""
    if fact_name == "fact_load_hourly":
        neg_load = fact_df.filter(F.col("load_mw") < 0).count()
        if neg_load > 0:
            raise ValueError(f"Integrity violation in fact_load_hourly: {neg_load} rows with negative load_mw!")
        inv_load = fact_df.filter(
            (F.col("daily_min_load_mw") > F.col("daily_avg_load_mw")) |
            (F.col("daily_avg_load_mw") > F.col("daily_peak_load_mw"))
        ).count()
        if inv_load > 0:
            raise ValueError(f"Integrity violation in fact_load_hourly: {inv_load} rows violate min <= avg <= peak!")

    elif fact_name in ("fact_weather_hourly", "fact_weather_daily"):
        precip_col = "precipitation" if fact_name == "fact_weather_hourly" else "precipitation_sum"
        neg_precip = fact_df.filter(F.col(precip_col) < 0).count()
        if neg_precip > 0:
            raise ValueError(f"Integrity violation in {fact_name}: {neg_precip} rows with negative precipitation!")

    elif fact_name == "fact_hydro_status":
        neg_inflow = fact_df.filter(F.col("reservoir_inflow_m3_per_s") < 0).count()
        if neg_inflow > 0:
            raise ValueError(f"Integrity violation in fact_hydro_status: {neg_inflow} rows with negative inflow!")

    elif fact_name in ("fact_generation_daily", "fact_generation_capacity_daily"):
        share_col = "generation_share_pct" if fact_name == "fact_generation_daily" else "capacity_share_pct"
        invalid_share = fact_df.filter((F.col(share_col) < 0) | (F.col(share_col) > 100)).count()
        if invalid_share > 0:
            raise ValueError(f"Integrity violation in {fact_name}: {invalid_share} rows with invalid share percentage!")


def audit_row_conservation(
    fact_name: str,
    source_df: DataFrame,
    fact_df: DataFrame,
    changed_partitions: list[str],
) -> None:
    """Audit row conservation between Silver source and Gold output per partition."""
    silver_counts = {
        str(r["data_date"]): r["count"]
        for r in source_df.filter(F.col("data_date").cast("string").isin(changed_partitions))
        .groupBy("data_date").agg(F.count(F.lit(1)).alias("count")).collect()
    }
    gold_counts = {
        f"{str(r['date_key'])[:4]}-{str(r['date_key'])[4:6]}-{str(r['date_key'])[6:8]}": r["count"]
        for r in fact_df.groupBy("date_key").agg(F.count(F.lit(1)).alias("count")).collect()
    }

    for p_date in changed_partitions:
        s_cnt = silver_counts.get(p_date, 0)
        g_cnt = gold_counts.get(p_date, 0)
        if s_cnt != g_cnt:
            logger.warning(
                f"  [!] Row conservation warning for {fact_name} partition {p_date}: "
                f"Silver has {s_cnt} rows, Gold has {g_cnt} rows (Diff: {g_cnt - s_cnt})."
            )
        else:
            logger.info(f"  [✓] 100% Row conservation on {fact_name} [{p_date}]: {g_cnt}/{s_cnt} rows.")


def write_fact_table(spark, fact_name: str, source_table: str, pipeline_func, run_date: str | None = None) -> None:
    """Transform and write a fact table to nessie.gold partitioned by date_key for changed partitions."""
    table_name = f"nessie.gold.{fact_name}"
    logger.info(f"Processing fact {fact_name} from {source_table} -> {table_name}...")

    # 1. Identify which partitions in Silver have changed or are new
    changed_partitions = get_changed_partitions(spark, fact_name, source_table, run_date=run_date)
    if not changed_partitions:
        logger.info(f"No changed or new partitions detected for {fact_name}. Table is up-to-date. Skipped.")
        return

    logger.info(f"Found {len(changed_partitions)} changed/new partition(s) for {fact_name}: {changed_partitions}")

    min_date = min(changed_partitions)
    max_date = max(changed_partitions)

    source_df = spark.table(source_table)
    # Filter source data with 2-day lookback from min_date to max_date to preserve window lag calculations
    source_df = source_df.filter(
        (F.col("data_date") <= F.to_date(F.lit(max_date))) &
        (F.col("data_date") >= F.date_sub(F.to_date(F.lit(min_date)), 2))
    )

    fact_df = pipeline_func(spark, source_df)

    # Filter strictly to the target changed partitions for writing
    target_date_keys = [int(d.replace("-", "")) for d in changed_partitions]
    fact_df = fact_df.filter(F.col("date_key").isin(target_date_keys))

    # Optimization: Cache/Persist fact_df to avoid multiple Spark DAG re-evaluations
    fact_df = fact_df.persist()

    try:
        # Pre-write Data Quality & Integrity Gate
        written_count = fact_df.count()
        if written_count == 0:
            logger.warning(f"Integrity check: {fact_name} generated 0 rows for partitions {changed_partitions}. Write skipped.")
            return

        null_keys = fact_df.filter(F.col("date_key").isNull()).count()
        if null_keys > 0:
            raise ValueError(f"Integrity check failed: {null_keys} rows with NULL date_key detected in {fact_name}!")

        # Semantic Invariants Validation
        validate_fact_invariants(fact_name, fact_df)

        # End-to-End Row Conservation Audit
        audit_row_conservation(fact_name, source_df, fact_df, changed_partitions)

        fact_df = fact_df.repartition("date_key").sortWithinPartitions("date_key")

        if spark.catalog.tableExists(table_name):
            fact_df.writeTo(table_name) \
                .option("fanout-enabled", "false") \
                .overwritePartitions()
        else:
            fact_df.writeTo(table_name) \
                .tableProperty("write.format.default", "parquet") \
                .tableProperty("format-version", "2") \
                .tableProperty("write.distribution-mode", "hash") \
                .tableProperty("write.spark.fanout.enabled", "false") \
                .partitionedBy("date_key") \
                .using("iceberg") \
                .create()

        logger.info(f"  -> Wrote {written_count} records for {len(changed_partitions)} partition(s) to {table_name}")

        # Update watermarks for the successfully loaded partitions
        update_gold_watermarks(fact_name, changed_partitions, spark.table(source_table), written_count)

    finally:
        fact_df.unpersist()


def optimize_gold_tables(spark, target_tables: list[str]) -> None:
    """Run Iceberg compaction (rewrite_data_files) and snapshot maintenance."""
    for tbl in target_tables:
        full_name = f"nessie.gold.{tbl}"
        if spark.catalog.tableExists(full_name):
            logger.info(f"Optimizing Iceberg data files for {full_name}...")
            try:
                spark.sql(f"CALL nessie.system.rewrite_data_files(table => '{full_name}')")
                logger.info(f"  [✓] Compaction completed for {full_name}")
            except Exception as exc:
                logger.warning(f"Could not rewrite data files for {full_name}: {exc}")


def main():
    """Main CLI entrypoint for building Gold layer tables."""
    all_targets = ["all", "dims", "facts", *DIMENSIONS.keys(), *FACTS.keys()]
    parser = argparse.ArgumentParser(description="Gold Layer Pipeline Dispatcher")
    parser.add_argument("--dataset", choices=all_targets, default="all")
    parser.add_argument("--run-date", type=str, help="Filter date (YYYY-MM-DD) for incremental fact updates.")
    parser.add_argument("--optimize", action="store_true", help="Run Iceberg compaction (rewrite_data_files) on gold tables.")
    args = parser.parse_args()

    spark = create_spark_session("write_gold")
    spark.sparkContext.setLogLevel("WARN")

    try:
        init_gold_control_table()
        spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold")

        if args.optimize:
            opt_targets = list(FACTS.keys()) if args.dataset in {"all", "facts"} else [args.dataset]
            optimize_gold_tables(spark, opt_targets)
            return

        # 1. Process Dimensions
        if args.dataset in {"all", "dims"}:
            for name, gen in DIMENSIONS.items():
                write_dimension_table(spark, name, gen)
        elif args.dataset in DIMENSIONS:
            write_dimension_table(spark, args.dataset, DIMENSIONS[args.dataset])

        # 2. Process Facts
        if args.dataset in {"all", "facts"}:
            for name, (src, pipe) in FACTS.items():
                write_fact_table(spark, name, src, pipe, run_date=args.run_date)
        elif args.dataset in FACTS:
            src, pipe = FACTS[args.dataset]
            write_fact_table(spark, args.dataset, src, pipe, run_date=args.run_date)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
