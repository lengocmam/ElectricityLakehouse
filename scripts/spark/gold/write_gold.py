import argparse
from datetime import date, datetime, timedelta
from functools import reduce
from zoneinfo import ZoneInfo
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from utils.spark import create_spark_session
from utils.logging import create_logger
from utils.audit import record_task_metrics

from gold.dim_date import generate_dim_date
from gold.dim_time import generate_dim_time
from gold.dim_region import generate_dim_region
from gold.dim_location import generate_dim_location
from gold.dim_plant import generate_dim_plant
from gold.dim_energy_source import generate_dim_energy_source, migrate_energy_source_aliases
from gold.dim_period import generate_dim_period
from gold import gold_pipeline
from gold.gold_control import advance_gold_snapshot_checkpoint, get_changed_partitions

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
    "fact_generation_power_daily": ("nessie.silver.power_daily", gold_pipeline.generation_power_daily),
    "fact_generation_power_hourly": ("nessie.silver.power_hourly", gold_pipeline.generation_power_hourly),
}

FACT_GRAINS = {
    "fact_load_hourly": ("date_key", "time_key", "region_key"),
    "fact_weather_hourly": ("date_key", "time_key", "location_key"),
    "fact_weather_daily": ("date_key", "location_key"),
    "fact_hydro_status": ("date_key", "observation_time_key", "plant_key"),
    "fact_generation_daily": ("date_key", "energy_source_key"),
    "fact_generation_power_daily": ("date_key", "period_key", "energy_source_key"),
    "fact_generation_power_hourly": ("date_key", "generation_mix_key"),
}

EXACT_HOUR_SOURCE_COLUMNS = {
    "fact_load_hourly": "observation_timestamp",
    "fact_generation_power_hourly": "observation_timestamp_local",
}


def write_dimension_table(spark, dim_name: str, generator_func) -> None:
    """Merge current dimension members without removing historical keys."""
    table_name = f"nessie.gold.{dim_name}"
    logger.info(f"Generating dimension {dim_name} -> {table_name}...")

    dim_df = generator_func(spark)
    dim_count = dim_df.count()
    if dim_count == 0:
        logger.warning(f"Dimension {dim_name} has 0 records. Skipping write to avoid blank tables.")
        return

    if dim_name == "dim_energy_source" and spark.catalog.tableExists(table_name):
        existing = set(spark.table(table_name).columns)
        for column, dtype in (
            ("source_category", "STRING"),
            ("canonical_energy_source_key", "BIGINT"),
            ("is_active", "BOOLEAN"),
        ):
            if column not in existing:
                spark.sql(f"ALTER TABLE {table_name} ADD COLUMNS ({column} {dtype})")
    _merge_gold_rows(spark, table_name, dim_df, (f"{dim_name[4:]}_key",))
    if dim_name == "dim_energy_source":
        migrate_energy_source_aliases(spark)

    logger.info(f"  -> Wrote {dim_count} records to {table_name}")
    try:
        today = datetime.now().date()
        record_task_metrics(
            dataset_name=dim_name,
            records_read=dim_count,
            records_written=dim_count,
            records_rejected=0,
            start_date=today,
            end_date=today,
        )
    except Exception as exc:
        logger.debug(f"Dim audit metric recording skipped: {exc}")


def validate_fact_invariants(fact_name: str, fact_df: DataFrame) -> None:
    """Check measures calculated in Gold; raw value rules belong to Silver."""
    if fact_name == "fact_load_hourly":
        tolerance_mw = F.lit(0.01)
        inv_load = fact_df.filter(
            (F.col("daily_min_load_mw") > F.col("daily_avg_load_mw") + tolerance_mw) |
            (F.col("daily_avg_load_mw") > F.col("daily_peak_load_mw") + tolerance_mw)
        ).count()
        if inv_load > 0:
            raise ValueError(f"Integrity violation in fact_load_hourly: {inv_load} rows violate min <= avg <= peak!")

    elif fact_name in ("fact_generation_daily", "fact_generation_power_daily"):
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
    sample_timestamp = EXACT_HOUR_SOURCE_COLUMNS.get(fact_name)
    if sample_timestamp:
        source_df = source_df.where(
            (F.minute(sample_timestamp) == 0) &
            (F.second(sample_timestamp) == 0)
        )
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


def _merge_gold_rows(spark, table_name: str, rows: DataFrame,
                     keys: tuple[str, ...], partition_col: str | None = None) -> None:
    """Create a Gold table on first publication; otherwise upsert by its grain."""
    if not spark.catalog.tableExists(table_name):
        writer = (rows.writeTo(table_name)
                  .tableProperty("write.format.default", "parquet")
                  .tableProperty("format-version", "2"))
        if partition_col:
            writer = writer.tableProperty("write.distribution-mode", "hash").partitionedBy(partition_col)
        writer.using("iceberg").create()
        return

    if spark.table(table_name).columns != rows.columns:
        raise ValueError(f"Gold schema mismatch for {table_name}; migrate the table before MERGE")

    merge_source = rows.localCheckpoint(eager=True)
    view_name = f"_gold_merge_source_{table_name.rsplit('.', 1)[-1]}"
    merge_source.createOrReplaceTempView(view_name)
    try:
        match = " AND ".join(f"target.`{key}` = source.`{key}`" for key in keys)
        same = " AND ".join(
            f"target.`{column}` <=> source.`{column}`"
            for column in merge_source.columns if column not in keys
        )
        spark.sql(
            f"MERGE INTO {table_name} AS target USING {view_name} AS source "
            f"ON {match} "
            f"WHEN MATCHED AND NOT ({same}) THEN UPDATE SET * "
            "WHEN NOT MATCHED THEN INSERT *"
        )
    finally:
        spark.catalog.dropTempView(view_name)
        merge_source.unpersist()


def write_fact_table(
    spark,
    fact_name: str,
    source_table: str,
    pipeline_func,
    run_date: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    run_mode: str = "incremental",
) -> None:
    """Transform changed Silver reports and merge fact rows by their grain."""
    if not spark.catalog.tableExists(source_table):
        raise RuntimeError(f"Silver source table missing: {source_table}")
    if fact_name in {"fact_generation_daily", "fact_generation_power_daily"}:
        spark.conf.set("spark.sql.iceberg.vectorization.enabled", "false")
    table_name = f"nessie.gold.{fact_name}"
    logger.info(f"Processing fact {fact_name} from {source_table} -> {table_name}...")

    changes = get_changed_partitions(
        spark, fact_name, source_table, run_date=run_date, start_date=start_date,
        end_date=end_date, run_mode=run_mode,
    )
    if not changes.report_dates:
        logger.info("No changed Silver report partitions for %s", fact_name)
        if changes.advance_checkpoint and changes.snapshot_id != changes.checkpoint_id:
            advance_gold_snapshot_checkpoint(
                fact_name, source_table, changes.checkpoint_id, changes.snapshot_id,
            )
        try:
            target_dt = run_date or datetime.now().date().isoformat()
            record_task_metrics(
                dataset_name=fact_name,
                records_read=0,
                records_written=0,
                records_rejected=0,
                start_date=target_dt,
                end_date=target_dt,
            )
        except Exception as exc:
            logger.debug(f"Fact audit metric recording skipped: {exc}")
        return

    changed_report_dates = {date.fromisoformat(d) for d in changes.report_dates}
    silver_snapshot = spark.read.option("versionAsOf", str(changes.snapshot_id)).table(source_table)
    changed_source = silver_snapshot.where(F.col("report_date").isin(sorted(changed_report_dates)))
    target_dates = {row.data_date for row in changed_source.select("data_date").distinct().collect()}
    if not target_dates or None in target_dates:
        raise ValueError(f"Missing data_date in changed Silver reports for {fact_name}")
    read_report_dates = set(changed_report_dates)
    if fact_name in {"fact_load_hourly", "fact_generation_daily",
                     "fact_generation_power_daily"}:
        read_report_dates.update(
            d + timedelta(days=offset)
            for d in changed_report_dates for offset in (-1, 1)
        )
    if fact_name in {"fact_generation_daily", "fact_generation_power_daily"}:
        target_dates.update(d + timedelta(days=1) for d in tuple(target_dates))
    changed_partitions = sorted(d.isoformat() for d in target_dates)
    logger.info("Silver changed report dates for %s: %s", fact_name, changes.report_dates)

    min_date = min(changed_partitions)
    max_date = max(changed_partitions)
    source_df = silver_snapshot.where(F.col("report_date").isin(sorted(read_report_dates)))

    fact_df = pipeline_func(spark, source_df)

    target_date_keys = [int(d.replace("-", "")) for d in changed_partitions]
    fact_df = fact_df.where(F.col("date_key").isin(target_date_keys))

    fact_df = fact_df.persist()

    try:
        written_count = fact_df.count()
        if written_count == 0:
            logger.warning(f"Integrity check: {fact_name} generated 0 rows for partitions {changed_partitions}. Write skipped.")
            return

        keys = FACT_GRAINS[fact_name]
        null_key = reduce(lambda left, right: left | right,
                          (F.col(key).isNull() for key in keys))
        if fact_df.where(null_key).limit(1).count():
            raise ValueError(f"Integrity check failed: NULL merge key in {fact_name}")
        if fact_df.groupBy(*keys).count().where(F.col("count") > 1).limit(1).count():
            raise ValueError(f"Integrity check failed: duplicate merge key in {fact_name}")

        validate_fact_invariants(fact_name, fact_df)

        if "energy_source_key" in fact_df.columns:
            dimension_keys = spark.table("nessie.gold.dim_energy_source").select("energy_source_key")
            if fact_df.join(dimension_keys, "energy_source_key", "left_anti").limit(1).count():
                raise ValueError(f"Orphan energy_source_key in {fact_name}")

        audit_row_conservation(fact_name, source_df, fact_df, changed_partitions)

        _merge_gold_rows(spark, table_name, fact_df, FACT_GRAINS[fact_name], "date_key")

        logger.info(f"  -> Wrote {written_count} records for {len(changed_partitions)} partition(s) to {table_name}")
        try:
            record_task_metrics(
                dataset_name=fact_name,
                records_read=written_count,
                records_written=written_count,
                records_rejected=0,
                start_date=min_date,
                end_date=max_date,
            )
        except Exception as exc:
            logger.debug(f"Fact audit metric recording skipped: {exc}")

        if changes.advance_checkpoint:
            advance_gold_snapshot_checkpoint(
                fact_name, source_table, changes.checkpoint_id, changes.snapshot_id,
            )

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
    parser.add_argument("--run-date", type=str, default=None, help="Filter date (YYYY-MM-DD) for specific fact partition update.")
    parser.add_argument("--start-date", type=str, default=None, help="Start date (YYYY-MM-DD) for backfill updates.")
    parser.add_argument("--end-date", type=str, default=None, help="End date (YYYY-MM-DD) for backfill updates.")
    parser.add_argument("--run-mode", choices=["incremental", "backfill"], default="incremental")
    parser.add_argument("--optimize", action="store_true", help="Run Iceberg compaction (rewrite_data_files) on gold tables.")
    args = parser.parse_args()
    run_date = args.run_date if args.run_date not in (None, "", "None") else None
    start_date = args.start_date if args.start_date not in (None, "", "None") else None
    end_date = args.end_date if args.end_date not in (None, "", "None") else None

    spark = create_spark_session("write_gold")
    spark.sparkContext.setLogLevel("WARN")

    try:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold")

        if args.optimize:
            opt_targets = list(FACTS.keys()) if args.dataset in {"all", "facts"} else [args.dataset]
            optimize_gold_tables(spark, opt_targets)
            return

        if args.dataset in {"all", "dims"}:
            for name, gen in DIMENSIONS.items():
                write_dimension_table(spark, name, gen)
        elif args.dataset in DIMENSIONS:
            write_dimension_table(spark, args.dataset, DIMENSIONS[args.dataset])

        if args.dataset in {"all", "facts"}:
            for name, (src, pipe) in FACTS.items():
                write_fact_table(spark, name, src, pipe, run_date=run_date, start_date=start_date, end_date=end_date, run_mode=args.run_mode)
        elif args.dataset in FACTS:
            src, pipe = FACTS[args.dataset]
            write_fact_table(spark, args.dataset, src, pipe, run_date=run_date, start_date=start_date, end_date=end_date, run_mode=args.run_mode)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
