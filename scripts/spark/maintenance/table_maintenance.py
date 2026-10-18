"""Apache Iceberg Table Maintenance & Governance Procedure.

Operations:
1. Snapshot Expiration: Purge snapshots older than retention window (default 7 days) while retaining at least 10 snapshots.
2. Orphan Files Removal: Delete unreferenced data and metadata files older than retention window.
3. Compaction (Rewrite Data Files): Consolidate small parquet files into optimized partitions.
"""
import argparse
from datetime import datetime, timedelta, timezone
from pyspark.sql import SparkSession

from utils.spark import create_spark_session
from utils.logging import create_logger

logger = create_logger(__name__)

SILVER_TABLES = [
    "nessie.silver.power",
    "nessie.silver.generation",
    "nessie.silver.hydro",
    "nessie.silver.load",
    "nessie.silver.weather_daily",
    "nessie.silver.weather_hourly",
    "nessie.silver.power_reject",
    "nessie.silver.generation_reject",
    "nessie.silver.hydro_reject",
    "nessie.silver.load_reject",
    "nessie.silver.weather_daily_reject",
    "nessie.silver.weather_hourly_reject",
]

GOLD_TABLES = [
    "nessie.gold.fact_load_hourly",
    "nessie.gold.fact_weather_hourly",
    "nessie.gold.fact_weather_daily",
    "nessie.gold.fact_hydro_status",
    "nessie.gold.fact_generation_daily",
    "nessie.gold.fact_generation_capacity_daily",
    "nessie.gold.dim_date",
    "nessie.gold.dim_time",
    "nessie.gold.dim_region",
    "nessie.gold.dim_location",
    "nessie.gold.dim_plant",
    "nessie.gold.dim_energy_source",
    "nessie.gold.dim_period",
]


def expire_snapshots(spark: SparkSession, table_name: str, retention_days: int = 7, retain_last: int = 10) -> None:
    """Expire Iceberg snapshots older than retention_days while preserving retain_last snapshots."""
    cutoff_dt = datetime.now(timezone.utc) - timedelta(days=retention_days)
    cutoff_ts = cutoff_dt.strftime("%Y-%m-%d %H:%M:%S.000")

    logger.info(f"Expiring snapshots for {table_name} (older than {cutoff_ts}, retain_last={retain_last})...")
    try:
        sql = (
            f"CALL nessie.system.expire_snapshots("
            f"table => '{table_name}', "
            f"older_than => TIMESTAMP '{cutoff_ts}', "
            f"retain_last => {retain_last})"
        )
        spark.sql(sql)
        logger.info(f"  [✓] Expired snapshots successfully on {table_name}")
    except Exception as exc:
        logger.warning(f"Snapshot expiration skipped/failed for {table_name}: {exc}")


def remove_orphan_files(spark: SparkSession, table_name: str, retention_days: int = 7) -> None:
    """Remove orphaned data and metadata files no longer referenced by any snapshot."""
    cutoff_dt = datetime.now(timezone.utc) - timedelta(days=retention_days)
    cutoff_ts = cutoff_dt.strftime("%Y-%m-%d %H:%M:%S.000")

    logger.info(f"Removing orphan files for {table_name} (older than {cutoff_ts})...")
    try:
        sql = (
            f"CALL nessie.system.remove_orphan_files("
            f"table => '{table_name}', "
            f"older_than => TIMESTAMP '{cutoff_ts}')"
        )
        spark.sql(sql)
        logger.info(f"  [✓] Orphan files cleanup completed on {table_name}")
    except Exception as exc:
        logger.warning(f"Orphan files removal skipped/failed for {table_name}: {exc}")


def rewrite_data_files(spark: SparkSession, table_name: str) -> None:
    """Compact small data files to optimize I/O and query performance."""
    logger.info(f"Compacting data files (rewrite_data_files) for {table_name}...")
    try:
        sql = f"CALL nessie.system.rewrite_data_files(table => '{table_name}')"
        spark.sql(sql)
        logger.info(f"  [✓] Compaction completed successfully on {table_name}")
    except Exception as exc:
        logger.warning(f"Compaction skipped/failed for {table_name}: {exc}")


def maintain_table(
    spark: SparkSession,
    table_name: str,
    retention_days: int = 7,
    do_compaction: bool = True,
    do_expire: bool = True,
    do_orphans: bool = True,
) -> None:
    """Perform full maintenance procedure on a single Iceberg table."""
    if not spark.catalog.tableExists(table_name):
        logger.info(f"Table {table_name} does not exist. Skipping maintenance.")
        return

    logger.info(f"\n--- Maintaining Iceberg Table: {table_name} ---")

    if do_compaction:
        rewrite_data_files(spark, table_name)

    if do_expire:
        expire_snapshots(spark, table_name, retention_days=retention_days)

    if do_orphans:
        remove_orphan_files(spark, table_name, retention_days=retention_days)


def main():
    parser = argparse.ArgumentParser(description="Iceberg Table Governance & Maintenance")
    parser.add_argument("--layer", choices=["all", "silver", "gold"], default="all")
    parser.add_argument("--table", type=str, default=None, help="Explicit table name (e.g. nessie.silver.load)")
    parser.add_argument("--retention-days", type=int, default=7, help="Snapshot retention period in days (default: 7)")
    parser.add_argument("--skip-compaction", action="store_true", help="Skip rewrite_data_files")
    parser.add_argument("--skip-expire", action="store_true", help="Skip expire_snapshots")
    parser.add_argument("--skip-orphans", action="store_true", help="Skip remove_orphan_files")
    args = parser.parse_args()

    spark = create_spark_session("iceberg_table_maintenance")
    spark.sparkContext.setLogLevel("WARN")

    try:
        if args.table:
            targets = [args.table]
        elif args.layer == "silver":
            targets = SILVER_TABLES
        elif args.layer == "gold":
            targets = GOLD_TABLES
        else:
            targets = SILVER_TABLES + GOLD_TABLES

        logger.info(f"Starting Iceberg table maintenance for {len(targets)} tables (Retention: {args.retention_days} days)...")
        for tbl in targets:
            maintain_table(
                spark=spark,
                table_name=tbl,
                retention_days=args.retention_days,
                do_compaction=not args.skip_compaction,
                do_expire=not args.skip_expire,
                do_orphans=not args.skip_orphans,
            )
        logger.info("All table maintenance procedures finished successfully.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
