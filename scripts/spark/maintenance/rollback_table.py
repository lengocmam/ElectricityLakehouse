"""Emergency Rollback & Time-Travel Utility for Apache Iceberg Lakehouse.

Usage:
1. List snapshots:
   python scripts/spark/maintenance/rollback_table.py --table nessie.silver.load --list

2. Rollback to snapshot ID:
   python scripts/spark/maintenance/rollback_table.py --table nessie.silver.load --to-snapshot 123456789012345

3. Rollback to timestamp:
   python scripts/spark/maintenance/rollback_table.py --table nessie.silver.load --to-timestamp "2026-10-16 12:00:00"
"""
import argparse
import sys
from datetime import datetime
from pyspark.sql import SparkSession

from utils.spark import create_spark_session
from utils.logging import create_logger

logger = create_logger(__name__)


def list_snapshots(spark: SparkSession, table_name: str, limit: int = 15) -> None:
    """Display table snapshot history for analysis before rollback."""
    snapshot_table = f"{table_name}$snapshots"
    if not spark.catalog.tableExists(table_name):
        logger.error(f"Table {table_name} does not exist.")
        return

    logger.info(f"\n{'='*80}\nSNAPSHOT HISTORY FOR: {table_name}\n{'='*80}")
    try:
        df = spark.sql(
            f"""
            SELECT 
                snapshot_id,
                to_utc_timestamp(committed_at, 'UTC') as committed_at_utc,
                operation,
                summary['total-records'] as total_records,
                summary['added-records'] as added_records,
                summary['deleted-records'] as deleted_records
            FROM {snapshot_table}
            ORDER BY committed_at DESC
            LIMIT {limit}
            """
        )
        df.show(limit, truncate=False)
    except Exception as exc:
        logger.error(f"Failed to query snapshots for {table_name}: {exc}")


def rollback_to_snapshot(spark: SparkSession, table_name: str, snapshot_id: int) -> None:
    """Roll back table state to a specific snapshot ID."""
    logger.info(f"Initiating emergency rollback on {table_name} to snapshot {snapshot_id}...")
    try:
        sql = f"CALL nessie.system.rollback_to_snapshot(table => '{table_name}', snapshot_id => {snapshot_id})"
        spark.sql(sql)
        logger.info(f"  [✓] Rollback SUCCESSFUL! Table {table_name} is now restored to snapshot {snapshot_id}.")
    except Exception as exc:
        logger.error(f"Rollback FAILED on {table_name}: {exc}")
        raise


def rollback_to_timestamp(spark: SparkSession, table_name: str, timestamp_str: str) -> None:
    """Roll back table state to a specific point-in-time timestamp."""
    logger.info(f"Initiating emergency rollback on {table_name} to timestamp {timestamp_str}...")
    try:
        sql = f"CALL nessie.system.rollback_to_timestamp(table => '{table_name}', timestamp => TIMESTAMP '{timestamp_str}')"
        spark.sql(sql)
        logger.info(f"  [✓] Rollback SUCCESSFUL! Table {table_name} is now restored to {timestamp_str}.")
    except Exception as exc:
        logger.error(f"Rollback FAILED on {table_name}: {exc}")
        raise


def main():
    parser = argparse.ArgumentParser(description="Emergency Lakehouse Rollback Utility")
    parser.add_argument("--table", type=str, required=True, help="Target Iceberg table (e.g. nessie.silver.load)")
    parser.add_argument("--list", action="store_true", help="List recent snapshots")
    parser.add_argument("--to-snapshot", type=int, default=None, help="Snapshot ID to rollback to")
    parser.add_argument("--to-timestamp", type=str, default=None, help="Timestamp string (YYYY-MM-DD HH:MM:SS) to rollback to")
    args = parser.parse_args()

    if not args.list and not args.to_snapshot and not args.to_timestamp:
        parser.print_help()
        sys.exit(1)

    spark = create_spark_session("lakehouse_rollback_utility")
    spark.sparkContext.setLogLevel("WARN")

    try:
        if args.list:
            list_snapshots(spark, args.table)

        if args.to_snapshot:
            rollback_to_snapshot(spark, args.table, args.to_snapshot)
            list_snapshots(spark, args.table, limit=5)

        elif args.to_timestamp:
            rollback_to_timestamp(spark, args.table, args.to_timestamp)
            list_snapshots(spark, args.table, limit=5)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
