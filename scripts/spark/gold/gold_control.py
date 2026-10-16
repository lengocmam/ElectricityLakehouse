import os
from datetime import date, datetime
from typing import Any, Optional
from zoneinfo import ZoneInfo

import psycopg
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F

from utils.logging import create_logger

logger = create_logger(__name__)

VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")


def _normalize_ts(dt: Any) -> Optional[datetime]:
    """Convert any datetime (naive, aware, or ISO string) to UTC-aware datetime for safe comparison."""
    if dt is None:
        return None
    if isinstance(dt, str):
        try:
            dt = datetime.fromisoformat(dt)
        except Exception:
            return None
    if not isinstance(dt, datetime):
        return None
    if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
        dt = dt.replace(tzinfo=VN_TZ)
    return dt.astimezone(ZoneInfo("UTC"))


def _get_db_connection():
    """Establish connection to PostgreSQL lakehouse_control database if configured."""
    host = os.environ.get("WATERMARK_DB_HOST")
    if not host:
        return None
    try:
        return psycopg.connect(
            host=host,
            port=os.environ.get("WATERMARK_DB_PORT", "5432"),
            dbname=os.environ.get("WATERMARK_DB_NAME", "lakehouse_control"),
            user=os.environ.get("WATERMARK_DB_USER", "lakehouse_control"),
            password=os.environ.get("WATERMARK_DB_PASSWORD", "lakehouse_control"),
            connect_timeout=3,
        )
    except Exception as exc:
        logger.warning(f"Could not connect to watermark database: {exc}. Using Spark metadata fallback.")
        return None


def init_gold_control_table():
    """Ensure the gold_partition_watermark table exists in PostgreSQL."""
    conn = _get_db_connection()
    if not conn:
        return
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS gold_partition_watermark (
                        fact_table VARCHAR(100) NOT NULL,
                        data_date DATE NOT NULL,
                        silver_max_checked_at TIMESTAMPTZ,
                        last_loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        records_count BIGINT DEFAULT 0,
                        PRIMARY KEY (fact_table, data_date)
                    );
                    """
                )
    except Exception as exc:
        logger.warning(f"Could not initialize gold_partition_watermark table: {exc}")
    finally:
        conn.close()


def get_watermark_map(fact_table: str) -> dict[str, Optional[datetime]]:
    """Retrieve existing partition watermarks for a fact table from PostgreSQL."""
    conn = _get_db_connection()
    if not conn:
        return {}
    watermarks = {}
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT data_date, silver_max_checked_at
                    FROM gold_partition_watermark
                    WHERE fact_table = %s
                    """,
                    (fact_table,),
                )
                for row in cur.fetchall():
                    watermarks[row[0].isoformat()] = _normalize_ts(row[1])
    except Exception as exc:
        logger.warning(f"Failed to query gold watermarks for {fact_table}: {exc}")
    finally:
        conn.close()
    return watermarks


def update_gold_watermarks(
    fact_table: str,
    loaded_partitions: list[str],
    silver_df: DataFrame,
    records_count: int = 0,
):
    """Update gold partition watermarks after successful load into Iceberg."""
    conn = _get_db_connection()
    if not conn:
        return

    # Extract max dq_checked_at for each loaded partition from silver_df
    try:
        agg_rows = (
            silver_df
            .filter(F.col("data_date").cast("string").isin(loaded_partitions))
            .groupBy("data_date")
            .agg(F.max("dq_checked_at").alias("max_checked_at"))
            .collect()
        )
        date_ts_map = {
            str(row["data_date"]): row["max_checked_at"]
            for row in agg_rows
        }
    except Exception as exc:
        logger.warning(f"Could not extract max dq_checked_at from silver_df: {exc}")
        date_ts_map = {}

    try:
        with conn:
            with conn.cursor() as cur:
                for p_date in loaded_partitions:
                    silver_ts = _normalize_ts(date_ts_map.get(p_date))
                    cur.execute(
                        """
                        INSERT INTO gold_partition_watermark (
                            fact_table, data_date, silver_max_checked_at, last_loaded_at, records_count
                        ) VALUES (%s, %s, %s, CURRENT_TIMESTAMP, %s)
                        ON CONFLICT (fact_table, data_date) DO UPDATE SET
                            silver_max_checked_at = EXCLUDED.silver_max_checked_at,
                            last_loaded_at = CURRENT_TIMESTAMP,
                            records_count = EXCLUDED.records_count
                        """,
                        (fact_table, p_date, silver_ts, records_count),
                    )
        logger.info(f"Updated gold watermarks for {len(loaded_partitions)} partition(s) of {fact_table}.")
    except Exception as exc:
        logger.warning(f"Failed to update gold watermarks for {fact_table}: {exc}")
    finally:
        conn.close()


def get_changed_partitions(
    spark: SparkSession,
    fact_name: str,
    silver_table: str,
    run_date: str | None = None,
) -> list[str]:
    """
    Detect which partitions in the Silver table have changed or are new,
    and must be loaded/overwritten into the Gold fact table.

    Returns:
        List of data_date strings formatted as 'YYYY-MM-DD', sorted ascending.
    """
    # 1. If explicit run_date is passed, verify it actually exists in the Silver table
    if run_date:
        if not spark.catalog.tableExists(silver_table):
            logger.warning(f"Silver source table {silver_table} does not exist. Cannot process {fact_name} for {run_date}.")
            return []

        silver_df = spark.table(silver_table)
        if "data_date" not in silver_df.columns:
            logger.warning(f"Silver table {silver_table} missing data_date column. Skipped.")
            return []

        # Check if partition actually exists in Silver (has at least 1 record)
        partition_present = silver_df.filter(F.col("data_date").cast("string") == str(run_date)).limit(1).count() > 0
        if not partition_present:
            logger.info(f"Partition {run_date} does NOT exist in Silver {silver_table}. No data to process. Skipped.")
            return []

        logger.info(f"Explicit partition {run_date} verified present in Silver {silver_table}. Proceeding with load.")
        return [run_date]

    if not spark.catalog.tableExists(silver_table):
        logger.warning(f"Silver source table {silver_table} does not exist.")
        return []

    silver_df = spark.table(silver_table)
    if "data_date" not in silver_df.columns:
        logger.warning(f"Silver table {silver_table} does not contain data_date partition column.")
        return []

    gold_table = f"nessie.gold.{fact_name}"
    gold_exists = spark.catalog.tableExists(gold_table)

    # If Gold table does not exist yet, all distinct Silver partitions are new
    if not gold_exists:
        logger.info(f"Gold table {gold_table} does not exist yet. Initializing full load of all Silver partitions.")
        distinct_dates = [
            str(row["data_date"])
            for row in silver_df.select("data_date").distinct().sort("data_date").collect()
            if row["data_date"] is not None
        ]
        return distinct_dates

    # 2. Gold table exists: Compare partitions between Silver and Gold
    logger.info(f"Analyzing changed partitions between {silver_table} and {gold_table}...")

    # Group Silver by partition with metadata
    silver_summary = (
        silver_df
        .groupBy("data_date")
        .agg(
            F.count(F.lit(1)).alias("silver_count"),
            F.max("dq_checked_at").alias("silver_max_checked_at"),
        )
        .collect()
    )

    if not silver_summary:
        logger.info(f"Silver table {silver_table} is empty. No partitions to process.")
        return []

    # Get distinct date_keys currently in Gold
    gold_df = spark.table(gold_table)
    gold_date_keys = set(
        row["date_key"]
        for row in gold_df.select("date_key").distinct().collect()
        if row["date_key"] is not None
    )

    # Check PostgreSQL watermark records if available
    watermarks = get_watermark_map(fact_name)

    changed_partitions: list[str] = []

    for row in silver_summary:
        raw_date = row["data_date"]
        if raw_date is None:
            continue

        p_date_str = str(raw_date)
        date_key = int(p_date_str.replace("-", ""))

        # Check 1: Partition is completely new (missing in Gold)
        if date_key not in gold_date_keys:
            logger.info(f"  [+] Partition {p_date_str} (key: {date_key}) is NEW in Silver (not in Gold).")
            changed_partitions.append(p_date_str)
            continue

        # Check 2: Watermark comparison (Silver max dq_checked_at is newer than last gold load)
        if p_date_str in watermarks:
            last_silver_ts = _normalize_ts(watermarks[p_date_str])
            current_silver_ts = _normalize_ts(row["silver_max_checked_at"])
            if current_silver_ts and last_silver_ts and current_silver_ts > last_silver_ts:
                logger.info(
                    f"  [~] Partition {p_date_str} has UPDATED data in Silver "
                    f"({current_silver_ts} > {last_silver_ts})."
                )
                changed_partitions.append(p_date_str)
                continue
        elif watermarks:
            # Watermarks exist for other partitions, but not this one
            logger.info(f"  [~] Partition {p_date_str} has no recorded watermark in Gold. Marking as changed.")
            changed_partitions.append(p_date_str)
            continue

    # 3. If watermarks are not configured or empty, fallback to checking record count consistency
    if not watermarks and not changed_partitions and gold_exists:
        logger.info("Watermark database empty/unreachable. Running fallback check on partition presence...")
        # If any partition is missing or has 0 rows in Gold
        gold_counts = {
            row["date_key"]: row["count"]
            for row in gold_df.groupBy("date_key").agg(F.count(F.lit(1)).alias("count")).collect()
        }
        for row in silver_summary:
            p_date_str = str(row["data_date"])
            date_key = int(p_date_str.replace("-", ""))
            if gold_counts.get(date_key, 0) == 0:
                changed_partitions.append(p_date_str)

    changed_partitions.sort()
    return changed_partitions
