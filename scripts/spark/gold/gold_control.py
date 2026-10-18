"""Gold Layer Partition Management & Change Detection Module.

Pure Iceberg-based partition detection without external database dependency.
Watermark is maintained exclusively at the Bronze layer (Single Source of Truth).
"""
import os
from datetime import date, datetime
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F

from utils.logging import create_logger

logger = create_logger(__name__)


def get_changed_partitions(
    spark: SparkSession,
    fact_name: str,
    silver_table: str,
    run_date: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> list[str]:
    """
    Detect which partitions in the Silver table must be loaded/overwritten into Gold.

    Logic:
    1. If explicit start_date and end_date are given (Backfill mode):
       Returns all data_date partitions in Silver between start_date and end_date.
    2. If explicit run_date is given:
       Returns [run_date] if present in Silver.
    3. If no explicit dates are passed:
       - If Gold table does not exist: Returns ALL distinct data_date partitions from Silver (Initial load).
       - If Gold table exists: Returns all data_date partitions in Silver whose date_key is not in Gold (Incremental load).
         Fallback: checks if any partition in Silver has row_count != Gold row_count.

    Returns:
        List of data_date strings formatted as 'YYYY-MM-DD', sorted ascending.
    """
    # 1. Backfill Mode: explicit date range
    if start_date and end_date:
        if not spark.catalog.tableExists(silver_table):
            logger.warning(f"Silver source table {silver_table} does not exist. Cannot process {fact_name} for range.")
            return []

        silver_df = spark.table(silver_table)
        matched = [
            str(row["data_date"])
            for row in silver_df.filter(
                (F.col("data_date") >= F.to_date(F.lit(start_date))) &
                (F.col("data_date") <= F.to_date(F.lit(end_date)))
            ).select("data_date").distinct().sort("data_date").collect()
            if row["data_date"] is not None
        ]
        logger.info(f"Backfill range [{start_date} -> {end_date}]: Found {len(matched)} partition(s) in Silver {silver_table}.")
        return matched

    # 2. Single Run Date Mode
    if run_date:
        if not spark.catalog.tableExists(silver_table):
            logger.warning(f"Silver source table {silver_table} does not exist. Cannot process {fact_name} for {run_date}.")
            return []

        silver_df = spark.table(silver_table)
        if "data_date" not in silver_df.columns:
            logger.warning(f"Silver table {silver_table} missing data_date column. Skipped.")
            return []

        partition_present = silver_df.filter(F.col("data_date").cast("string") == str(run_date)).limit(1).count() > 0
        if not partition_present:
            logger.info(f"Partition {run_date} does NOT exist in Silver {silver_table}. No data to process. Skipped.")
            return []

        logger.info(f"Explicit partition {run_date} verified present in Silver {silver_table}. Proceeding with load.")
        return [run_date]

    # 3. Default Mode (Initial or Daily Incremental)
    if not spark.catalog.tableExists(silver_table):
        logger.warning(f"Silver source table {silver_table} does not exist.")
        return []

    silver_df = spark.table(silver_table)
    if "data_date" not in silver_df.columns:
        logger.warning(f"Silver table {silver_table} does not contain data_date partition column.")
        return []

    gold_table = f"nessie.gold.{fact_name}"
    gold_exists = spark.catalog.tableExists(gold_table)

    # Case A: Initial load - Gold table does not exist yet
    if not gold_exists:
        logger.info(f"Gold table {gold_table} does not exist yet. Initializing full load of all Silver partitions.")
        distinct_dates = [
            str(row["data_date"])
            for row in silver_df.select("data_date").distinct().sort("data_date").collect()
            if row["data_date"] is not None
        ]
        return distinct_dates

    # Case B: Incremental load - Compare Silver and Gold partitions
    logger.info(f"Analyzing changed partitions between {silver_table} and {gold_table}...")
    silver_dates = [
        str(row["data_date"])
        for row in silver_df.select("data_date").distinct().sort("data_date").collect()
        if row["data_date"] is not None
    ]

    gold_df = spark.table(gold_table)
    gold_date_keys = set(
        row["date_key"]
        for row in gold_df.select("date_key").distinct().collect()
        if row["date_key"] is not None
    )

    # Partitions in Silver that do not yet exist in Gold
    changed_partitions = [
        d for d in silver_dates
        if int(d.replace("-", "")) not in gold_date_keys
    ]

    if changed_partitions:
        logger.info(f"Found {len(changed_partitions)} NEW partition(s) not in Gold: {changed_partitions}")
    else:
        # Fallback check: compare row counts to detect updated partitions
        logger.info("No brand new partitions found. Checking row count consistency between Silver and Gold...")
        silver_counts = {
            str(r["data_date"]): r["count"]
            for r in silver_df.groupBy("data_date").agg(F.count(F.lit(1)).alias("count")).collect()
            if r["data_date"] is not None
        }
        gold_counts = {
            f"{str(r['date_key'])[:4]}-{str(r['date_key'])[4:6]}-{str(r['date_key'])[6:8]}": r["count"]
            for r in gold_df.groupBy("date_key").agg(F.count(F.lit(1)).alias("count")).collect()
            if r["date_key"] is not None
        }
        for p_date, s_cnt in silver_counts.items():
            g_cnt = gold_counts.get(p_date, 0)
            if s_cnt != g_cnt:
                logger.info(f"  [~] Partition {p_date} row count differs (Silver: {s_cnt}, Gold: {g_cnt}). Re-processing.")
                changed_partitions.append(p_date)

    changed_partitions.sort()
    return changed_partitions
