"""Data Quality Control, Circuit Breakers & Volume Skew Detection for Silver Layer.

Protections:
1. Quality Circuit Breaker: Automatically halts write operations if reject rate exceeds threshold (default 10%).
2. Volume Skew / Drop Detector: Detects significant volume drop (> 30%) compared to 7-day historical average.
"""
from datetime import datetime
from typing import Optional
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F

from utils.logging import create_logger

logger = create_logger(__name__)


class QualityCircuitBreakerError(Exception):
    """Raised when data quality rejection rate exceeds safety limits, halting pipeline execution."""
    pass


def enforce_quality_circuit_breaker(
    dataset_name: str,
    valid_count: int,
    reject_count: int,
    max_reject_ratio: float = 0.10,
) -> float:
    """
    Enforce Quality Circuit Breaker on the cleaned dataset.

    Args:
        dataset_name: Name of the Silver dataset.
        valid_count: Number of records meeting all quality rules.
        reject_count: Number of records failing quality rules.
        max_reject_ratio: Maximum allowable ratio of rejected rows (default 0.10 = 10%).

    Returns:
        The calculated reject ratio.

    Raises:
        QualityCircuitBreakerError: If reject_count / total_count > max_reject_ratio.
    """
    total_count = valid_count + reject_count
    if total_count == 0:
        logger.warning(f"Circuit Breaker Check [{dataset_name}]: Batch contains 0 records.")
        return 0.0

    reject_ratio = reject_count / total_count
    logger.info(
        f"Circuit Breaker Check [{dataset_name}]: Valid={valid_count}, "
        f"Reject={reject_count}, Total={total_count}, RejectRate={reject_ratio * 100:.2f}% "
        f"(Limit: {max_reject_ratio * 100:.1f}%)"
    )

    if reject_ratio > max_reject_ratio:
        error_msg = (
            f"🚨 [QUALITY CIRCUIT BREAKER TRIPPED] Dataset '{dataset_name}' exceeded error threshold! "
            f"Reject rate is {reject_ratio * 100:.2f}% ({reject_count}/{total_count} rows), "
            f"which exceeds the maximum allowed {max_reject_ratio * 100:.1f}%. "
            f"Write operation aborted to protect Silver & Gold Lakehouse from corrupted upstream data."
        )
        logger.critical(error_msg)
        raise QualityCircuitBreakerError(error_msg)

    return reject_ratio


def detect_volume_skew(
    spark: SparkSession,
    dataset_name: str,
    table_name: str,
    current_count: int,
    run_date: str | None = None,
    max_drop_ratio: float = 0.30,
) -> Optional[float]:
    """
    Detect significant volume drops compared to the 7-day average of historical daily partition row counts.

    Args:
        spark: Active SparkSession.
        dataset_name: Dataset name for logging.
        table_name: Target Iceberg Silver table.
        current_count: Total records in current batch.
        run_date: Date string (YYYY-MM-DD) of the current run partition.
        max_drop_ratio: Threshold drop ratio to flag warning (default 0.30 = 30%).

    Returns:
        Drop ratio if drop exceeds max_drop_ratio, else None.
    """
    if not spark.catalog.tableExists(table_name):
        logger.info(f"Volume Skew Check [{dataset_name}]: Table {table_name} does not exist yet (Initial run).")
        return None

    try:
        table_df = spark.table(table_name)
        if "data_date" not in table_df.columns:
            return None

        hist_df = table_df
        if run_date:
            hist_df = hist_df.filter(F.col("data_date") < F.to_date(F.lit(run_date)))

        # Group by data_date and get last 7 distinct days
        daily_counts = (
            hist_df.groupBy("data_date")
            .agg(F.count(F.lit(1)).alias("day_count"))
            .orderBy(F.col("data_date").desc())
            .limit(7)
            .collect()
        )

        if not daily_counts or len(daily_counts) < 2:
            logger.info(f"Volume Skew Check [{dataset_name}]: Insufficient history (<2 days) for baseline comparison.")
            return None

        avg_count = sum(r["day_count"] for r in daily_counts) / len(daily_counts)
        if avg_count <= 0:
            return None

        drop_ratio = (avg_count - current_count) / avg_count
        if drop_ratio > max_drop_ratio:
            logger.warning(
                f"⚠️ [VOLUME SKEW WARNING] Significant volume drop detected for {dataset_name}! "
                f"Current batch: {current_count} rows, 7-day historical avg: {avg_count:.1f} rows "
                f"(Sụt giảm -{drop_ratio * 100:.1f}%, vượt ngưỡng cảnh báo {max_drop_ratio * 100:.0f}%). "
                f"Cần kiểm tra thay đổi cấu trúc web nguồn EVN/NSMO hoặc lịch nghỉ lễ."
            )
            return drop_ratio
        else:
            logger.info(
                f"  [✓] Volume health check passed for {dataset_name}: {current_count} rows "
                f"vs 7-day avg {avg_count:.1f} rows (Delta: {-drop_ratio * 100:+.1f}%)."
            )
            return None

    except Exception as exc:
        logger.warning(f"Could not compute volume skew for {dataset_name}: {exc}")
        return None
