"""Select Silver report partitions changed since each Gold fact's last successful read."""

from dataclasses import dataclass
from datetime import date

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from bronze.control import _get_connection
from utils.logging import create_logger

logger = create_logger(__name__)


@dataclass(frozen=True)
class SilverPartitionChanges:
    report_dates: list[str]
    snapshot_id: int | None
    checkpoint_id: int | None
    advance_checkpoint: bool


def _ensure_checkpoint_table(cur) -> None:
    cur.execute("""
        CREATE TABLE IF NOT EXISTS gold_silver_snapshot_checkpoint (
            fact_name VARCHAR(128) PRIMARY KEY,
            silver_table VARCHAR(255) NOT NULL,
            last_processed_snapshot_id BIGINT NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """)


def get_gold_snapshot_checkpoint(fact_name: str, silver_table: str) -> int | None:
    """Read the last Silver snapshot fully published by this Gold fact."""
    with _get_connection() as conn:
        with conn.cursor() as cur:
            _ensure_checkpoint_table(cur)
            cur.execute(
                "SELECT silver_table, last_processed_snapshot_id "
                "FROM gold_silver_snapshot_checkpoint WHERE fact_name = %s",
                (fact_name,),
            )
            row = cur.fetchone()
    if row and row[0] != silver_table:
        raise ValueError(
            f"Checkpoint source mismatch for {fact_name}: {row[0]} != {silver_table}"
        )
    return int(row[1]) if row else None


def advance_gold_snapshot_checkpoint(
    fact_name: str, silver_table: str, previous_snapshot_id: int | None,
    snapshot_id: int,
) -> None:
    """Advance only after all writes for the selected Silver snapshot succeed."""
    with _get_connection() as conn:
        with conn.cursor() as cur:
            _ensure_checkpoint_table(cur)
            cur.execute(
                "SELECT silver_table, last_processed_snapshot_id "
                "FROM gold_silver_snapshot_checkpoint WHERE fact_name = %s FOR UPDATE",
                (fact_name,),
            )
            row = cur.fetchone()
            if row:
                if row[0] != silver_table or int(row[1]) != previous_snapshot_id:
                    raise RuntimeError(f"Concurrent or mismatched Gold checkpoint for {fact_name}")
                cur.execute(
                    "UPDATE gold_silver_snapshot_checkpoint "
                    "SET last_processed_snapshot_id = %s, updated_at = CURRENT_TIMESTAMP "
                    "WHERE fact_name = %s",
                    (snapshot_id, fact_name),
                )
            else:
                if previous_snapshot_id is not None:
                    raise RuntimeError(f"Gold checkpoint disappeared for {fact_name}")
                cur.execute(
                    "INSERT INTO gold_silver_snapshot_checkpoint "
                    "(fact_name, silver_table, last_processed_snapshot_id) "
                    "VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
                    (fact_name, silver_table, snapshot_id),
                )
                if cur.rowcount != 1:
                    raise RuntimeError(f"Concurrent Gold checkpoint creation for {fact_name}")


def get_changed_partitions(
    spark: SparkSession,
    fact_name: str,
    silver_table: str,
    run_date: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    run_mode: str = "incremental",
) -> SilverPartitionChanges:
    """Find current report_date partitions changed since the Gold checkpoint.

    Iceberg metadata is read instead of comparing Silver and Gold rows. Explicit
    dates are backfills and never advance the incremental checkpoint.
    """
    if not spark.catalog.tableExists(silver_table):
        logger.warning("Silver source table %s does not exist", silver_table)
        return SilverPartitionChanges([], None, None, False)

    checkpoint_id = get_gold_snapshot_checkpoint(fact_name, silver_table)
    history = (
        spark.table(f"{silver_table}.history")
        .where(F.col("is_current_ancestor"))
        .orderBy("made_current_at")
        .select("snapshot_id")
        .collect()
    )
    ancestor_ids = list(dict.fromkeys(int(row.snapshot_id) for row in history))
    if not ancestor_ids:
        return SilverPartitionChanges([], None, checkpoint_id, False)
    snapshot_id = ancestor_ids[-1]

    explicit = any((run_date, start_date, end_date))
    if run_mode == "backfill" and not explicit:
        raise ValueError("Backfill requires a run date or a date range")
    advance_checkpoint = run_mode == "incremental" and not explicit
    gold_exists = spark.catalog.tableExists(f"nessie.gold.{fact_name}")

    if advance_checkpoint and gold_exists and checkpoint_id == snapshot_id:
        return SilverPartitionChanges([], snapshot_id, checkpoint_id, True)

    pending_ids = None
    if advance_checkpoint and gold_exists and checkpoint_id in ancestor_ids:
        pending_ids = set(ancestor_ids[ancestor_ids.index(checkpoint_id) + 1:])
    elif advance_checkpoint and checkpoint_id is not None and gold_exists:
        logger.warning(
            "Silver snapshot %s is no longer an ancestor for %s; refreshing all report partitions",
            checkpoint_id, fact_name,
        )

    # The partitions metadata table describes only active report partitions.
    # A missing/expired checkpoint refreshes all of them safely.
    partition_rows = (
        spark.table(f"{silver_table}.partitions")
        .select(
            F.col("partition.report_date").alias("report_date"),
            F.col("last_updated_snapshot_id").alias("updated_snapshot_id"),
        )
        .collect()
    )
    active_dates = {
        date.fromisoformat(str(row.report_date))
        for row in partition_rows if row.report_date is not None
    }
    if pending_ids:
        # A removed report has no row in the current partitions metadata.
        # Detect it before advancing the checkpoint; publishing an empty Gold
        # partition needs a separate, explicitly approved deletion policy.
        deleted_rows = (
            spark.table(f"{silver_table}.all_entries")
            .where((F.col("status") == 2) & F.col("snapshot_id").isin(list(pending_ids)))
            .select(F.col("data_file.partition.report_date").alias("report_date"))
            .distinct()
            .collect()
        )
        removed_dates = {
            date.fromisoformat(str(row.report_date))
            for row in deleted_rows if row.report_date is not None
        } - active_dates
        if removed_dates:
            raise RuntimeError(
                f"Silver report partitions were removed from {silver_table}: "
                f"{sorted(d.isoformat() for d in removed_dates)}; "
                "Gold checkpoint was not advanced"
            )

    dates = set()
    for row in partition_rows:
        if row.report_date is None:
            continue
        # Older Iceberg partitions can have a NULL last_updated_snapshot_id.
        # They are not new and must not trigger a full historical refresh.
        if pending_ids is not None and (
            row.updated_snapshot_id is None
            or int(row.updated_snapshot_id) not in pending_ids
        ):
            continue
        report_date = date.fromisoformat(str(row.report_date))
        if run_date and report_date != date.fromisoformat(run_date):
            continue
        if start_date and report_date < date.fromisoformat(start_date):
            continue
        if end_date and report_date > date.fromisoformat(end_date):
            continue
        dates.add(report_date.isoformat())

    return SilverPartitionChanges(
        sorted(dates), snapshot_id, checkpoint_id, advance_checkpoint
    )
