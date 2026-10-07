import os
from datetime import date, datetime
from typing import Optional

import psycopg

from utils.logging import create_logger


def _get_connection():
    """Establish a connection to the watermark PostgreSQL database."""
    return psycopg.connect(
        host=os.environ["WATERMARK_DB_HOST"],
        port=os.environ.get("WATERMARK_DB_PORT", "5432"),
        dbname=os.environ["WATERMARK_DB_NAME"],
        user=os.environ["WATERMARK_DB_USER"],
        password=os.environ["WATERMARK_DB_PASSWORD"],
    )


def get_watermark(dataset_name: str) -> Optional[date]:
    """Get the last successfully processed data date."""
    with _get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT last_successful_data_date
                FROM pipeline_watermark
                WHERE dataset_name = %s
                """,
                (dataset_name,),
            )

            row = cur.fetchone()

    return row[0] if row else None


def update_watermark(dataset_name: str, data_date: date) -> None:
    """Update the watermark after a successful pipeline run."""
    with _get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline_watermark (
                    dataset_name,
                    last_successful_data_date,
                    last_successful_run_at
                )
                VALUES (%s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (dataset_name)
                DO UPDATE SET
                    last_successful_data_date = EXCLUDED.last_successful_data_date,
                    last_successful_run_at = CURRENT_TIMESTAMP
                """,
                (dataset_name, data_date),
            )
logger = create_logger(__name__)


def start_ingestion_log(
    run_id: str,
    dataset_name: str,
    run_mode: str,
    start_date: Optional[date],
    end_date: Optional[date],
    started_at: datetime,
) -> None:
    """Record the start of an ingestion run."""
    try:
        with _get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO bronze_ingestion_log (
                        run_id, dataset_name, run_mode, start_date, end_date,
                        status, started_at
                    ) VALUES (
                        %s, %s, %s, %s, %s,
                        'RUNNING', %s
                    )
                    ON CONFLICT (run_id) DO UPDATE SET
                        dataset_name = EXCLUDED.dataset_name,
                        run_mode = EXCLUDED.run_mode,
                        start_date = EXCLUDED.start_date,
                        end_date = EXCLUDED.end_date,
                        status = EXCLUDED.status,
                        started_at = EXCLUDED.started_at
                    """,
                    (
                        run_id,
                        dataset_name,
                        run_mode,
                        start_date,
                        end_date,
                        started_at,
                    )
                )
    except Exception as exc:
        logger.error(f"Failed to start ingestion log for {run_id}: {exc}")


def finish_ingestion_log(
    run_id: str,
    status: str,
    records_count: int,
    error_message: Optional[str],
    finished_at: datetime,
    duration_seconds: float,
) -> None:
    """Update the execution log with the final result of the run."""
    try:
        with _get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE bronze_ingestion_log
                    SET
                        status = %s,
                        records_count = %s,
                        error_message = %s,
                        finished_at = %s,
                        duration_seconds = %s
                    WHERE run_id = %s
                    """,
                    (
                        status,
                        records_count,
                        error_message,
                        finished_at,
                        duration_seconds,
                        run_id,
                    )
                )
    except Exception as exc:
        logger.error(f"Failed to finish ingestion log for {run_id}: {exc}")
