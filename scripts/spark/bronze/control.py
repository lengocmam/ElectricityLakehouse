import os
import time
from datetime import date, datetime
from typing import Any, Optional

import psycopg

from utils.logging import create_logger

logger = create_logger(__name__)


def _clean_param(val: Any) -> Any:
    """Normalize parameters to prevent storing 'None' as a string."""
    if val is None:
        return None
    if isinstance(val, str) and val.strip() in ("", "None", "null", "NULL"):
        return None
    return val


def _get_connection(max_retries: int = 3, retry_delay: float = 1.0):
    """Establish a connection to PostgreSQL lakehouse_control database with bounded retry."""
    host = os.environ.get("WATERMARK_DB_HOST", "postgres")
    port = os.environ.get("WATERMARK_DB_PORT", "5432")
    dbname = os.environ.get("WATERMARK_DB_NAME", "lakehouse_control")
    user = os.environ.get("WATERMARK_DB_USER", "lakehouse_control")
    password = os.environ.get("WATERMARK_DB_PASSWORD", "lakehouse_control")

    last_exc = None
    for attempt in range(1, max_retries + 1):
        try:
            return psycopg.connect(
                host=host,
                port=port,
                dbname=dbname,
                user=user,
                password=password,
                connect_timeout=3,
            )
        except Exception as exc:
            last_exc = exc
            if attempt < max_retries:
                time.sleep(retry_delay * attempt)
    raise last_exc


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


def start_ingestion_log(
    run_id: str,
    dataset_name: str,
    run_mode: str,
    start_date: Optional[date],
    end_date: Optional[date],
    started_at: datetime,
) -> None:
    """Record the start of an ingestion run into pipeline_task_audit."""
    clean_mode = _clean_param(run_mode) or "incremental"
    clean_start = _clean_param(start_date)
    clean_end = _clean_param(end_date)
    norm_dataset = dataset_name.replace("-", "_")

    # Resolve Airflow task identifiers if available, or fallback to standalone defaults
    dag_id = os.environ.get("AIRFLOW_DAG_ID", "electricity_lakehouse_pipeline")
    task_id = os.environ.get("AIRFLOW_TASK_ID", f"bronze_ingestion.ingest_{norm_dataset}")
    airflow_run_id = os.environ.get("AIRFLOW_RUN_ID", run_id)
    try:
        try_number = int(os.environ.get("AIRFLOW_TRY_NUMBER", "1"))
    except ValueError:
        try_number = 1

    try:
        with _get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO pipeline_task_audit (
                        dag_id, task_id, run_id, logical_date, start_time,
                        state, try_number, dataset_name, mode, start_date, end_date
                    ) VALUES (
                        %s, %s, %s, %s, %s,
                        'RUNNING', %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (dag_id, task_id, run_id, try_number) DO UPDATE SET
                        state = 'RUNNING',
                        start_time = EXCLUDED.start_time,
                        dataset_name = EXCLUDED.dataset_name,
                        mode = EXCLUDED.mode,
                        start_date = EXCLUDED.start_date,
                        end_date = EXCLUDED.end_date;
                    """,
                    (
                        dag_id,
                        task_id,
                        airflow_run_id,
                        started_at,
                        started_at,
                        try_number,
                        dataset_name,
                        clean_mode,
                        clean_start,
                        clean_end,
                    ),
                )
    except Exception as exc:
        logger.error(f"Failed to record start ingestion audit for {run_id}: {exc}")


def finish_ingestion_log(
    run_id: str,
    status: str,
    records_count: int = 0,
    error_message: Optional[str] = None,
    finished_at: Optional[datetime] = None,
    duration_seconds: Optional[float] = None,
    records_read: Optional[int] = None,
    records_written: Optional[int] = None,
    records_rejected: Optional[int] = None,
    dataset_name: Optional[str] = None,
    start_date: Optional[Any] = None,
    end_date: Optional[Any] = None,
) -> None:
    """Update task execution record with final metrics in pipeline_task_audit."""
    norm_status = status.upper() if status else "SUCCESS"
    if records_rejected is None:
        records_rejected = 0

    if records_written is None:
        records_written = records_count if norm_status in ("SUCCESS", "PARTIAL") else 0

    if records_read is None:
        records_read = records_count + records_rejected

    if finished_at is None:
        finished_at = datetime.now()

    dag_id = os.environ.get("AIRFLOW_DAG_ID", "electricity_lakehouse_pipeline")
    task_id = os.environ.get("AIRFLOW_TASK_ID")
    if not task_id:
        ds_part = dataset_name.replace("-", "_") if dataset_name else "dataset"
        task_id = f"bronze_ingestion.ingest_{ds_part}"
    airflow_run_id = os.environ.get("AIRFLOW_RUN_ID", run_id)
    try:
        try_number = int(os.environ.get("AIRFLOW_TRY_NUMBER", "1"))
    except ValueError:
        try_number = 1

    try:
        with _get_connection() as conn:
            with conn.cursor() as cur:
                clean_start = _clean_param(start_date)
                clean_end = _clean_param(end_date)
                cur.execute(
                    """
                    INSERT INTO pipeline_task_audit (
                        dag_id, task_id, run_id, end_time, duration_seconds,
                        state, try_number, dataset_name, records_read, records_written, records_rejected,
                        start_date, end_date, error_message
                    ) VALUES (
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s,
                        %s, %s, %s
                    )
                    ON CONFLICT (dag_id, task_id, run_id, try_number) DO UPDATE SET
                        state = EXCLUDED.state,
                        end_time = EXCLUDED.end_time,
                        duration_seconds = EXCLUDED.duration_seconds,
                        dataset_name = COALESCE(EXCLUDED.dataset_name, pipeline_task_audit.dataset_name),
                        records_read = COALESCE(EXCLUDED.records_read, pipeline_task_audit.records_read),
                        records_written = COALESCE(EXCLUDED.records_written, pipeline_task_audit.records_written),
                        records_rejected = COALESCE(EXCLUDED.records_rejected, pipeline_task_audit.records_rejected),
                        start_date = COALESCE(EXCLUDED.start_date, pipeline_task_audit.start_date),
                        end_date = COALESCE(EXCLUDED.end_date, pipeline_task_audit.end_date),
                        error_message = EXCLUDED.error_message;
                    """,
                    (
                        dag_id,
                        task_id,
                        airflow_run_id,
                        finished_at,
                        duration_seconds,
                        norm_status,
                        try_number,
                        dataset_name,
                        records_read,
                        records_written,
                        records_rejected,
                        clean_start,
                        clean_end,
                        error_message,
                    ),
                )
    except Exception as exc:
        logger.error(f"Failed to record finish ingestion audit for {run_id}: {exc}")
