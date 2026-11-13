"""Record task lifecycle and processing metrics without interrupting data writes."""

import os
from datetime import date, datetime
from typing import Any, Optional

from utils.control_db import get_connection
from utils.logging import create_logger

logger = create_logger(__name__)


def _clean_param(val: Any) -> Any:
    """Normalize parameters to prevent storing 'None' as a string."""
    if val is None:
        return None
    if isinstance(val, str) and val.strip() in ("", "None", "null", "NULL"):
        return None
    return val


def _get_airflow_context(
    fallback_task_id: Optional[str] = None,
    fallback_run_id: Optional[str] = None,
) -> tuple[str, Optional[str], Optional[str], int]:
    """Resolve the audit key, allowing ingestion to supply standalone defaults."""
    dag_id = os.environ.get("AIRFLOW_DAG_ID", "electricity_lakehouse_pipeline")
    task_id = os.environ.get("AIRFLOW_TASK_ID", fallback_task_id)
    run_id = os.environ.get("AIRFLOW_RUN_ID", fallback_run_id)
    try:
        try_number = int(os.environ.get("AIRFLOW_TRY_NUMBER", "1"))
    except ValueError:
        try_number = 1
    return dag_id, task_id, run_id, try_number


def record_task_metrics(
    dataset_name: str,
    records_read: Optional[int] = None,
    records_written: Optional[int] = None,
    records_rejected: Optional[int] = 0,
    mode: Optional[str] = None,
    start_date: Optional[Any] = None,
    end_date: Optional[Any] = None,
    status: Optional[str] = None,
    error_message: Optional[str] = None,
) -> None:
    """Record processing metrics from Spark tasks into pipeline_task_audit.

    Uses UPSERT on (dag_id, task_id, run_id, try_number) to update metrics.
    NO_DATA is a processing hint; the Airflow callback owns the final state.
    """
    dag_id, task_id, airflow_run_id, try_number = _get_airflow_context()

    if not task_id or not airflow_run_id:
        logger.debug(
            f"Standalone execution or missing Airflow context (task_id={task_id}, run_id={airflow_run_id}). Metric logging skipped."
        )
        return

    clean_mode = _clean_param(mode) or "incremental"
    clean_start = _clean_param(start_date)
    clean_end = _clean_param(end_date)
    clean_dataset = _clean_param(dataset_name)
    initial_state = status or "RUNNING"

    try:
        with get_connection(max_retries=3, retry_delay=1.0) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO pipeline_task_audit (
                        dag_id, task_id, run_id, state, try_number,
                        dataset_name, records_read, records_written, records_rejected,
                        mode, start_date, end_date, error_message
                    ) VALUES (
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s, %s
                    )
                    ON CONFLICT (dag_id, task_id, run_id, try_number) DO UPDATE SET
                        state = CASE WHEN EXCLUDED.state = 'NO_DATA' THEN 'NO_DATA'
                                     ELSE pipeline_task_audit.state END,
                        records_read = EXCLUDED.records_read,
                        records_written = EXCLUDED.records_written,
                        records_rejected = EXCLUDED.records_rejected,
                        dataset_name = COALESCE(EXCLUDED.dataset_name, pipeline_task_audit.dataset_name),
                        mode = COALESCE(EXCLUDED.mode, pipeline_task_audit.mode),
                        start_date = COALESCE(EXCLUDED.start_date, pipeline_task_audit.start_date),
                        end_date = COALESCE(EXCLUDED.end_date, pipeline_task_audit.end_date),
                        error_message = COALESCE(EXCLUDED.error_message, pipeline_task_audit.error_message);
                    """,
                    (
                        dag_id, task_id, airflow_run_id, initial_state, try_number,
                        clean_dataset, records_read, records_written, records_rejected,
                        clean_mode, clean_start, clean_end, error_message,
                    ),
                )
        logger.info(
            f"Successfully recorded metrics for {task_id}: read={records_read}, written={records_written}, rejected={records_rejected}"
        )
    except Exception as exc:
        logger.error(
            f"[AUDIT ERROR] Failed to record task metrics for {task_id} (DAG={dag_id}, run_id={airflow_run_id}, try={try_number}) after retries: {exc}. Iceberg data commit unaffected."
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
    dag_id, task_id, airflow_run_id, try_number = _get_airflow_context(
        fallback_task_id=f"bronze_ingestion.ingest_{norm_dataset}",
        fallback_run_id=run_id,
    )

    try:
        with get_connection() as conn:
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

    dag_id, task_id, airflow_run_id, try_number = _get_airflow_context(
        fallback_run_id=run_id,
    )
    if not task_id:
        ds_part = dataset_name.replace("-", "_") if dataset_name else "dataset"
        task_id = f"bronze_ingestion.ingest_{ds_part}"

    try:
        with get_connection() as conn:
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
