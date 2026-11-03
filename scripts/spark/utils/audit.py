
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
    """Establish connection to PostgreSQL lakehouse_control database with bounded retry."""
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
    """Record execution metrics from Silver/Gold Spark tasks into pipeline_task_audit.

    Uses UPSERT on (dag_id, task_id, run_id, try_number) to update metrics without
    overwriting Airflow-controlled lifecycle states or duration baselines.
    """
    dag_id = os.environ.get("AIRFLOW_DAG_ID", "electricity_lakehouse_pipeline")
    task_id = os.environ.get("AIRFLOW_TASK_ID")
    airflow_run_id = os.environ.get("AIRFLOW_RUN_ID")

    try:
        try_number = int(os.environ.get("AIRFLOW_TRY_NUMBER", "1"))
    except ValueError:
        try_number = 1

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
        with _get_connection(max_retries=3, retry_delay=1.0) as conn:
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
