"""Pipeline Monitoring, Anomaly Detection & Alerting Module for Lakehouse.

Features:
- Historical duration baseline computation (Mean, StdDev).
- Task duration anomaly detection (Spike / Dynamic Thresholding).
- PostgreSQL audit logging into `pipeline_task_audit`.
- Multi-channel notification dispatcher (Telegram Bot / Webhook / Airflow Logger).
- Airflow Callbacks: on_failure, on_retry, on_success, sla_miss.
"""
import json
import logging
import math
import os
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any, Optional

logger = logging.getLogger(__name__)

# Try psycopg2 or psycopg3
try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
except ImportError:
    try:
        import psycopg as psycopg2
        RealDictCursor = None
    except ImportError:
        psycopg2 = None
        RealDictCursor = None


def _get_db_connection():
    """Establish connection to PostgreSQL lakehouse_control database."""
    if not psycopg2:
        logger.warning("Neither psycopg2 nor psycopg is installed. Monitoring DB logging disabled.")
        return None

    host = os.environ.get("WATERMARK_DB_HOST", "postgres")
    port = os.environ.get("WATERMARK_DB_PORT", "5432")
    dbname = os.environ.get("WATERMARK_DB_NAME", "lakehouse_control")
    user = os.environ.get("WATERMARK_DB_USER", "lakehouse_control")
    password = os.environ.get("WATERMARK_DB_PASSWORD", "lakehouse_control")

    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password,
            connect_timeout=3,
        )
        return conn
    except Exception as exc:
        logger.warning(f"Could not connect to monitoring database: {exc}")
        return None


def init_audit_table():
    """Ensure pipeline_task_audit table exists in lakehouse_control."""
    conn = _get_db_connection()
    if not conn:
        return
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS pipeline_task_audit (
                        audit_id SERIAL PRIMARY KEY,
                        dag_id VARCHAR(100) NOT NULL,
                        task_id VARCHAR(100) NOT NULL,
                        run_id VARCHAR(150) NOT NULL,
                        logical_date TIMESTAMPTZ,
                        start_time TIMESTAMPTZ,
                        end_time TIMESTAMPTZ,
                        duration_seconds DOUBLE PRECISION,
                        baseline_avg_seconds DOUBLE PRECISION,
                        state VARCHAR(20) NOT NULL,
                        try_number INT NOT NULL DEFAULT 1,
                        is_anomaly BOOLEAN DEFAULT FALSE,
                        anomaly_reason TEXT,
                        error_message TEXT,
                        created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
                    );
                    CREATE INDEX IF NOT EXISTS idx_audit_task_time ON pipeline_task_audit(task_id, start_time DESC);
                    """
                )
    except Exception as exc:
        logger.warning(f"Failed to initialize pipeline_task_audit table: {exc}")
    finally:
        conn.close()


def send_alert(message: str, level: str = "INFO") -> None:
    """
    Dispatch alert notification to Telegram Bot or Webhook.
    Falls back cleanly to logging if channels are not configured.
    """
    logger.info(f"[MONITORING ALERT - {level}]\n{message}")

    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")

    # 1. Dispatch via Telegram Bot if configured
    if bot_token and chat_id:
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": message,
            "parse_mode": "HTML",
        }
        try:
            req = urllib.request.Request(
                url,
                data=json.dumps(payload).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=5) as resp:
                if resp.status == 200:
                    logger.debug("Telegram alert sent successfully.")
        except Exception as exc:
            logger.warning(f"Failed to dispatch Telegram alert: {exc}")

    # 2. Dispatch via Generic Webhook (Slack / Discord / Teams) if configured
    webhook_url = os.environ.get("ALERT_WEBHOOK_URL")
    if webhook_url:
        payload = {"text": message}
        try:
            req = urllib.request.Request(
                webhook_url,
                data=json.dumps(payload).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=5):
                logger.debug("Webhook alert sent successfully.")
        except Exception as exc:
            logger.warning(f"Failed to dispatch webhook alert: {exc}")


def get_task_baseline(task_id: str, dag_id: str, limit: int = 15) -> tuple[Optional[float], Optional[float], int]:
    """
    Query historical successful task durations to calculate mean and standard deviation.
    Returns: (mean_seconds, stddev_seconds, sample_count)
    """
    conn = _get_db_connection()
    if not conn:
        return None, None, 0

    durations: list[float] = []
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT duration_seconds
                    FROM pipeline_task_audit
                    WHERE task_id = %s AND dag_id = %s AND state = 'SUCCESS' AND is_anomaly = FALSE
                    ORDER BY start_time DESC
                    LIMIT %s;
                    """,
                    (task_id, dag_id, limit),
                )
                rows = cur.fetchall()
                durations = [r[0] for r in rows if r[0] is not None and r[0] > 0]
    except Exception as exc:
        logger.warning(f"Failed to query baseline for {task_id}: {exc}")
    finally:
        conn.close()

    count = len(durations)
    if count == 0:
        return None, None, 0

    mean_sec = sum(durations) / count
    if count == 1:
        return mean_sec, 0.0, 1

    variance = sum((x - mean_sec) ** 2 for x in durations) / (count - 1)
    std_sec = math.sqrt(variance)
    return mean_sec, std_sec, count


def check_duration_anomaly(task_id: str, dag_id: str, duration_seconds: float) -> tuple[bool, Optional[str], Optional[float]]:
    """
    Evaluate if task execution duration is abnormal using Dynamic Thresholding & Z-Score.
    Returns: (is_anomaly, reason_description, baseline_mean_seconds)
    """
    mean_sec, std_sec, count = get_task_baseline(task_id, dag_id)

    # Need at least 3 historical runs to form a reliable baseline
    if mean_sec is None or count < 3:
        return False, None, mean_sec

    # Anomaly Rule: Duration > max(mean + 2.5 * stddev, 2.0 * mean, 60s)
    threshold = max(mean_sec + 2.5 * (std_sec or 0.0), 2.0 * mean_sec, 60.0)

    if duration_seconds > threshold:
        spike_pct = ((duration_seconds / mean_sec) - 1.0) * 100.0
        reason = (
            f"Duration {duration_seconds:.1f}s exceeded threshold {threshold:.1f}s "
            f"(Baseline mean: {mean_sec:.1f}s, spike: +{spike_pct:.1f}% over {count} historical runs)"
        )
        return True, reason, mean_sec

    return False, None, mean_sec


def record_task_audit(
    dag_id: str,
    task_id: str,
    run_id: str,
    state: str,
    logical_date: Optional[datetime],
    start_time: Optional[datetime],
    end_time: Optional[datetime],
    duration_seconds: Optional[float],
    baseline_avg_seconds: Optional[float] = None,
    try_number: int = 1,
    is_anomaly: bool = False,
    anomaly_reason: Optional[str] = None,
    error_message: Optional[str] = None,
) -> None:
    """Insert task execution record into pipeline_task_audit table."""
    conn = _get_db_connection()
    if not conn:
        return

    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO pipeline_task_audit (
                        dag_id, task_id, run_id, logical_date, start_time, end_time,
                        duration_seconds, baseline_avg_seconds, state, try_number,
                        is_anomaly, anomaly_reason, error_message
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                    """,
                    (
                        dag_id,
                        task_id,
                        run_id,
                        logical_date,
                        start_time,
                        end_time,
                        duration_seconds,
                        baseline_avg_seconds,
                        state,
                        try_number,
                        is_anomaly,
                        anomaly_reason,
                        error_message,
                    ),
                )
    except Exception as exc:
        logger.warning(f"Failed to record task audit for {task_id}: {exc}")
    finally:
        conn.close()


# =============================================================================
# AIRFLOW CALLBACK HANDLERS
# =============================================================================

def alert_task_failure(context: dict[str, Any]) -> None:
    """Callback triggered when a task fails."""
    ti = context.get("task_instance")
    task = context.get("task")
    dag = context.get("dag")

    dag_id = dag.dag_id if dag else "unknown_dag"
    task_id = ti.task_id if ti else "unknown_task"
    run_id = context.get("run_id", "unknown_run")
    try_number = ti.try_number if ti else 1
    max_tries = task.retries if task else 2
    start_time = ti.start_date if ti else None
    end_time = ti.end_date if ti else datetime.now(timezone.utc)
    duration_sec = ti.duration if ti and ti.duration else None

    exception = context.get("exception")
    error_summary = str(exception)[:400] if exception else "Task failed with unhandled exception"
    log_url = ti.log_url if ti else "N/A"

    # Record in audit DB
    record_task_audit(
        dag_id=dag_id,
        task_id=task_id,
        run_id=run_id,
        state="FAILED",
        logical_date=context.get("logical_date") or context.get("execution_date"),
        start_time=start_time,
        end_time=end_time,
        duration_seconds=duration_sec,
        try_number=try_number,
        is_anomaly=False,
        error_message=error_summary,
    )

    alert_msg = (
        f"🚨 <b>[LAKEHOUSE ALERT] Task Failed!</b>\n"
        f"• <b>DAG:</b> <code>{dag_id}</code>\n"
        f"• <b>Task:</b> <code>{task_id}</code>\n"
        f"• <b>Lần thử:</b> {try_number}/{max_tries}\n"
        f"• <b>Thời gian:</b> {duration_sec:.1f}s\n" if duration_sec else ""
        f"• <b>Lỗi:</b> <i>{error_summary}</i>\n"
        f"• <b>Log:</b> <a href=\"{log_url}\">Xem chi tiết trên Airflow</a>"
    )
    send_alert(alert_msg, level="ERROR")


def alert_task_retry(context: dict[str, Any]) -> None:
    """Callback triggered when a task is scheduled for retry."""
    ti = context.get("task_instance")
    task = context.get("task")
    dag = context.get("dag")

    dag_id = dag.dag_id if dag else "unknown_dag"
    task_id = ti.task_id if ti else "unknown_task"
    run_id = context.get("run_id", "unknown_run")
    try_number = ti.try_number if ti else 1
    max_tries = task.retries if task else 2
    exception = context.get("exception")
    error_summary = str(exception)[:200] if exception else "Retry triggered"

    record_task_audit(
        dag_id=dag_id,
        task_id=task_id,
        run_id=run_id,
        state="UP_FOR_RETRY",
        logical_date=context.get("logical_date") or context.get("execution_date"),
        start_time=ti.start_date if ti else None,
        end_time=ti.end_date if ti else None,
        duration_seconds=ti.duration if ti else None,
        try_number=try_number,
        error_message=error_summary,
    )

    alert_msg = (
        f"⚠️ <b>[LAKEHOUSE RETRY] Task đang thử lại...</b>\n"
        f"• <b>DAG:</b> <code>{dag_id}</code>\n"
        f"• <b>Task:</b> <code>{task_id}</code>\n"
        f"• <b>Lần thử:</b> {try_number}/{max_tries}\n"
        f"• <b>Lý do:</b> <i>{error_summary}</i>"
    )
    send_alert(alert_msg, level="WARNING")


def check_duration_anomaly_and_audit(context: dict[str, Any]) -> None:
    """Callback triggered on successful task execution to audit and detect duration anomalies."""
    ti = context.get("task_instance")
    dag = context.get("dag")

    dag_id = dag.dag_id if dag else "unknown_dag"
    task_id = ti.task_id if ti else "unknown_task"
    run_id = context.get("run_id", "unknown_run")
    duration_sec = ti.duration if ti and ti.duration else 0.0

    # Check for duration anomaly
    is_anomaly, reason, baseline_mean = check_duration_anomaly(task_id, dag_id, duration_sec)

    record_task_audit(
        dag_id=dag_id,
        task_id=task_id,
        run_id=run_id,
        state="SUCCESS",
        logical_date=context.get("logical_date") or context.get("execution_date"),
        start_time=ti.start_date if ti else None,
        end_time=ti.end_date if ti else None,
        duration_seconds=duration_sec,
        baseline_avg_seconds=baseline_mean,
        try_number=ti.try_number if ti else 1,
        is_anomaly=is_anomaly,
        anomaly_reason=reason,
    )

    if is_anomaly and baseline_mean:
        spike_pct = ((duration_sec / baseline_mean) - 1.0) * 100.0
        alert_msg = (
            f"⚠️ <b>[LAKEHOUSE ANOMALY] Thời gian chạy tăng đột biến!</b>\n"
            f"• <b>DAG:</b> <code>{dag_id}</code>\n"
            f"• <b>Task:</b> <code>{task_id}</code>\n"
            f"• <b>Thời gian chạy:</b> {duration_sec:.1f}s ({duration_sec / 60:.1f} phút)\n"
            f"• <b>Trung bình lịch sử:</b> {baseline_mean:.1f}s ({baseline_mean / 60:.1f} phút)\n"
            f"• <b>Độ lệch:</b> +{spike_pct:.1f}% so với thông thường\n"
            f"• <b>Khuyến nghị:</b> Kiểm tra tải Spark Cluster hoặc số lượng bản ghi phân vùng."
        )
        send_alert(alert_msg, level="WARNING")


def alert_sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis) -> None:
    """Callback triggered when an SLA is missed."""
    dag_id = dag.dag_id if dag else "unknown_dag"
    blocking_names = [t.task_id for t in blocking_task_list] if blocking_task_list else []
    alert_msg = (
        f"⏰ <b>[LAKEHOUSE SLA MISSED] Pipeline vi phạm cam kết SLA!</b>\n"
        f"• <b>DAG:</b> <code>{dag_id}</code>\n"
        f"• <b>Các task gây nghẽn:</b> {', '.join(blocking_names) if blocking_names else 'Không xác định'}\n"
        f"• <b>Hành động:</b> Cần rà soát tắc nghẽn tài nguyên trên hệ thống."
    )
    send_alert(alert_msg, level="CRITICAL")
