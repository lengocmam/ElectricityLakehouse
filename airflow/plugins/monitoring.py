"""Pipeline Monitoring, Anomaly Detection & Alerting Module for Lakehouse.

Features:
- Historical duration baseline computation (Mean, StdDev).
- Task duration anomaly detection (Spike / Dynamic Thresholding).
- PostgreSQL audit logging into `pipeline_task_audit`.
- DAG run lifecycle audit logging into `pipeline_dag_audit`.
- Alert event history logging into `alert_event`.
- Table/Dataset level data lineage tracking into `data_lineage`.
- Multi-channel notification dispatcher (Telegram Bot / Webhook / Airflow Logger).
- Airflow Callbacks: on_failure, on_retry, on_success, sla_miss, dag_success, dag_failure.
"""
import json
import time
import logging
import math
import os
import re
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, timezone
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


def _get_db_connection(max_retries: int = 3, retry_delay: float = 0.5):
    """Establish connection to PostgreSQL lakehouse_control database with bounded retry."""
    if not psycopg2:
        logger.warning("Neither psycopg2 nor psycopg is installed. Monitoring DB logging disabled.")
        return None

    host = os.environ.get("WATERMARK_DB_HOST", "postgres")
    port = os.environ.get("WATERMARK_DB_PORT", "5432")
    dbname = os.environ.get("WATERMARK_DB_NAME", "lakehouse_control")
    user = os.environ.get("WATERMARK_DB_USER", "lakehouse_control")
    password = os.environ.get("WATERMARK_DB_PASSWORD", "lakehouse_control")

    last_exc = None
    for attempt in range(1, max_retries + 1):
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
            last_exc = exc
            if attempt < max_retries:
                time.sleep(retry_delay * attempt)

    logger.warning(f"Could not connect to monitoring database after {max_retries} attempts: {last_exc}")
    return None


def _clean_param_val(val: Any) -> Any:
    """Normalize parameter values: turn string 'None', empty string or None into None."""
    if val is None:
        return None
    if isinstance(val, str) and val.strip() in ("", "None", "null", "NULL"):
        return None
    return val


def init_monitoring_tables():
    """Ensure all monitoring, audit, alert, and lineage tables exist and are up to date."""
    conn = _get_db_connection()
    if not conn:
        return
    try:
        with conn:
            with conn.cursor() as cur:
                # 1. Base pipeline_task_audit table and columns
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
                        dataset_name VARCHAR(100),
                        records_read BIGINT,
                        records_written BIGINT,
                        records_rejected BIGINT,
                        mode VARCHAR(50),
                        start_date DATE,
                        end_date DATE,
                        created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                        CONSTRAINT uq_pipeline_task_audit_try UNIQUE (dag_id, task_id, run_id, try_number)
                    );

                    CREATE INDEX IF NOT EXISTS idx_audit_task_time ON pipeline_task_audit(task_id, start_time DESC);
                    CREATE INDEX IF NOT EXISTS idx_audit_dag_task_time ON pipeline_task_audit(dag_id, task_id, start_time DESC);
                    CREATE INDEX IF NOT EXISTS idx_audit_dag_run ON pipeline_task_audit(dag_id, run_id);
                    CREATE INDEX IF NOT EXISTS idx_audit_task_state ON pipeline_task_audit(state);
                    CREATE INDEX IF NOT EXISTS idx_audit_dataset ON pipeline_task_audit(dataset_name) WHERE dataset_name IS NOT NULL;

                    -- 2. pipeline_dag_audit table
                    CREATE TABLE IF NOT EXISTS pipeline_dag_audit (
                        audit_id SERIAL PRIMARY KEY,
                        dag_id VARCHAR(100) NOT NULL,
                        run_id VARCHAR(150) NOT NULL,
                        logical_date TIMESTAMPTZ,
                        run_type VARCHAR(50),
                        start_time TIMESTAMPTZ,
                        end_time TIMESTAMPTZ,
                        duration_seconds DOUBLE PRECISION,
                        state VARCHAR(20) NOT NULL,
                        error_message TEXT,
                        created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                        CONSTRAINT uq_pipeline_dag_audit UNIQUE (dag_id, run_id)
                    );
                    CREATE INDEX IF NOT EXISTS idx_dag_audit_lookup ON pipeline_dag_audit(dag_id, start_time DESC);
                    CREATE INDEX IF NOT EXISTS idx_dag_audit_state ON pipeline_dag_audit(state);

                    -- 3. alert_event table
                    CREATE TABLE IF NOT EXISTS alert_event (
                        alert_id SERIAL PRIMARY KEY,
                        dag_id VARCHAR(100),
                        task_id VARCHAR(100),
                        run_id VARCHAR(150),
                        dataset_name VARCHAR(100),
                        alert_type VARCHAR(50) NOT NULL,
                        severity VARCHAR(20) NOT NULL DEFAULT 'INFO',
                        status VARCHAR(20) NOT NULL DEFAULT 'OPEN',
                        message TEXT NOT NULL,
                        created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                        resolved_at TIMESTAMPTZ,
                        notification_status VARCHAR(20) DEFAULT 'PENDING',
                        notification_error TEXT
                    );
                    CREATE INDEX IF NOT EXISTS idx_alert_event_lookup ON alert_event(dag_id, task_id, run_id);
                    CREATE INDEX IF NOT EXISTS idx_alert_event_created ON alert_event(created_at DESC);
                    CREATE INDEX IF NOT EXISTS idx_alert_event_type ON alert_event(alert_type, severity);
                    CREATE INDEX IF NOT EXISTS idx_alert_event_status ON alert_event(status);

                    -- 4. data_lineage table
                    CREATE TABLE IF NOT EXISTS data_lineage (
                        lineage_id SERIAL PRIMARY KEY,
                        run_id VARCHAR(150),
                        dag_id VARCHAR(100) NOT NULL,
                        task_id VARCHAR(100) NOT NULL,
                        source_name VARCHAR(150) NOT NULL,
                        source_layer VARCHAR(50) NOT NULL,
                        target_name VARCHAR(150) NOT NULL,
                        target_layer VARCHAR(50) NOT NULL,
                        transformation_name VARCHAR(150),
                        created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                        CONSTRAINT uq_data_lineage UNIQUE (dag_id, task_id, run_id, source_name, target_name)
                    );
                    CREATE INDEX IF NOT EXISTS idx_data_lineage_target ON data_lineage(target_name, created_at DESC);
                    CREATE INDEX IF NOT EXISTS idx_data_lineage_source ON data_lineage(source_name, created_at DESC);
                    CREATE INDEX IF NOT EXISTS idx_data_lineage_dag_run ON data_lineage(dag_id, run_id);

                    -- 5. Last Silver snapshot fully published by each Gold fact
                    CREATE TABLE IF NOT EXISTS gold_silver_snapshot_checkpoint (
                        fact_name VARCHAR(128) PRIMARY KEY,
                        silver_table VARCHAR(255) NOT NULL,
                        last_processed_snapshot_id BIGINT NOT NULL,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                    );

                    -- 6. Reconciliation view for incomplete or abnormal audits
                    DROP VIEW IF EXISTS v_incomplete_task_audits;
                    CREATE VIEW v_incomplete_task_audits AS
                    SELECT
                        audit_id,
                        dag_id,
                        task_id,
                        run_id,
                        try_number,
                        logical_date,
                        start_time,
                        end_time,
                        duration_seconds,
                        state,
                        dataset_name,
                        records_read,
                        records_written,
                        records_rejected,
                        error_message,
                        CASE
                            WHEN task_id IN ('start', 'end') OR task_id LIKE '%%maintenance%%' OR task_id LIKE '%%backup%%' 
                                THEN 'NOT_APPLICABLE'
                            WHEN state = 'RUNNING' AND start_time < NOW() - INTERVAL '2 hours' 
                                THEN 'STALE_RUNNING'
                            WHEN state = 'RUNNING' 
                                THEN 'IN_PROGRESS'
                            WHEN state = 'SUCCESS' AND records_written IS NULL 
                                THEN 'MISSING_METRICS'
                            WHEN state = 'FAILED' AND records_read IS NULL 
                                THEN 'FAILED_PRE_EXECUTION'
                            WHEN state = 'FAILED' AND records_read IS NOT NULL 
                                THEN 'FAILED_WITH_METRICS'
                            WHEN state = 'UP_FOR_RETRY' 
                                THEN 'IN_RETRY'
                            WHEN state = 'SUCCESS' AND records_written IS NOT NULL 
                                THEN 'OK'
                            ELSE 'OK'
                        END AS audit_status
                    FROM pipeline_task_audit;

                    """
                )
    except Exception as exc:
        logger.warning(f"Failed to initialize monitoring tables: {exc}")
    finally:
        conn.close()


def init_audit_table():
    """Backward-compatible alias for init_monitoring_tables()."""
    init_monitoring_tables()


# =============================================================================
# DATA LINEAGE CATALOG
# =============================================================================

TASK_DATASET_MAP: dict[str, str] = {
    # Bronze layer tasks
    "bronze_ingestion.ingest_evn": "evn",
    "bronze_ingestion.ingest_evn_hydro": "evn_hydro",
    "bronze_ingestion.ingest_nsmo_load": "nsmo_load",
    "bronze_ingestion.ingest_nsmo_power": "nsmo_power",
    "bronze_ingestion.ingest_open_meteo": "open_meteo",
    # Silver layer tasks
    "silver_processing.silver_power_daily": "power_daily",
    "silver_processing.silver_generation": "generation",
    "silver_processing.silver_hydro": "hydro",
    "silver_processing.silver_load": "nsmo_load",
    "silver_processing.silver_power_hourly": "power_hourly",
    "silver_processing.silver_weather_daily": "weather_daily",
    "silver_processing.silver_weather_hourly": "weather_hourly",
    # Gold layer tasks
    "gold_processing.gold_dimensions": "dims",
    "gold_processing.gold_load_hourly": "nsmo_load",
    "gold_processing.gold_generation_power_hourly": "fact_generation_power_hourly",
    "gold_processing.gold_weather_hourly": "fact_weather_hourly",
    "gold_processing.gold_weather_daily": "fact_weather_daily",
    "gold_processing.gold_hydro_status": "fact_hydro_status",
    "gold_processing.gold_generation_daily": "fact_generation_daily",
    "gold_processing.gold_generation_power_daily": "fact_generation_power_daily",
}

LINEAGE_REGISTRY: dict[str, list[tuple[str, str, str, str, str]]] = {
    # Bronze layer tasks
    "bronze_ingestion.ingest_evn": [
        ("api.evn.com.vn", "SOURCE", "nessie.bronze.evn", "BRONZE", "bronze/ingest_bronze.py:evn")
    ],
    "bronze_ingestion.ingest_evn_hydro": [
        ("api.evn.com.vn/hydro", "SOURCE", "nessie.bronze.evn_hydro", "BRONZE", "bronze/ingest_bronze.py:evn_hydro")
    ],
    "bronze_ingestion.ingest_nsmo_load": [
        ("www.nsmo.vn", "SOURCE", "nessie.bronze.nsmo_load", "BRONZE", "bronze/ingest_bronze.py:nsmo_load")
    ],
    "bronze_ingestion.ingest_nsmo_power": [
        ("www.nsmo.vn", "SOURCE", "nessie.bronze.nsmo_power", "BRONZE", "bronze/nsmo_power.py")
    ],
    "bronze_ingestion.ingest_open_meteo": [
        ("archive-api.open-meteo.com", "SOURCE", "nessie.bronze.open_meteo", "BRONZE", "bronze/ingest_bronze.py:open_meteo")
    ],
    # Silver layer tasks
    "silver_processing.silver_power_daily": [
        ("nessie.bronze.evn", "BRONZE", "nessie.silver.power_daily", "SILVER", "silver/write_silver.py:power_daily"),
        ("nessie.bronze.evn", "BRONZE", "nessie.silver.power_daily_reject", "SILVER", "silver/write_silver.py:power_daily_reject"),
    ],
    "silver_processing.silver_generation": [
        ("nessie.bronze.evn", "BRONZE", "nessie.silver.generation", "SILVER", "silver/write_silver.py:generation"),
        ("nessie.bronze.evn", "BRONZE", "nessie.silver.generation_reject", "SILVER", "silver/write_silver.py:generation_reject"),
    ],
    "silver_processing.silver_hydro": [
        ("nessie.bronze.evn_hydro", "BRONZE", "nessie.silver.hydro", "SILVER", "silver/write_silver.py:hydro"),
        ("nessie.bronze.evn_hydro", "BRONZE", "nessie.silver.hydro_reject", "SILVER", "silver/write_silver.py:hydro_reject"),
    ],
    "silver_processing.silver_load": [
        ("nessie.bronze.nsmo_load", "BRONZE", "nessie.silver.load", "SILVER", "silver/write_silver.py:load"),
        ("nessie.bronze.nsmo_load", "BRONZE", "nessie.silver.load_reject", "SILVER", "silver/write_silver.py:load_reject"),
    ],
    "silver_processing.silver_power_hourly": [
        ("nessie.bronze.nsmo_power", "BRONZE", "nessie.silver.power_hourly", "SILVER", "silver/write_silver.py:power_hourly"),
        ("nessie.bronze.nsmo_power", "BRONZE", "nessie.silver.power_hourly_reject", "SILVER", "silver/write_silver.py:power_hourly_reject"),
    ],
    "silver_processing.silver_weather_daily": [
        ("nessie.bronze.open_meteo", "BRONZE", "nessie.silver.weather_daily", "SILVER", "silver/write_silver.py:weather_daily"),
        ("nessie.bronze.open_meteo", "BRONZE", "nessie.silver.weather_daily_reject", "SILVER", "silver/write_silver.py:weather_daily_reject"),
    ],
    "silver_processing.silver_weather_hourly": [
        ("nessie.bronze.open_meteo", "BRONZE", "nessie.silver.weather_hourly", "SILVER", "silver/write_silver.py:weather_hourly"),
        ("nessie.bronze.open_meteo", "BRONZE", "nessie.silver.weather_hourly_reject", "SILVER", "silver/write_silver.py:weather_hourly_reject"),
    ],
    # Gold layer tasks
    "gold_processing.gold_dimensions": [
        ("nessie.silver.hydro", "SILVER", "nessie.gold.dim_plant", "GOLD", "gold/write_gold.py:dim_plant"),
        ("generator", "SOURCE", "nessie.gold.dim_date", "GOLD", "gold/write_gold.py:dim_date"),
        ("generator", "SOURCE", "nessie.gold.dim_time", "GOLD", "gold/write_gold.py:dim_time"),
        ("generator", "SOURCE", "nessie.gold.dim_region", "GOLD", "gold/write_gold.py:dim_region"),
        ("generator", "SOURCE", "nessie.gold.dim_location", "GOLD", "gold/write_gold.py:dim_location"),
        ("generator", "SOURCE", "nessie.gold.dim_energy_source", "GOLD", "gold/write_gold.py:dim_energy_source"),
        ("generator", "SOURCE", "nessie.gold.dim_period", "GOLD", "gold/write_gold.py:dim_period"),
    ],
    "gold_processing.gold_load_hourly": [
        ("nessie.silver.load", "SILVER", "nessie.gold.fact_load_hourly", "GOLD", "gold/write_gold.py:fact_load_hourly"),
    ],
    "gold_processing.gold_generation_power_hourly": [
        ("nessie.silver.power_hourly", "SILVER", "nessie.gold.fact_generation_power_hourly", "GOLD", "gold/write_gold.py:fact_generation_power_hourly"),
    ],
    "gold_processing.gold_weather_hourly": [
        ("nessie.silver.weather_hourly", "SILVER", "nessie.gold.fact_weather_hourly", "GOLD", "gold/write_gold.py:fact_weather_hourly"),
    ],
    "gold_processing.gold_weather_daily": [
        ("nessie.silver.weather_daily", "SILVER", "nessie.gold.fact_weather_daily", "GOLD", "gold/write_gold.py:fact_weather_daily"),
    ],
    "gold_processing.gold_hydro_status": [
        ("nessie.silver.hydro", "SILVER", "nessie.gold.fact_hydro_status", "GOLD", "gold/write_gold.py:fact_hydro_status"),
    ],
    "gold_processing.gold_generation_daily": [
        ("nessie.silver.generation", "SILVER", "nessie.gold.fact_generation_daily", "GOLD", "gold/write_gold.py:fact_generation_daily"),
    ],
    "gold_processing.gold_generation_power_daily": [
        ("nessie.silver.power_daily", "SILVER", "nessie.gold.fact_generation_power_daily", "GOLD", "gold/write_gold.py:fact_generation_power_daily"),
    ],
}


def record_task_lineage(dag_id: str, task_id: str, run_id: Optional[str],
                        records_written: Optional[int] = None,
                        records_rejected: Optional[int] = None) -> None:
    """Record registered table lineage for a completed task with committed rows."""
    entries = LINEAGE_REGISTRY.get(task_id)
    if not entries and "." in task_id:
        entries = LINEAGE_REGISTRY.get(task_id.split(".")[-1])
    if not entries:
        return

    conn = _get_db_connection()
    if not conn:
        return

    try:
        with conn:
            with conn.cursor() as cur:
                for src_name, src_layer, tgt_name, tgt_layer, transform in entries:
                    if tgt_name.endswith("_reject") and records_rejected == 0:
                        continue
                    if not tgt_name.endswith("_reject") and records_written == 0:
                        continue
                    cur.execute(
                        """
                        INSERT INTO data_lineage (
                            run_id, dag_id, task_id, source_name, source_layer,
                            target_name, target_layer, transformation_name
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (dag_id, task_id, run_id, source_name, target_name) DO NOTHING;
                        """,
                        (run_id, dag_id, task_id, src_name, src_layer, tgt_name, tgt_layer, transform),
                    )
    except Exception as exc:
        logger.warning(f"Failed to record data lineage for {task_id}: {exc}")
    finally:
        conn.close()


# =============================================================================
# ALERTING & NOTIFICATION DISPATCHER
# =============================================================================

def _sanitize_message(message: str) -> str:
    """Redact sensitive credentials, tokens, or passwords from alert texts."""
    clean = re.sub(r'(token|password|secret|access_key|api_key)[=:]\s*["\']?[^"\'\s&]+["\']?', r'\1=***', message, flags=re.IGNORECASE)
    clean = re.sub(r'bot\d+:[A-Za-z0-9_-]+', 'bot***:***', clean)
    return clean


def send_alert(
    message: str,
    level: str = "INFO",
    dag_id: Optional[str] = None,
    task_id: Optional[str] = None,
    run_id: Optional[str] = None,
    dataset_name: Optional[str] = None,
    alert_type: Optional[str] = None,
) -> None:
    """
    Dispatch alert notification to Telegram Bot or Webhook and log into alert_event table.
    Ensures that database logging failures never break the pipeline or hide original errors.
    """
    logger.info(f"[MONITORING ALERT - {level}]\n{message}")
    sanitized_message = _sanitize_message(message)

    if not alert_type:
        if "SLA" in sanitized_message:
            alert_type = "SLA_MISSED"
        elif "ANOMALY" in sanitized_message:
            alert_type = "DURATION_ANOMALY"
        elif "DAG FAILED" in sanitized_message:
            alert_type = "DAG_FAILURE"
        elif level == "ERROR":
            alert_type = "TASK_FAILURE"
        elif level == "WARNING":
            alert_type = "TASK_RETRY"
        else:
            alert_type = "GENERAL_ALERT"

    # 1. Record alert in PostgreSQL alert_event table
    alert_id = None
    conn = _get_db_connection()
    if conn:
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO alert_event (
                            dag_id, task_id, run_id, dataset_name, alert_type,
                            severity, status, message, notification_status
                        ) VALUES (%s, %s, %s, %s, %s, %s, 'OPEN', %s, 'PENDING')
                        RETURNING alert_id;
                        """,
                        (dag_id, task_id, run_id, dataset_name, alert_type, level, sanitized_message),
                    )
                    row = cur.fetchone()
                    if row:
                        alert_id = row[0]
        except Exception as exc:
            logger.warning(f"Could not persist alert event to database: {exc}")
        finally:
            conn.close()

    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    webhook_url = os.environ.get("ALERT_WEBHOOK_URL")

    notification_status = "NOT_CONFIGURED"
    notification_error = None

    # 2. Dispatch via Telegram Bot if configured
    if bot_token and chat_id:
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": sanitized_message,
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
                    notification_status = "SENT"
                    logger.debug("Telegram alert sent successfully.")
        except Exception as exc:
            notification_status = "FAILED"
            notification_error = str(exc)
            logger.warning(f"Failed to dispatch Telegram alert: {exc}")

    # 3. Dispatch via Generic Webhook if configured
    if webhook_url:
        payload = {"text": sanitized_message}
        try:
            req = urllib.request.Request(
                webhook_url,
                data=json.dumps(payload).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=5):
                notification_status = "SENT"
                logger.debug("Webhook alert sent successfully.")
        except Exception as exc:
            notification_status = "FAILED"
            notification_error = (
                f"{notification_error}; Webhook error: {exc}"
                if notification_error
                else str(exc)
            )
            logger.warning(f"Failed to dispatch webhook alert: {exc}")

    # 4. Update alert notification status in alert_event
    if alert_id:
        conn = _get_db_connection()
        if conn:
            try:
                with conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            UPDATE alert_event
                            SET notification_status = %s, notification_error = %s
                            WHERE alert_id = %s;
                            """,
                            (notification_status, notification_error, alert_id),
                        )
            except Exception as exc:
                logger.warning(f"Failed to update alert notification status: {exc}")
            finally:
                conn.close()


# =============================================================================
# HISTORICAL BASELINE & ANOMALY DETECTION
# =============================================================================

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


# =============================================================================
# AUDIT RECORDING FUNCTIONS
# =============================================================================

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
    dataset_name: Optional[str] = None,
    records_read: Optional[int] = None,
    records_written: Optional[int] = None,
    records_rejected: Optional[int] = None,
    mode: Optional[str] = None,
    start_date: Optional[Any] = None,
    end_date: Optional[Any] = None,
) -> None:
    """
    Insert or update task execution record into pipeline_task_audit table.
    Uses UPSERT on (dag_id, task_id, run_id, try_number) to avoid duplicate records
    and preserve data metrics recorded by the processing tasks.
    """
    conn = _get_db_connection()
    if not conn:
        return

    mode = _clean_param_val(mode)
    start_date = _clean_param_val(start_date)
    end_date = _clean_param_val(end_date)
    dataset_name = _clean_param_val(dataset_name)

    # Sanitize datetime parameters against invalid/mock types
    logical_date = logical_date if isinstance(logical_date, (datetime, date)) else None
    start_time = start_time if isinstance(start_time, (datetime, date)) else None
    end_time = end_time if isinstance(end_time, (datetime, date)) else None
    duration_seconds = float(duration_seconds) if duration_seconds is not None and not isinstance(duration_seconds, bool) and isinstance(duration_seconds, (int, float)) else None
    baseline_avg_seconds = float(baseline_avg_seconds) if baseline_avg_seconds is not None and not isinstance(baseline_avg_seconds, bool) and isinstance(baseline_avg_seconds, (int, float)) else None

    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO pipeline_task_audit (
                        dag_id, task_id, run_id, logical_date, start_time, end_time,
                        duration_seconds, baseline_avg_seconds, state, try_number,
                        is_anomaly, anomaly_reason, error_message, dataset_name,
                        records_read, records_written, records_rejected, mode, start_date, end_date
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (dag_id, task_id, run_id, try_number) DO UPDATE SET
                        logical_date = COALESCE(pipeline_task_audit.logical_date, EXCLUDED.logical_date),
                        start_time = COALESCE(pipeline_task_audit.start_time, EXCLUDED.start_time),
                        end_time = COALESCE(EXCLUDED.end_time, pipeline_task_audit.end_time),
                        duration_seconds = COALESCE(EXCLUDED.duration_seconds, pipeline_task_audit.duration_seconds),
                        baseline_avg_seconds = COALESCE(EXCLUDED.baseline_avg_seconds, pipeline_task_audit.baseline_avg_seconds),
                        state = EXCLUDED.state,
                        is_anomaly = CASE WHEN EXCLUDED.is_anomaly IS TRUE THEN TRUE ELSE pipeline_task_audit.is_anomaly END,
                        anomaly_reason = COALESCE(EXCLUDED.anomaly_reason, pipeline_task_audit.anomaly_reason),
                        error_message = COALESCE(EXCLUDED.error_message, pipeline_task_audit.error_message),
                        dataset_name = COALESCE(EXCLUDED.dataset_name, pipeline_task_audit.dataset_name),
                        records_read = COALESCE(EXCLUDED.records_read, pipeline_task_audit.records_read),
                        records_written = COALESCE(EXCLUDED.records_written, pipeline_task_audit.records_written),
                        records_rejected = COALESCE(EXCLUDED.records_rejected, pipeline_task_audit.records_rejected),
                        mode = COALESCE(EXCLUDED.mode, pipeline_task_audit.mode),
                        start_date = COALESCE(EXCLUDED.start_date, pipeline_task_audit.start_date),
                        end_date = COALESCE(EXCLUDED.end_date, pipeline_task_audit.end_date);
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
                        dataset_name,
                        records_read,
                        records_written,
                        records_rejected,
                        mode,
                        start_date,
                        end_date,
                    ),
                )
    except Exception as exc:
        logger.warning(f"Failed to record task audit for {task_id}: {exc}")
    finally:
        conn.close()


def record_dag_audit(
    dag_id: str,
    run_id: str,
    state: str,
    logical_date: Optional[datetime] = None,
    run_type: Optional[str] = None,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    duration_seconds: Optional[float] = None,
    error_message: Optional[str] = None,
) -> None:
    """Insert or update DAG Run lifecycle record in pipeline_dag_audit table."""
    conn = _get_db_connection()
    if not conn:
        return

    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO pipeline_dag_audit (
                        dag_id, run_id, logical_date, run_type, start_time, end_time,
                        duration_seconds, state, error_message
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (dag_id, run_id) DO UPDATE SET
                        logical_date = COALESCE(pipeline_dag_audit.logical_date, EXCLUDED.logical_date),
                        run_type = COALESCE(pipeline_dag_audit.run_type, EXCLUDED.run_type),
                        start_time = COALESCE(pipeline_dag_audit.start_time, EXCLUDED.start_time),
                        end_time = COALESCE(EXCLUDED.end_time, pipeline_dag_audit.end_time),
                        duration_seconds = COALESCE(EXCLUDED.duration_seconds, pipeline_dag_audit.duration_seconds),
                        state = EXCLUDED.state,
                        error_message = COALESCE(EXCLUDED.error_message, pipeline_dag_audit.error_message);
                    """,
                    (
                        dag_id,
                        run_id,
                        logical_date,
                        run_type,
                        start_time,
                        end_time,
                        duration_seconds,
                        state,
                        error_message,
                    ),
                )
    except Exception as exc:
        logger.warning(f"Failed to record DAG audit for {dag_id} / {run_id}: {exc}")
    finally:
        conn.close()


# =============================================================================
# AIRFLOW CALLBACK HANDLERS
# =============================================================================

def record_task_start_audit(context: dict[str, Any]) -> None:
    """Callback triggered on task execution start (on_execute_callback).
    Creates initial audit record in pipeline_task_audit with state='RUNNING'.
    """
    ti = context.get("task_instance")
    task = context.get("task")
    dag = context.get("dag")

    dag_id = dag.dag_id if dag else (ti.dag_id if ti else "unknown_dag")
    task_id = ti.task_id if ti else "unknown_task"
    run_id = context.get("run_id") or (ti.run_id if ti else "unknown_run")
    try_number = ti.try_number if ti else 1
    start_time = ti.start_date if (ti and ti.start_date) else datetime.now(timezone.utc)

    params = context.get("params") or {}
    mode = _clean_param_val(params.get("run_mode")) or "incremental"
    start_date = _clean_param_val(params.get("start_date"))
    end_date = _clean_param_val(params.get("end_date"))

    logical_dt = context.get("logical_date") or context.get("execution_date")
    if start_date is None and logical_dt is not None:
        start_date = logical_dt.date() if hasattr(logical_dt, "date") else logical_dt
    if end_date is None and logical_dt is not None:
        end_date = logical_dt.date() if hasattr(logical_dt, "date") else logical_dt

    dataset_name = TASK_DATASET_MAP.get(task_id)
    if not dataset_name and "." in task_id:
        dataset_name = TASK_DATASET_MAP.get(task_id.split(".")[-1])

    try:
        record_task_audit(
            dag_id=dag_id,
            task_id=task_id,
            run_id=run_id,
            state="RUNNING",
            logical_date=logical_dt,
            start_time=start_time,
            end_time=None,
            duration_seconds=None,
            try_number=try_number,
            dataset_name=dataset_name,
            mode=mode,
            start_date=start_date,
            end_date=end_date,
        )
    except Exception as exc:
        logger.warning(f"Failed to record task start audit for {task_id}: {exc}")


def check_audit_reconciliation(limit: int = 100) -> list[dict[str, Any]]:
    """Query v_incomplete_task_audits to detect tasks missing metrics, stuck in RUNNING, or missing error info."""
    conn = _get_db_connection()
    if not conn:
        return []
    records = []
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT audit_id, dag_id, task_id, run_id, try_number, state, audit_status, records_read, records_written
                    FROM v_incomplete_task_audits
                    WHERE audit_status IN ('MISSING_METRICS', 'STALE_RUNNING', 'FAILED_PRE_EXECUTION')
                    ORDER BY audit_id DESC
                    LIMIT %s;
                    """,
                    (limit,),
                )
                rows = cur.fetchall()
                for r in rows:
                    records.append({
                        "audit_id": r[0],
                        "dag_id": r[1],
                        "task_id": r[2],
                        "run_id": r[3],
                        "try_number": r[4],
                        "state": r[5],
                        "audit_status": r[6],
                        "records_read": r[7],
                        "records_written": r[8],
                    })
    except Exception as exc:
        logger.warning(f"Failed to query audit reconciliation view: {exc}")
    finally:
        conn.close()
    return records


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

    params = context.get("params") or {}
    mode = _clean_param_val(params.get("run_mode"))
    start_date = _clean_param_val(params.get("start_date"))
    end_date = _clean_param_val(params.get("end_date"))

    logical_dt = context.get("logical_date") or context.get("execution_date")
    if start_date is None and logical_dt is not None:
        start_date = logical_dt.date() if hasattr(logical_dt, "date") else logical_dt
    if end_date is None and logical_dt is not None:
        end_date = logical_dt.date() if hasattr(logical_dt, "date") else logical_dt

    dataset_name = TASK_DATASET_MAP.get(task_id)
    if not dataset_name and "." in task_id:
        dataset_name = TASK_DATASET_MAP.get(task_id.split(".")[-1])

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
        dataset_name=dataset_name,
        mode=mode,
        start_date=start_date,
        end_date=end_date,
    )

    alert_msg = (
        f"🚨 <b>[LAKEHOUSE ALERT] Task Failed!</b>\n"
        f"• <b>DAG:</b> <code>{dag_id}</code>\n"
        f"• <b>Task:</b> <code>{task_id}</code>\n"
        f"• <b>Lần thử:</b> {try_number}/{max_tries}\n"
        + (f"• <b>Thời gian:</b> {duration_sec:.1f}s\n" if duration_sec else "")
        + f"• <b>Lỗi:</b> <i>{error_summary}</i>\n"
        f"• <b>Log:</b> <a href=\"{log_url}\">Xem chi tiết trên Airflow</a>"
    )
    send_alert(
        alert_msg,
        level="ERROR",
        dag_id=dag_id,
        task_id=task_id,
        run_id=run_id,
        alert_type="TASK_FAILURE",
    )


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

    params = context.get("params") or {}
    mode = _clean_param_val(params.get("run_mode"))
    start_date = _clean_param_val(params.get("start_date"))
    end_date = _clean_param_val(params.get("end_date"))

    logical_dt = context.get("logical_date") or context.get("execution_date")
    if start_date is None and logical_dt is not None:
        start_date = logical_dt.date() if hasattr(logical_dt, "date") else logical_dt
    if end_date is None and logical_dt is not None:
        end_date = logical_dt.date() if hasattr(logical_dt, "date") else logical_dt

    dataset_name = TASK_DATASET_MAP.get(task_id)
    if not dataset_name and "." in task_id:
        dataset_name = TASK_DATASET_MAP.get(task_id.split(".")[-1])

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
        dataset_name=dataset_name,
        mode=mode,
        start_date=start_date,
        end_date=end_date,
    )

    alert_msg = (
        f"⚠️ <b>[LAKEHOUSE RETRY] Task đang thử lại...</b>\n"
        f"• <b>DAG:</b> <code>{dag_id}</code>\n"
        f"• <b>Task:</b> <code>{task_id}</code>\n"
        f"• <b>Lần thử:</b> {try_number}/{max_tries}\n"
        f"• <b>Lý do:</b> <i>{error_summary}</i>"
    )
    send_alert(
        alert_msg,
        level="WARNING",
        dag_id=dag_id,
        task_id=task_id,
        run_id=run_id,
        alert_type="TASK_RETRY",
    )


def check_duration_anomaly_and_audit(context: dict[str, Any]) -> None:
    """Callback triggered on successful task execution to audit, trace lineage, and detect duration anomalies."""
    ti = context.get("task_instance")
    dag = context.get("dag")

    dag_id = dag.dag_id if dag else "unknown_dag"
    task_id = ti.task_id if ti else "unknown_task"
    run_id = context.get("run_id", "unknown_run")
    duration_sec = ti.duration if ti and ti.duration else 0.0

    params = context.get("params") or {}
    mode = _clean_param_val(params.get("run_mode"))
    start_date = _clean_param_val(params.get("start_date"))
    end_date = _clean_param_val(params.get("end_date"))

    logical_dt = context.get("logical_date") or context.get("execution_date")
    if start_date is None and logical_dt is not None:
        start_date = logical_dt.date() if hasattr(logical_dt, "date") else logical_dt
    if end_date is None and logical_dt is not None:
        end_date = logical_dt.date() if hasattr(logical_dt, "date") else logical_dt

    dataset_name = TASK_DATASET_MAP.get(task_id)
    if not dataset_name and "." in task_id:
        dataset_name = TASK_DATASET_MAP.get(task_id.split(".")[-1])

    # Airflow owns the final task state. Spark metrics only signal NO_DATA and
    # identify which registered lineage targets actually received rows.
    nsmo_tasks = {
        "bronze_ingestion.ingest_nsmo_power",
        "silver_processing.silver_power_hourly",
        "gold_processing.gold_generation_power_hourly",
        "silver_processing.silver_load",
        "gold_processing.gold_load_hourly",
    }
    app_state = None
    records_written = records_rejected = None
    if task_id in nsmo_tasks:
        conn = _get_db_connection()
        if conn:
            try:
                with conn:
                    with conn.cursor() as cur:
                        cur.execute("SELECT state, records_written, records_rejected FROM pipeline_task_audit WHERE dag_id=%s AND task_id=%s "
                                    "AND run_id=%s AND try_number=%s",
                                    (dag_id, task_id, run_id, ti.try_number if ti else 1))
                        row = cur.fetchone()
                        if row:
                            app_state, records_written, records_rejected = row
            finally:
                conn.close()

    # Check for duration anomaly
    is_anomaly, reason, baseline_mean = check_duration_anomaly(task_id, dag_id, duration_sec)

    record_task_audit(
        dag_id=dag_id,
        task_id=task_id,
        run_id=run_id,
        state="NO_DATA" if app_state == "NO_DATA" else "SUCCESS",
        logical_date=context.get("logical_date") or context.get("execution_date"),
        start_time=ti.start_date if ti else None,
        end_time=ti.end_date if ti else None,
        duration_seconds=duration_sec,
        baseline_avg_seconds=baseline_mean,
        try_number=ti.try_number if ti else 1,
        is_anomaly=is_anomaly,
        anomaly_reason=reason,
        dataset_name=dataset_name,
        mode=mode,
        start_date=start_date,
        end_date=end_date,
    )

    # Record data lineage for table-level transformation
    if task_id in nsmo_tasks:
        if records_written is not None or records_rejected is not None:
            record_task_lineage(dag_id=dag_id, task_id=task_id, run_id=run_id,
                                records_written=records_written,
                                records_rejected=records_rejected)
    else:
        record_task_lineage(dag_id=dag_id, task_id=task_id, run_id=run_id)

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
        send_alert(
            alert_msg,
            level="WARNING",
            dag_id=dag_id,
            task_id=task_id,
            run_id=run_id,
            alert_type="DURATION_ANOMALY",
        )


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
    send_alert(
        alert_msg,
        level="CRITICAL",
        dag_id=dag_id,
        alert_type="SLA_MISSED",
    )


def alert_dag_success(context: dict[str, Any]) -> None:
    """DAG-level callback triggered when DAG Run completes successfully."""
    dag = context.get("dag")
    dag_run = context.get("dag_run")
    dag_id = dag.dag_id if dag else "unknown_dag"
    run_id = dag_run.run_id if dag_run else context.get("run_id", "unknown_run")
    start_time = dag_run.start_date if dag_run else None
    end_time = dag_run.end_date or datetime.now(timezone.utc)
    duration = (end_time - start_time).total_seconds() if (start_time and end_time) else None

    record_dag_audit(
        dag_id=dag_id,
        run_id=run_id,
        state="SUCCESS",
        logical_date=dag_run.logical_date if dag_run else context.get("logical_date"),
        run_type=dag_run.run_type if dag_run else None,
        start_time=start_time,
        end_time=end_time,
        duration_seconds=duration,
        error_message=None,
    )


def alert_dag_failure(context: dict[str, Any]) -> None:
    """DAG-level callback triggered when DAG Run fails."""
    dag = context.get("dag")
    dag_run = context.get("dag_run")
    dag_id = dag.dag_id if dag else "unknown_dag"
    run_id = dag_run.run_id if dag_run else context.get("run_id", "unknown_run")
    start_time = dag_run.start_date if dag_run else None
    end_time = dag_run.end_date or datetime.now(timezone.utc)
    duration = (end_time - start_time).total_seconds() if (start_time and end_time) else None

    # Safely extract names of failed task instances
    failed_tasks: list[str] = []
    if dag_run:
        try:
            tis = dag_run.get_task_instances(state="failed")
            failed_tasks = [t.task_id for t in tis]
        except Exception:
            pass

    reason = context.get("reason")
    error_summary = (
        f"Failed tasks: {', '.join(failed_tasks)}"
        if failed_tasks
        else str(reason or "DAG Run marked as failed")
    )

    record_dag_audit(
        dag_id=dag_id,
        run_id=run_id,
        state="FAILED",
        logical_date=dag_run.logical_date if dag_run else context.get("logical_date"),
        run_type=dag_run.run_type if dag_run else None,
        start_time=start_time,
        end_time=end_time,
        duration_seconds=duration,
        error_message=error_summary[:500],
    )

    alert_msg = (
        f"🚨 <b>[LAKEHOUSE DAG FAILED] Pipeline thất bại!</b>\n"
        f"• <b>DAG:</b> <code>{dag_id}</code>\n"
        f"• <b>Run ID:</b> <code>{run_id}</code>\n"
        + (f"• <b>Thời gian:</b> {duration:.1f}s\n" if duration else "")
        + f"• <b>Lỗi:</b> <i>{error_summary}</i>"
    )
    send_alert(
        alert_msg,
        level="ERROR",
        dag_id=dag_id,
        run_id=run_id,
        alert_type="DAG_FAILURE",
    )
