#!/bin/sh
set -eu

if [ -n "${POSTGRES_HOST:-}" ]; then
  until pg_isready --host "$POSTGRES_HOST" --username "$POSTGRES_USER" >/dev/null 2>&1; do
    sleep 1
  done
fi

psql_cmd() {
  if [ -n "${POSTGRES_HOST:-}" ]; then
    psql -v ON_ERROR_STOP=1 --host "$POSTGRES_HOST" --username "$POSTGRES_USER" "$@"
  else
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" "$@"
  fi
}

create_user_database() {
  database="$1"
  user="$2"
  password="$3"

  psql_cmd <<-EOSQL
DO \$\$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '$user') THEN
    EXECUTE format('CREATE ROLE %I LOGIN PASSWORD %L', '$user', '$password');
  END IF;
  EXECUTE format('ALTER ROLE %I LOGIN PASSWORD %L', '$user', '$password');
END
\$\$;
SELECT 'CREATE DATABASE "$database" OWNER "$user"'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '$database')\gexec
EOSQL

  psql_cmd --dbname "$database" <<-EOSQL
GRANT ALL ON SCHEMA public TO "$user";
EOSQL
}

create_user_database airflow "${AIRFLOW_DB_USER:-airflow}" "${AIRFLOW_DB_PASSWORD:-airflow}"
create_user_database metabase "${METABASE_DB_USER:-metabase}" "${METABASE_DB_PASSWORD:-metabase}"
create_user_database nessie "${NESSIE_DB_USER:-nessie}" "${NESSIE_DB_PASSWORD:-nessie}"
create_user_database lakehouse_control "${LAKEHOUSE_CONTROL_DB_USER:-lakehouse_control}" "${LAKEHOUSE_CONTROL_DB_PASSWORD:-lakehouse_control}"

psql_cmd --dbname "lakehouse_control" <<-EOSQL

CREATE TABLE IF NOT EXISTS pipeline_watermark (
    dataset_name VARCHAR(100) PRIMARY KEY,
    last_successful_data_date DATE,
    last_successful_run_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

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

ALTER TABLE pipeline_watermark OWNER TO lakehouse_control;
ALTER TABLE pipeline_task_audit OWNER TO lakehouse_control;
ALTER TABLE pipeline_dag_audit OWNER TO lakehouse_control;
ALTER TABLE alert_event OWNER TO lakehouse_control;
ALTER TABLE data_lineage OWNER TO lakehouse_control;

GRANT SELECT, INSERT, UPDATE, DELETE
ON TABLE pipeline_watermark
TO lakehouse_control;

GRANT SELECT, INSERT, UPDATE, DELETE
ON TABLE pipeline_task_audit
TO lakehouse_control;

GRANT SELECT, INSERT, UPDATE, DELETE
ON TABLE pipeline_dag_audit
TO lakehouse_control;

GRANT SELECT, INSERT, UPDATE, DELETE
ON TABLE alert_event
TO lakehouse_control;

GRANT SELECT, INSERT, UPDATE, DELETE
ON TABLE data_lineage
TO lakehouse_control;

GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public
TO lakehouse_control;



-- 5. Reconciliation view for incomplete or abnormal audits
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
        WHEN task_id IN ('start', 'end') OR task_id LIKE '%maintenance%' OR task_id LIKE '%backup%' 
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

ALTER VIEW v_incomplete_task_audits OWNER TO lakehouse_control;
GRANT SELECT ON v_incomplete_task_audits TO lakehouse_control;

EOSQL