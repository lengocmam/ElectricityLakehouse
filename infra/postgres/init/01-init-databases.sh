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

CREATE TABLE IF NOT EXISTS bronze_ingestion_log (
    run_id VARCHAR(50) PRIMARY KEY,
    dataset_name VARCHAR(50) NOT NULL,
    run_mode VARCHAR(20) NOT NULL,
    start_date DATE,
    end_date DATE,
    records_count BIGINT DEFAULT 0,
    status VARCHAR(20) NOT NULL,
    error_message TEXT,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    duration_seconds DOUBLE PRECISION
);

GRANT SELECT, INSERT, UPDATE, DELETE
ON TABLE pipeline_watermark
TO lakehouse_control;

GRANT SELECT, INSERT, UPDATE, DELETE
ON TABLE bronze_ingestion_log
TO lakehouse_control;


EOSQL