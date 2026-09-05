#!/bin/sh
set -eu

create_user_database() {
  database="$1"
  user="$2"
  password="$3"

  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
DO \$\$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '$user') THEN
    EXECUTE format('CREATE ROLE %I LOGIN PASSWORD %L', '$user', '$password');
  END IF;
END
\$\$;
SELECT 'CREATE DATABASE "$database" OWNER "$user"'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '$database')\gexec
EOSQL

  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$database" <<-EOSQL
GRANT ALL ON SCHEMA public TO "$user";
EOSQL
}

create_user_database airflow "${AIRFLOW_DB_USER:-airflow}" "${AIRFLOW_DB_PASSWORD:-airflow}"
create_user_database superset "${SUPERSET_DB_USER:-superset}" "${SUPERSET_DB_PASSWORD:-superset}"
create_user_database nessie "${NESSIE_DB_USER:-nessie}" "${NESSIE_DB_PASSWORD:-nessie}"
