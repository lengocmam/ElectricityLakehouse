"""Lakehouse Disaster Recovery, Database Backup & Iceberg Maintenance DAG.

Schedule: Daily at 04:00 AM (after the Medallion Pipeline finishes).
Tasks:
1. backup_control_db: Dumps PostgreSQL metadata (watermark, audit) and uploads compressed backup to MinIO S3.
2. iceberg_maintenance: Consolidates small files (rewrite_data_files), expires snapshots older than 7 days, and cleans orphan files.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

try:
    from monitoring import (
        alert_task_failure,
        alert_task_retry,
        check_duration_anomaly_and_audit,
    )
except ImportError:
    import sys, os
    plugin_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "plugins"))
    if plugin_dir not in sys.path:
        sys.path.insert(0, plugin_dir)
    from monitoring import (
        alert_task_failure,
        alert_task_retry,
        check_duration_anomaly_and_audit,
    )

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": alert_task_failure,
    "on_retry_callback": alert_task_retry,
    "on_success_callback": check_duration_anomaly_and_audit,
}

SPARK_APP_DIR = "/opt/lakehouse/scripts/spark"

SPARK_CONF = {
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.defaultCatalog": "nessie",
    "spark.sql.catalog.nessie": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.nessie.type": "rest",
    "spark.sql.catalog.nessie.uri": "http://nessie:19120/iceberg/main/",
    "spark.sql.catalog.nessie.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    "spark.sql.catalog.nessie.s3.endpoint": "http://minio:9000",
    "spark.sql.catalog.nessie.s3.path-style-access": "true",
    "spark.sql.catalog.nessie.s3.access-key-id": "minioadmin",
    "spark.sql.catalog.nessie.s3.secret-access-key": "minioadmin",
    "spark.executorEnv.AWS_ACCESS_KEY_ID": "minioadmin",
    "spark.executorEnv.AWS_SECRET_ACCESS_KEY": "minioadmin",
    "spark.executorEnv.AWS_REGION": "us-east-1",
    "spark.sql.session.timeZone": "Asia/Ho_Chi_Minh",
}

ICEBERG_PACKAGES = (
    "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0,"
    "org.apache.iceberg:iceberg-aws-bundle:1.11.0"
)

SPARK_RESOURCES = {
    "deploy_mode": "client",
    "driver_memory": "1g",
    "executor_memory": "1g",
    "num_executors": 1,
    "executor_cores": 2,
}

SPARK_ENV = {
    "PYTHONPATH": SPARK_APP_DIR,
    "MINIO_ENDPOINT": "http://minio:9000",
    "AWS_ACCESS_KEY_ID": "minioadmin",
    "AWS_SECRET_ACCESS_KEY": "minioadmin",
}

with DAG(
    dag_id="lakehouse_maintenance_and_backup",
    description="Daily Control DB Backup & Iceberg Lakehouse Maintenance",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 9, 22),
    schedule="0 4 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "maintenance", "backup", "disaster-recovery"],
) as dag:

    backup_control_db = BashOperator(
        task_id="backup_control_db_to_minio",
        bash_command=(
            "python /opt/lakehouse/scripts/spark/maintenance/backup_control_db.py "
            "--bucket warehouse --prefix backups/postgres/"
        ),
        env={
            "WATERMARK_DB_HOST": "postgres",
            "WATERMARK_DB_PORT": "5432",
            "WATERMARK_DB_NAME": "lakehouse_control",
            "WATERMARK_DB_USER": "lakehouse_control",
            "WATERMARK_DB_PASSWORD": "lakehouse_control",
            "MINIO_ENDPOINT": "http://minio:9000",
            "AWS_ACCESS_KEY_ID": "minioadmin",
            "AWS_SECRET_ACCESS_KEY": "minioadmin",
            "AWS_REGION": "us-east-1",
            "BACKUP_BUCKET": "warehouse",
        },
        execution_timeout=timedelta(minutes=15),
    )

    iceberg_maintenance = SparkSubmitOperator(
        task_id="iceberg_table_maintenance",
        application=f"{SPARK_APP_DIR}/maintenance/table_maintenance.py",
        application_args=["--layer", "all", "--retention-days", "7"],
        conn_id="spark_default",
        name="lakehouse_iceberg_maintenance",
        conf=SPARK_CONF,
        packages=ICEBERG_PACKAGES,
        env_vars=SPARK_ENV,
        **SPARK_RESOURCES,
        execution_timeout=timedelta(hours=2),
        verbose=True,
    )

    backup_control_db >> iceberg_maintenance
