"""Lakehouse Machine Learning Data Export & Feature Extraction DAG.

Schedule: None (Manual on-demand trigger or weekly run)
Tasks:
1. export_ml_load_forecasting: National & regional electricity load with weather lags & rolling metrics.
2. export_ml_hydro_inflow: Reservoir water levels, inflows, discharge, rainfall lags.
3. export_ml_generation_mix: Daily energy source outputs and renewable mix.
4. export_raw_gold_tables: Complete dump of all Gold Facts and Dims into Parquet & CSV.
"""
from datetime import datetime, timedelta

from airflow import DAG
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
    "retry_delay": timedelta(minutes=3),
    "on_failure_callback": alert_task_failure,
    "on_retry_callback": alert_task_retry,
    "on_success_callback": check_duration_anomaly_and_audit,
}

SPARK_APP_DIR = "/opt/lakehouse/scripts/spark"
OUTPUT_DIR = "/opt/lakehouse/data_exports"

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
    dag_id="lakehouse_ml_data_export",
    description="Export Lakehouse Gold data into ML-ready tabular formats (CSV, Parquet)",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 9, 22),
    schedule=None,  # On-demand manual trigger
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "export", "machine-learning", "ai-training"],
) as dag:

    export_ml_load = SparkSubmitOperator(
        task_id="export_ml_load_forecasting",
        application=f"{SPARK_APP_DIR}/export/export_ml_data.py",
        application_args=["--dataset", "ml_load", "--format", "both", "--output-dir", OUTPUT_DIR],
        conn_id="spark_default",
        name="export_ml_load_forecasting",
        conf=SPARK_CONF,
        packages=ICEBERG_PACKAGES,
        env_vars=SPARK_ENV,
        **SPARK_RESOURCES,
        execution_timeout=timedelta(minutes=30),
        verbose=True,
    )

    export_ml_hydro = SparkSubmitOperator(
        task_id="export_ml_hydro_inflow",
        application=f"{SPARK_APP_DIR}/export/export_ml_data.py",
        application_args=["--dataset", "ml_hydro", "--format", "both", "--output-dir", OUTPUT_DIR],
        conn_id="spark_default",
        name="export_ml_hydro_inflow",
        conf=SPARK_CONF,
        packages=ICEBERG_PACKAGES,
        env_vars=SPARK_ENV,
        **SPARK_RESOURCES,
        execution_timeout=timedelta(minutes=30),
        verbose=True,
    )

    export_ml_generation = SparkSubmitOperator(
        task_id="export_ml_generation_mix",
        application=f"{SPARK_APP_DIR}/export/export_ml_data.py",
        application_args=["--dataset", "ml_generation", "--format", "both", "--output-dir", OUTPUT_DIR],
        conn_id="spark_default",
        name="export_ml_generation_mix",
        conf=SPARK_CONF,
        packages=ICEBERG_PACKAGES,
        env_vars=SPARK_ENV,
        **SPARK_RESOURCES,
        execution_timeout=timedelta(minutes=30),
        verbose=True,
    )

    export_raw_gold = SparkSubmitOperator(
        task_id="export_raw_gold_tables",
        application=f"{SPARK_APP_DIR}/export/export_ml_data.py",
        application_args=["--dataset", "gold_tables", "--format", "both", "--output-dir", OUTPUT_DIR],
        conn_id="spark_default",
        name="export_raw_gold_tables",
        conf=SPARK_CONF,
        packages=ICEBERG_PACKAGES,
        env_vars=SPARK_ENV,
        **SPARK_RESOURCES,
        execution_timeout=timedelta(minutes=45),
        verbose=True,
    )

    # Run ML feature datasets concurrently, then raw gold tables
    [export_ml_load, export_ml_hydro, export_ml_generation] >> export_raw_gold
