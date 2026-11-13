from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.models.param import Param

try:
    from monitoring import (
        record_task_start_audit,
        alert_task_failure,
        alert_task_retry,
        check_duration_anomaly_and_audit,
        alert_sla_miss,
        alert_dag_success,
        alert_dag_failure,
        init_audit_table,
    )
except ImportError:
    import sys, os
    plugin_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "plugins"))
    if plugin_dir not in sys.path:
        sys.path.insert(0, plugin_dir)
    from monitoring import (
        record_task_start_audit,
        alert_task_failure,
        alert_task_retry,
        check_duration_anomaly_and_audit,
        alert_sla_miss,
        alert_dag_success,
        alert_dag_failure,
        init_audit_table,
    )

# Ensure audit table exists in PostgreSQL
init_audit_table()

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_execute_callback": record_task_start_audit,
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
    "spark.executorEnv.PYTHONPATH": SPARK_APP_DIR,
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
    "AIRFLOW_DAG_ID": "{{ dag.dag_id }}",
    "AIRFLOW_TASK_ID": "{{ ti.task_id }}",
    "AIRFLOW_RUN_ID": "{{ run_id }}",
    "AIRFLOW_TRY_NUMBER": "{{ ti.try_number }}",
    "WATERMARK_DB_HOST": "postgres",
    "WATERMARK_DB_PORT": "5432",
    "WATERMARK_DB_NAME": "lakehouse_control",
    "WATERMARK_DB_USER": "lakehouse_control",
    "WATERMARK_DB_PASSWORD": "lakehouse_control",
}


with DAG(
    dag_id="electricity_lakehouse_pipeline",
    description="Vietnam Electricity Lakehouse Pipeline",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 9, 22),
    schedule="0 2 * * *",
    catchup=False,
    params={
        "run_mode": Param(
            "incremental",
            type="string",
            enum=["incremental", "backfill"],
        ),
        "start_date": Param(
            None,
            type=["null", "string"],
            description="Only required for backfill.",
        ),
        "end_date": Param(
            None,
            type=["null", "string"],
            description="Only required for backfill.",
        ),
    },
    max_active_runs=1,
    max_active_tasks=2,
    dagrun_timeout=timedelta(hours=2),
    sla_miss_callback=alert_sla_miss,
    on_success_callback=alert_dag_success,
    on_failure_callback=alert_dag_failure,
    tags=["lakehouse", "electricity"],
) as dag:

    start = EmptyOperator(task_id="start")

    with TaskGroup(group_id="bronze_ingestion"):

        bronze_evn = SparkSubmitOperator(
            task_id="ingest_evn",
            application=f"{SPARK_APP_DIR}/bronze/ingest_bronze.py",
            application_args=[
                "evn",
                "--run-mode",
                "{{ params.run_mode }}",
                "--start-date",
                "{{ params.start_date }}",
                "--end-date",
                "{{ params.end_date }}",
            ],
            conn_id="spark_default",
            name="bronze_evn",
            conf=SPARK_CONF,
            packages=ICEBERG_PACKAGES,
            env_vars=SPARK_ENV,
            **SPARK_RESOURCES,
            execution_timeout=timedelta(hours=2),
            verbose=True,
        )

        bronze_evn_hydro = SparkSubmitOperator(
            task_id="ingest_evn_hydro",
            application=f"{SPARK_APP_DIR}/bronze/ingest_bronze.py",
            application_args=[
                "evn_hydro",
                "--run-mode",
                "{{ params.run_mode }}",
                "--start-date",
                "{{ params.start_date }}",
                "--end-date",
                "{{ params.end_date }}",
            ],
            conn_id="spark_default",
            name="bronze_evn_hydro",
            conf=SPARK_CONF,
            packages=ICEBERG_PACKAGES,
            env_vars=SPARK_ENV,
            **SPARK_RESOURCES,
            execution_timeout=timedelta(minutes=45),
            verbose=True,
        )
        
        bronze_nsmo_load = SparkSubmitOperator(
            task_id="ingest_nsmo_load",
            application=f"{SPARK_APP_DIR}/bronze/ingest_bronze.py",
            application_args=[
                "nsmo_load",
                "--run-mode",
                "{{ params.run_mode }}",
                "--start-date",
                "{{ params.start_date }}",
                "--end-date",
                "{{ params.end_date }}",
            ],
            conn_id="spark_default",
            name="bronze_nsmo_load",
            conf=SPARK_CONF,
            packages=ICEBERG_PACKAGES,
            env_vars=SPARK_ENV,
            **SPARK_RESOURCES,
            execution_timeout=timedelta(minutes=45),
            verbose=True,
        )

        bronze_nsmo_power = SparkSubmitOperator(
            task_id="ingest_nsmo_power",
            retries=0,
            application=f"{SPARK_APP_DIR}/bronze/ingest_bronze.py",
            application_args=[
                "nsmo_power", "--run-mode", "{{ params.run_mode }}",
                "--start-date", "{{ params.start_date }}",
                "--end-date", "{{ params.end_date }}",
            ],
            conn_id="spark_default", name="bronze_nsmo_power",
            conf=SPARK_CONF, packages=ICEBERG_PACKAGES, env_vars=SPARK_ENV,
            **SPARK_RESOURCES, execution_timeout=timedelta(minutes=45), verbose=True,
        )
        
        bronze_open_meteo = SparkSubmitOperator(
            task_id="ingest_open_meteo",
            application=f"{SPARK_APP_DIR}/bronze/ingest_bronze.py",
            application_args=[
                "open_meteo",
                "--run-mode",
                "{{ params.run_mode }}",
                "--start-date",
                "{{ params.start_date }}",
                "--end-date",
                "{{ params.end_date }}",
            ],
            conn_id="spark_default",
            name="bronze_open_meteo",
            conf=SPARK_CONF,
            packages=ICEBERG_PACKAGES,
            env_vars=SPARK_ENV,
            **SPARK_RESOURCES,
            execution_timeout=timedelta(hours=2),
            verbose=True,
        )

    with TaskGroup(group_id="silver_processing"):

        silver_power_daily = SparkSubmitOperator(
            task_id="silver_power_daily",
            application=f"{SPARK_APP_DIR}/silver/write_silver.py",
            application_args=["--dataset", "power_daily"],
            conn_id="spark_default",
            name="silver_power_daily",
            conf=SPARK_CONF,
            packages=ICEBERG_PACKAGES,
            env_vars=SPARK_ENV,
            **SPARK_RESOURCES,
            execution_timeout=timedelta(hours=1),
            verbose=True,
        )

        silver_generation = SparkSubmitOperator(
            task_id="silver_generation",
            application=f"{SPARK_APP_DIR}/silver/write_silver.py",
            application_args=["--dataset", "generation"],
            conn_id="spark_default",
            name="silver_generation",
            conf=SPARK_CONF,
            packages=ICEBERG_PACKAGES,
            env_vars=SPARK_ENV,
            **SPARK_RESOURCES,
            execution_timeout=timedelta(hours=1),
            verbose=True,
        )

        silver_hydro = SparkSubmitOperator(
            task_id="silver_hydro",
            application=f"{SPARK_APP_DIR}/silver/write_silver.py",
            application_args=["--dataset", "hydro"],
            conn_id="spark_default",
            name="silver_hydro",
            conf=SPARK_CONF,
            packages=ICEBERG_PACKAGES,
            env_vars=SPARK_ENV,
            **SPARK_RESOURCES,
            execution_timeout=timedelta(minutes=45),
            verbose=True,
        )

        silver_load = SparkSubmitOperator(
            task_id="silver_load",
            application=f"{SPARK_APP_DIR}/silver/write_silver.py",
            application_args=["--dataset", "load"],
            conn_id="spark_default",
            name="silver_load",
            conf=SPARK_CONF,
            packages=ICEBERG_PACKAGES,
            env_vars=SPARK_ENV,
            **SPARK_RESOURCES,
            execution_timeout=timedelta(minutes=45),
            verbose=True,
        )

        silver_power_hourly = SparkSubmitOperator(
            task_id="silver_power_hourly",
            application=f"{SPARK_APP_DIR}/silver/write_silver.py",
            application_args=["--dataset", "power_hourly"],
            conn_id="spark_default", name="silver_power_hourly",
            conf=SPARK_CONF, packages=ICEBERG_PACKAGES, env_vars=SPARK_ENV,
            **SPARK_RESOURCES, execution_timeout=timedelta(hours=1), verbose=True,
        )

        silver_weather_daily = SparkSubmitOperator(
            task_id="silver_weather_daily",
            application=f"{SPARK_APP_DIR}/silver/write_silver.py",
            application_args=["--dataset", "weather_daily"],
            conn_id="spark_default",
            name="silver_weather_daily",
            conf=SPARK_CONF,
            packages=ICEBERG_PACKAGES,
            env_vars=SPARK_ENV,
            **SPARK_RESOURCES,
            execution_timeout=timedelta(hours=1),
            verbose=True,
        )

        silver_weather_hourly = SparkSubmitOperator(
            task_id="silver_weather_hourly",
            application=f"{SPARK_APP_DIR}/silver/write_silver.py",
            application_args=["--dataset", "weather_hourly"],
            conn_id="spark_default",
            name="silver_weather_hourly",
            conf=SPARK_CONF,
            packages=ICEBERG_PACKAGES,
            env_vars=SPARK_ENV,
            **SPARK_RESOURCES,
            execution_timeout=timedelta(hours=1),
            verbose=True,
        )

    with TaskGroup(group_id="gold_processing"):

        gold_dimensions = SparkSubmitOperator(
            task_id="gold_dimensions",
            application=f"{SPARK_APP_DIR}/gold/write_gold.py",
            application_args=["--dataset", "dims"],
            conn_id="spark_default",
            name="gold_dimensions",
            conf=SPARK_CONF,
            packages=ICEBERG_PACKAGES,
            env_vars=SPARK_ENV,
            **SPARK_RESOURCES,
            execution_timeout=timedelta(minutes=45),
            verbose=True,
        )

        gold_load_hourly = SparkSubmitOperator(
            task_id="gold_load_hourly",
            application=f"{SPARK_APP_DIR}/gold/write_gold.py",
            application_args=[
                "--dataset", "fact_load_hourly",
                "--start-date", "{{ params.start_date }}",
                "--end-date", "{{ params.end_date }}",
            ],
            conn_id="spark_default",
            name="gold_fact_load_hourly",
            conf=SPARK_CONF,
            packages=ICEBERG_PACKAGES,
            env_vars=SPARK_ENV,
            **SPARK_RESOURCES,
            execution_timeout=timedelta(minutes=45),
            verbose=True,
        )

        gold_generation_power_hourly = SparkSubmitOperator(
            task_id="gold_generation_power_hourly",
            application=f"{SPARK_APP_DIR}/gold/write_gold.py",
            application_args=["--dataset", "fact_generation_power_hourly",
                              "--run-mode", "{{ params.run_mode }}",
                              "--start-date", "{{ params.start_date }}",
                              "--end-date", "{{ params.end_date }}"],
            conn_id="spark_default", name="gold_fact_generation_power_hourly",
            conf=SPARK_CONF, packages=ICEBERG_PACKAGES, env_vars=SPARK_ENV,
            **SPARK_RESOURCES, execution_timeout=timedelta(hours=1), verbose=True,
        )

        gold_weather_hourly = SparkSubmitOperator(
            task_id="gold_weather_hourly",
            application=f"{SPARK_APP_DIR}/gold/write_gold.py",
            application_args=[
                "--dataset", "fact_weather_hourly",
                "--start-date", "{{ params.start_date }}",
                "--end-date", "{{ params.end_date }}",
            ],
            conn_id="spark_default",
            name="gold_fact_weather_hourly",
            conf=SPARK_CONF,
            packages=ICEBERG_PACKAGES,
            env_vars=SPARK_ENV,
            **SPARK_RESOURCES,
            execution_timeout=timedelta(hours=1),
            verbose=True,
        )

        gold_weather_daily = SparkSubmitOperator(
            task_id="gold_weather_daily",
            application=f"{SPARK_APP_DIR}/gold/write_gold.py",
            application_args=[
                "--dataset", "fact_weather_daily",
                "--start-date", "{{ params.start_date }}",
                "--end-date", "{{ params.end_date }}",
            ],
            conn_id="spark_default",
            name="gold_fact_weather_daily",
            conf=SPARK_CONF,
            packages=ICEBERG_PACKAGES,
            env_vars=SPARK_ENV,
            **SPARK_RESOURCES,
            execution_timeout=timedelta(hours=1),
            verbose=True,
        )

        gold_hydro_status = SparkSubmitOperator(
            task_id="gold_hydro_status",
            application=f"{SPARK_APP_DIR}/gold/write_gold.py",
            application_args=[
                "--dataset", "fact_hydro_status",
                "--start-date", "{{ params.start_date }}",
                "--end-date", "{{ params.end_date }}",
            ],
            conn_id="spark_default",
            name="gold_fact_hydro_status",
            conf=SPARK_CONF,
            packages=ICEBERG_PACKAGES,
            env_vars=SPARK_ENV,
            **SPARK_RESOURCES,
            execution_timeout=timedelta(minutes=45),
            verbose=True,
        )

        gold_generation_daily = SparkSubmitOperator(
            task_id="gold_generation_daily",
            application=f"{SPARK_APP_DIR}/gold/write_gold.py",
            application_args=[
                "--dataset", "fact_generation_daily",
                "--start-date", "{{ params.start_date }}",
                "--end-date", "{{ params.end_date }}",
            ],
            conn_id="spark_default",
            name="gold_fact_generation_daily",
            conf=SPARK_CONF,
            packages=ICEBERG_PACKAGES,
            env_vars=SPARK_ENV,
            **SPARK_RESOURCES,
            execution_timeout=timedelta(hours=1),
            verbose=True,
        )

        gold_generation_power_daily = SparkSubmitOperator(
            task_id="gold_generation_power_daily",
            application=f"{SPARK_APP_DIR}/gold/write_gold.py",
            application_args=[
                "--dataset", "fact_generation_power_daily",
                "--start-date", "{{ params.start_date }}",
                "--end-date", "{{ params.end_date }}",
            ],
            conn_id="spark_default",
            name="gold_fact_generation_power_daily",
            conf=SPARK_CONF,
            packages=ICEBERG_PACKAGES,
            env_vars=SPARK_ENV,
            **SPARK_RESOURCES,
            execution_timeout=timedelta(hours=1),
            verbose=True,
        )


    end = EmptyOperator(task_id="end")

    start >> [bronze_evn, bronze_evn_hydro, bronze_nsmo_load, bronze_nsmo_power, bronze_open_meteo]

    bronze_evn >> [silver_power_daily, silver_generation]
    bronze_evn_hydro >> silver_hydro
    bronze_nsmo_load >> silver_load
    bronze_nsmo_power >> silver_power_hourly
    bronze_open_meteo >> [silver_weather_daily, silver_weather_hourly]

    [silver_hydro, silver_generation, silver_power_daily] >> gold_dimensions

    [silver_load, gold_dimensions] >> gold_load_hourly
    [silver_power_hourly, gold_dimensions] >> gold_generation_power_hourly
    [silver_weather_hourly, gold_dimensions] >> gold_weather_hourly
    [silver_weather_daily, gold_dimensions] >> gold_weather_daily
    [silver_hydro, gold_dimensions] >> gold_hydro_status
    [silver_generation, gold_dimensions] >> gold_generation_daily
    [silver_power_daily, gold_dimensions] >> gold_generation_power_daily

    [
        gold_load_hourly,
        gold_generation_power_hourly,
        gold_weather_hourly,
        gold_weather_daily,
        gold_hydro_status,
        gold_generation_daily,
        gold_generation_power_daily,
    ] >> end
