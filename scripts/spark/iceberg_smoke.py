from pyspark.sql import SparkSession


def main() -> None:
    spark = SparkSession.builder.appName("lakehouse-iceberg-smoke").getOrCreate()

    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.demo")
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS nessie.demo.airflow_events (
            id BIGINT,
            event_name STRING,
            created_at TIMESTAMP
        )
        USING iceberg
        """
    )
    spark.sql(
        """
        INSERT INTO nessie.demo.airflow_events
        VALUES (1, 'airflow_spark_iceberg_smoke', current_timestamp())
        """
    )
    spark.sql("SELECT * FROM nessie.demo.airflow_events").show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
