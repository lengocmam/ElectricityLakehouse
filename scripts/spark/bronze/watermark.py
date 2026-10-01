import os
from datetime import date
from typing import Optional

import psycopg


def _get_connection():
    return psycopg.connect(
        host=os.environ["WATERMARK_DB_HOST"],
        port=os.environ.get("WATERMARK_DB_PORT", "5432"),
        dbname=os.environ["WATERMARK_DB_NAME"],
        user=os.environ["WATERMARK_DB_USER"],
        password=os.environ["WATERMARK_DB_PASSWORD"],
    )


def get_watermark(dataset_name: str) -> Optional[date]:
    """Get the last successfully processed data date."""
    with _get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT last_successful_data_date
                FROM pipeline_watermark
                WHERE dataset_name = %s
                """,
                (dataset_name,),
            )

            row = cur.fetchone()

    return row[0] if row else None


def update_watermark(dataset_name: str, data_date: date) -> None:
    """Update the watermark after a successful pipeline run."""
    with _get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline_watermark (
                    dataset_name,
                    last_successful_data_date,
                    last_successful_run_at
                )
                VALUES (%s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (dataset_name)
                DO UPDATE SET
                    last_successful_data_date = EXCLUDED.last_successful_data_date,
                    last_successful_run_at = CURRENT_TIMESTAMP
                """,
                (dataset_name, data_date),
            )
