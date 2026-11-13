"""Shared PostgreSQL connection for audit records and pipeline progress."""

import os
import time

import psycopg


def get_connection(max_retries: int = 3, retry_delay: float = 1.0):
    """Connect to the control database with bounded retry."""
    host = os.environ.get("WATERMARK_DB_HOST", "postgres")
    port = os.environ.get("WATERMARK_DB_PORT", "5432")
    dbname = os.environ.get("WATERMARK_DB_NAME", "lakehouse_control")
    user = os.environ.get("WATERMARK_DB_USER", "lakehouse_control")
    password = os.environ.get("WATERMARK_DB_PASSWORD", "lakehouse_control")

    last_exc = None
    for attempt in range(1, max_retries + 1):
        try:
            return psycopg.connect(
                host=host,
                port=port,
                dbname=dbname,
                user=user,
                password=password,
                connect_timeout=3,
            )
        except Exception as exc:
            last_exc = exc
            if attempt < max_retries:
                time.sleep(retry_delay * attempt)
    raise last_exc
