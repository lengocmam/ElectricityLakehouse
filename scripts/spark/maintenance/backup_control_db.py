"""Control Database Disaster Recovery & Automated Backup to MinIO S3.

Features:
1. Dumps critical operational metadata from PostgreSQL (lakehouse_control):
   - pipeline_watermark (Single Source of Truth)
   - bronze_ingestion_log (Run history)
   - pipeline_task_audit (Monitoring metrics)
2. Compresses dump into .sql.gz format.
3. Uploads directly to MinIO S3 bucket: s3://warehouse/backups/postgres/
4. Retention policy: Automatically purges backup archives older than 30 days.
5. Disaster Recovery Restore: Supports --restore <s3_key> to restore database in seconds.
"""
import argparse
import gzip
import io
import os
import sys
from datetime import datetime, timedelta, timezone

try:
    import psycopg
except ImportError:
    try:
        import psycopg2 as psycopg
    except ImportError:
        psycopg = None

try:
    import boto3
    from botocore.client import Config
except ImportError:
    boto3 = None


def get_db_connection():
    """Connect to PostgreSQL lakehouse_control database."""
    if not psycopg:
        raise RuntimeError("Neither psycopg nor psycopg2 is installed.")

    host = os.environ.get("WATERMARK_DB_HOST", "postgres")
    port = os.environ.get("WATERMARK_DB_PORT", "5432")
    dbname = os.environ.get("WATERMARK_DB_NAME", "lakehouse_control")
    user = os.environ.get("WATERMARK_DB_USER", "postgres")
    password = os.environ.get("WATERMARK_DB_PASSWORD", "postgres")

    return psycopg.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password,
    )


def get_s3_client():
    """Create a boto3 S3 client configured for MinIO."""
    if not boto3:
        raise RuntimeError("boto3 is not installed. Please install boto3.")

    endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    access_key = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")
    region = os.environ.get("AWS_REGION", "us-east-1")

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def dump_table_as_sql(conn, table_name: str) -> str:
    """Generate idempotent INSERT statements for all rows in a table."""
    with conn.cursor() as cur:
        # Check if table exists
        cur.execute(
            """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_name = %s
            );
            """,
            (table_name,),
        )
        if not cur.fetchone()[0]:
            return f"-- Table {table_name} does not exist. Skipped.\n"

        cur.execute(f"SELECT * FROM {table_name}")
        rows = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]

    if not rows:
        return f"-- Table {table_name} has 0 records.\n"

    lines = [f"\n-- Dump for table: {table_name} ({len(rows)} records) --"]
    cols_joined = ", ".join(f'"{c}"' for c in colnames)

    for row in rows:
        val_strs = []
        for val in row:
            if val is None:
                val_strs.append("NULL")
            elif isinstance(val, (int, float)):
                val_strs.append(str(val))
            elif isinstance(val, bool):
                val_strs.append("TRUE" if val else "FALSE")
            else:
                escaped = str(val).replace("'", "''")
                val_strs.append(f"'{escaped}'")

        values_joined = ", ".join(val_strs)
        lines.append(
            f'INSERT INTO {table_name} ({cols_joined}) VALUES ({values_joined}) ON CONFLICT DO NOTHING;'
        )

    return "\n".join(lines) + "\n"


def create_backup_dump(conn) -> bytes:
    """Generate gzipped SQL dump of all control and audit tables."""
    header = (
        f"-- =========================================================================\n"
        f"-- VIETNAM ELECTRICITY LAKEHOUSE - CONTROL METADATA BACKUP\n"
        f"-- Created at: {datetime.now(timezone.utc).isoformat()}\n"
        f"-- Database: lakehouse_control\n"
        f"-- =========================================================================\n\n"
    )

    tables = ["pipeline_watermark", "bronze_ingestion_log", "pipeline_task_audit"]
    full_sql = header
    for tbl in tables:
        full_sql += dump_table_as_sql(conn, tbl)

    # Compress to gzip bytes
    compressed = gzip.compress(full_sql.encode("utf-8"))
    return compressed


def backup_to_minio(bucket: str = "warehouse", prefix: str = "backups/postgres/") -> str:
    """Perform full database dump and upload to MinIO S3."""
    print("Connecting to PostgreSQL control database...")
    conn = get_db_connection()
    try:
        print("Generating database dump...")
        compressed_bytes = create_backup_dump(conn)
        print(f"Dump generated: {len(compressed_bytes)} bytes (compressed).")
    finally:
        conn.close()

    now_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    object_key = f"{prefix.strip('/')}/lakehouse_control_{now_str}.sql.gz"

    s3 = get_s3_client()
    print(f"Uploading backup to s3://{bucket}/{object_key}...")
    s3.put_object(
        Bucket=bucket,
        Key=object_key,
        Body=compressed_bytes,
        ContentType="application/gzip",
    )
    print(f"[✓] Backup successfully uploaded to s3://{bucket}/{object_key}")

    # Retention Cleanup: purge backups older than 30 days
    cleanup_old_backups(s3, bucket, prefix, retention_days=30)
    return object_key


def cleanup_old_backups(s3, bucket: str, prefix: str, retention_days: int = 30) -> None:
    """Delete backups in MinIO older than retention_days."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
    try:
        resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix.strip('/') + "/")
        contents = resp.get("Contents", [])
        for obj in contents:
            key = obj["Key"]
            last_modified = obj["LastModified"]
            if last_modified < cutoff:
                print(f"Purging expired backup ({last_modified}): s3://{bucket}/{key}")
                s3.delete_object(Bucket=bucket, Key=key)
    except Exception as exc:
        print(f"Warning: Cleanup of old backups failed: {exc}")


def restore_from_minio(s3_key: str, bucket: str = "warehouse") -> None:
    """Download backup from MinIO and restore into PostgreSQL."""
    s3 = get_s3_client()
    print(f"Downloading backup from s3://{bucket}/{s3_key}...")
    resp = s3.get_object(Bucket=bucket, Key=s3_key)
    compressed_bytes = resp["Body"].read()

    decompressed_sql = gzip.decompress(compressed_bytes).decode("utf-8")
    print(f"Downloaded and decompressed {len(decompressed_sql)} characters of SQL.")

    conn = get_db_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(decompressed_sql)
        print(f"[✓] Database successfully RESTORED from s3://{bucket}/{s3_key}!")
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Control DB Disaster Recovery & Backup")
    parser.add_argument("--bucket", default=os.environ.get("BACKUP_BUCKET", "warehouse"))
    parser.add_argument("--prefix", default="backups/postgres/")
    parser.add_argument("--restore", type=str, default=None, help="S3 key of the backup to restore")
    args = parser.parse_args()

    if args.restore:
        restore_from_minio(args.restore, bucket=args.bucket)
    else:
        backup_to_minio(bucket=args.bucket, prefix=args.prefix)


if __name__ == "__main__":
    main()
