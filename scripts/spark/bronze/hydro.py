import time
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests
from bronze.bronze_utils import write_raw_bronze
from bronze.watermark import get_watermark
from bronze.http_client import create_legacy_tls_session
from utils.spark import create_spark_session
from utils.logging import create_logger

logger = create_logger(__name__)

BASE_URL = "https://hochuathuydien.evn.com.vn/PageHoChuaThuyDienEmbedEVN.aspx"
SOURCE_NAME = "evn_hydro"
START_DATE_DEFAULT = date(2023, 1, 1)
VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

END_OF_DAY_HOUR = 23
END_OF_DAY_MINUTE = 0

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/151.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8",
}

NAMESPACE = "nessie.bronze"
TABLE_NAME = "nessie.bronze.evn_hydro"


def create_hydro_session() -> requests.Session:
    """Create a legacy TLS session for the Hydro website."""
    return create_legacy_tls_session(BASE_URL)


def build_end_of_day_td_param(data_date: date) -> str:
    """Build the end-of-day time parameter string for a given date."""
    return (
        f"{data_date.day:02d}/{data_date.month:02d}/{data_date.year} "
        f"{END_OF_DAY_HOUR:02d}:{END_OF_DAY_MINUTE:02d}"
    )


def crawl_hydro_dates(
    session: requests.Session,
    start_date: date,
    end_date: date,
    ingestion_timestamp: datetime,
    ingest_date: str,
    batch_id: str,
) -> list[dict]:
    """Crawl raw HTML snapshots for reservoir data within the specified date range."""
    records = []
    current_date = start_date
    index = 1

    while current_date <= end_date:
        td_param = build_end_of_day_td_param(current_date)
        
        try:
            response = session.get(BASE_URL, params={"td": td_param}, headers=HEADERS, timeout=60)
            response.raise_for_status()
            
            record = {
                "bronze_key": f"{batch_id}_{index}",
                "source_name": SOURCE_NAME,
                "source_url": response.url,
                "source_data_date": current_date.isoformat(),
                "source_data_start_date": None,
                "source_data_end_date": None,
                "batch_id": batch_id,
                "ingestion_timestamp": ingestion_timestamp.isoformat(),
                "ingest_date": ingest_date,
                "raw": response.text,
            }
            records.append(record)
            
        except Exception as e:
            logger.error(f"Error crawling data_date={current_date.isoformat()}: {e}")
            raise

        current_date += timedelta(days=1)
        index += 1
        time.sleep(0.1)

    return records


def write_bronze(
    spark,
    records: list[dict],
    ingest_date: str,
    last_successful_data_date: date | None,
    update_watermark_after_write: bool,
) -> None:
    """Write raw records to the bronze layer."""
    write_raw_bronze(
        spark=spark,
        records=records,
        table_name=TABLE_NAME,
        ingest_date=ingest_date,
        last_successful_data_date=last_successful_data_date,
        dataset_name=SOURCE_NAME,
        update_watermark_after_write=update_watermark_after_write,
    )


def main(
    run_mode: str = "incremental",
    start_date: str | None = None,
    end_date: str | None = None,
) -> None:
    """Run the bronze ingestion pipeline for hydro data."""
    if run_mode not in {"incremental", "backfill"}:
        raise ValueError(f"Unsupported run mode: {run_mode}")

    ingestion_timestamp = datetime.now(timezone.utc)
    ingest_date = ingestion_timestamp.date().isoformat()
    batch_id = ingestion_timestamp.strftime("%Y%m%d%H%M%S")

    logger.info(f"Starting hydro ingestion pipeline with batch_id: {batch_id} and ingest_date: {ingest_date}")

    if run_mode == "backfill":
        if start_date is None or end_date is None:
            raise ValueError("Backfill requires both start_date and end_date.")

        crawl_start_date = date.fromisoformat(start_date)
        crawl_end_date = date.fromisoformat(end_date)

        if crawl_start_date > crawl_end_date:
            raise ValueError("start_date must be less than or equal to end_date.")

        logger.info(f"Run mode: backfill. Date range: {crawl_start_date} to {crawl_end_date}")

    else:
        last_successful_data_date = get_watermark(SOURCE_NAME)

        if last_successful_data_date:
            crawl_start_date = (last_successful_data_date + timedelta(days=1))
        else:
            crawl_start_date = START_DATE_DEFAULT

        crawl_end_date = (datetime.now(VN_TZ).date() - timedelta(days=1))

        logger.info(f"Run mode: incremental. Previous watermark: {last_successful_data_date}. Date range: {crawl_start_date} to {crawl_end_date}")

    if crawl_start_date > crawl_end_date:
        logger.info("No new dates to crawl. Dataset is up to date.")
        return

    spark = None

    try:
        spark = create_spark_session("ingest_evn_hydro")

        logger.info(f"Creating namespace if needed: {NAMESPACE}")

        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {NAMESPACE}")

        session = create_hydro_session()

        records = crawl_hydro_dates(
            session=session,
            start_date=crawl_start_date,
            end_date=crawl_end_date,
            ingestion_timestamp=ingestion_timestamp,
            ingest_date=ingest_date,
            batch_id=batch_id,
        )

        if not records:
            logger.info("No Hydro data found for the requested range. Nothing to write.")
            return

        write_bronze(
            spark=spark,
            records=records,
            ingest_date=ingest_date,
            last_successful_data_date=(crawl_end_date if run_mode == "incremental" else None),
            update_watermark_after_write=(run_mode == "incremental"),
        )
        logger.info(f"Successfully processed and wrote {len(records)} records.")

    finally:
        if spark is not None:
            spark.stop()
            logger.info("Spark session stopped.")


if __name__ == "__main__":
    main()
