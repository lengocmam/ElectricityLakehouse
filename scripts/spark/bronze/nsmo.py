import json
import sys
import time
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests
import urllib3

from bronze.bronze_utils import write_raw_bronze
from bronze.control import get_watermark, start_ingestion_log, finish_ingestion_log
from utils.spark import create_spark_session
from utils.logging import create_logger

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = create_logger(__name__)

BASE_URL = "https://www.nsmo.vn"
API_PATH = "/api/services/app/Pages/GetChartPhuTaiVM"

SOURCE_NAME = "nsmo"
NAMESPACE = "nessie.bronze"
TABLE_NAME = "nessie.bronze.nsmo"

VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")
START_DATE_DEFAULT = date(2023, 1, 1)


HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/128.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Referer": f"{BASE_URL}/HeThongDien",
    "X-Requested-With": "XMLHttpRequest",
}


def create_nsmo_session() -> requests.Session:
    """Create and return an authenticated session for NSMO API requests."""
    session = requests.Session()
    session.headers.update(HEADERS)

    session.get(f"{BASE_URL}/HeThongDien", verify=False, timeout=30)

    return session


def validate_nsmo_payload(text: str) -> tuple[bool, str | None]:
    """Validate NSMO JSON payload for empty response, maintenance, captcha, or empty data."""
    if not text or not text.strip() or len(text.strip()) < 50:
        return False, "PAYLOAD_EMPTY"

    stripped = text.strip()
    if stripped.startswith("<"):
        lower_text = stripped.lower()
        if "cloudflare" in lower_text or "captcha" in lower_text or "just a moment..." in lower_text:
            return False, "CAPTCHA_PAGE"
        if "bảo trì" in lower_text or "maintenance" in lower_text:
            return False, "MAINTENANCE_PAGE"
        return False, "HTML_RESPONSE_EXPECTED_JSON"

    try:
        payload = json.loads(text)
    except Exception as exc:
        return False, f"INVALID_JSON: {exc}"

    if not isinstance(payload, dict):
        return False, "INVALID_JSON_STRUCTURE"

    if payload.get("success") is False:
        err = payload.get("error", {})
        err_msg = err.get("message") if isinstance(err, dict) else str(err)
        return False, f"API_ERROR: {err_msg or 'Unsuccessful response'}"

    result = payload.get("result")
    if result is None:
        raw_data = payload.get("data")
        if raw_data is None or raw_data == []:
            return False, "EMPTY_DATA"
        return False, "MISSING_RESULT"

    if isinstance(result, dict):
        if result.get("status") is False:
            return False, f"API_STATUS_FALSE: {result.get('message') or 'status is false'}"

        inner_data = result.get("data")
        if inner_data is None or inner_data == []:
            return False, "EMPTY_DATA"

        if isinstance(inner_data, dict):
            phu_tais = inner_data.get("phuTais")
            if not phu_tais:
                return False, "EMPTY_PHUTAIS"
    elif isinstance(result, list):
        if not result:
            return False, "EMPTY_RESULT"

    return True, None


def crawl_nsmo_dates(
    session: requests.Session,
    start_date: date,
    end_date: date,
    ingestion_timestamp: datetime,
    ingest_date: str,
    batch_id: str,
) -> tuple[list[dict], list[tuple[date, str]]]:
    """Fetch raw data from API across the specified date range."""
    records = []
    failed_dates: list[tuple[date, str]] = []

    current_date = start_date
    index = 1

    while current_date <= end_date:

        day_str_api = current_date.strftime("%d/%m/%Y")
        source_data_date = current_date.isoformat()

        try:
            response = session.get(f"{BASE_URL}{API_PATH}", params={"day": day_str_api}, verify=False, timeout=30)

            response.raise_for_status()

            content_type = response.headers.get("content-type", "")

            if "application/json" not in content_type.lower():
                logger.info("Response is not JSON, re-warming session and retrying.")

                session.get(f"{BASE_URL}/HeThongDien", verify=False, timeout=30)

                response = session.get(f"{BASE_URL}{API_PATH}", params={"day": day_str_api}, verify=False, timeout=30)

                response.raise_for_status()

                content_type = response.headers.get("content-type", "")

                if "application/json" not in content_type.lower():
                    raise RuntimeError(
                        f"Expected JSON but received "
                        f"{content_type}. "
                        f"URL: {response.url}"
                    )

            is_valid, error_reason = validate_nsmo_payload(response.text)
            if not is_valid:
                logger.warning(
                    f"Invalid payload for data_date={source_data_date}: {error_reason}"
                )
                failed_dates.append((current_date, error_reason))
                current_date += timedelta(days=1)
                time.sleep(0.1)
                continue

            records.append(
                {
                    "bronze_key": f"{batch_id}_{index}",
                    "source_name": SOURCE_NAME,
                    "source_url": response.url,
                    "source_data_date": source_data_date,
                    "source_data_start_date": None,
                    "source_data_end_date": None,
                    "batch_id": batch_id,
                    "ingestion_timestamp": (
                        ingestion_timestamp.isoformat()
                    ),
                    "ingest_date": ingest_date,
                    "raw": response.text,
                }
            )

            index += 1

        except (requests.RequestException, RuntimeError) as e:
            logger.error(
                f"Failed to fetch data for "
                f"data_date={source_data_date}: {e}"
            )

            failed_dates.append((current_date, f"REQUEST_ERROR: {e}"))

        current_date += timedelta(days=1)

        time.sleep(0.1)

    return records, failed_dates


def write_bronze(
    spark,
    records: list[dict],
    ingest_date: str,
    last_successful_data_date: date | None,
    update_watermark_after_write: bool,
) -> None:
    """Write the fetched records to the bronze layer."""
    write_raw_bronze(
        spark=spark,
        records=records,
        table_name=TABLE_NAME,
        ingest_date=ingest_date,
        last_successful_data_date=last_successful_data_date,
        namespace=NAMESPACE,
        dataset_name=SOURCE_NAME,
        update_watermark_after_write=(
            update_watermark_after_write
        ),
    )


def main(
    run_mode: str = "incremental",
    start_date: str | None = None,
    end_date: str | None = None,
) -> None:
    """Execute the main ingestion workflow for NSMO data."""
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    if run_mode not in {"incremental", "backfill"}:
        raise ValueError(f"Unsupported run mode: {run_mode}")

    ingestion_timestamp = datetime.now(timezone.utc)
    ingest_date = ingestion_timestamp.date().isoformat()
    batch_id = ingestion_timestamp.strftime("%Y%m%d%H%M%S")

    logger.info(f"Starting ingestion with batch_id: {batch_id}, ingest_date: {ingest_date}")

    if run_mode == "backfill":
        if start_date is None or end_date is None:
            raise ValueError("Backfill requires both start_date and end_date.")

        crawl_start_date = date.fromisoformat(start_date)
        crawl_end_date = date.fromisoformat(end_date)

        if crawl_start_date > crawl_end_date:
            raise ValueError("start_date must be less than or equal to end_date.")

        logger.info(f"Run mode: backfill from {crawl_start_date} to {crawl_end_date}")

        last_successful_data_date = None

    else:
        last_successful_data_date = get_watermark(SOURCE_NAME)

        if last_successful_data_date:
            crawl_start_date = (last_successful_data_date + timedelta(days=1))
        else:
            crawl_start_date = START_DATE_DEFAULT

        crawl_end_date = (datetime.now(VN_TZ).date() - timedelta(days=1))

        logger.info(f"Run mode: incremental. Previous watermark: {last_successful_data_date}")
        logger.info(f"Dataset date range: {crawl_start_date} to {crawl_end_date}")

    start_ingestion_log(
        run_id=batch_id,
        dataset_name=SOURCE_NAME,
        run_mode=run_mode,
        start_date=crawl_start_date,
        end_date=crawl_end_date,
        started_at=ingestion_timestamp,
    )

    start_time = time.time()
    status = "SUCCESS"
    error_msg = None
    records_count = 0

    spark = None
    try:
        if crawl_start_date > crawl_end_date:
            logger.info("No new dates to crawl. Dataset is up to date.")
            return

        session = create_nsmo_session()

        records, failed_dates = crawl_nsmo_dates(
            session=session,
            start_date=crawl_start_date,
            end_date=crawl_end_date,
            ingestion_timestamp=ingestion_timestamp,
            ingest_date=ingest_date,
            batch_id=batch_id,
        )

        records_count = len(records)

        if failed_dates:
            error_msg = "; ".join(f"{dt.isoformat()} - {reason}" for dt, reason in failed_dates)
            for dt, reason in failed_dates:
                logger.error(f"Failed date {dt.isoformat()}: {reason}")

        if records_count > 0 and failed_dates:
            status = "PARTIAL"
        elif records_count > 0 and not failed_dates:
            status = "SUCCESS"
        else:
            status = "FAILED"
            logger.warning("No NSMO records were crawled. Nothing to write to Bronze.")
            if not error_msg:
                error_msg = "No records were crawled."
            return

        successful_dates = [
            date.fromisoformat(r["source_data_date"])
            for r in records
            if r.get("source_data_date")
        ]
        last_successful_data_date = max(successful_dates) if successful_dates else None

        logger.info(
            f"Run status: {status}. Records written: {records_count}. "
            f"Furthest successful date: {last_successful_data_date}"
        )

        spark = create_spark_session("ingest_nsmo")

        logger.info(f"Creating namespace if needed: {NAMESPACE}")

        spark.sql(
            f"CREATE NAMESPACE IF NOT EXISTS "
            f"{NAMESPACE}"
        )

        write_bronze(
            spark=spark,
            records=records,
            ingest_date=ingest_date,
            last_successful_data_date=(
                last_successful_data_date if run_mode == "incremental" else None
            ),
            update_watermark_after_write=(run_mode == "incremental")
        )

    except Exception as exc:
        status = "FAILED"
        error_msg = str(exc)
        raise

    finally:
        if spark is not None:
            spark.stop()

        duration_seconds = time.time() - start_time
        finished_at = datetime.now(timezone.utc)
        finish_ingestion_log(
            run_id=batch_id,
            status=status,
            records_count=records_count,
            error_message=error_msg,
            finished_at=finished_at,
            duration_seconds=duration_seconds,
        )


if __name__ == "__main__":
    main()