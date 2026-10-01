import sys
import time
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests
import urllib3

from bronze.bronze_utils import write_raw_bronze
from bronze.watermark import get_watermark
from utils.spark import create_spark_session

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


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
    session = requests.Session()
    session.headers.update(HEADERS)

    # Warm-up request để khởi tạo cookie session như trình duyệt
    session.get(
        f"{BASE_URL}/HeThongDien",
        verify=False,
        timeout=30,
    )

    return session


def crawl_nsmo_dates(
    session: requests.Session,
    start_date: date,
    end_date: date,
    ingestion_timestamp: datetime,
    ingest_date: str,
    batch_id: str,
) -> tuple[list[dict], list[date]]:

    records = []
    failed_dates = []

    current_date = start_date
    index = 1

    while current_date <= end_date:

        day_str_api = current_date.strftime("%d/%m/%Y")
        source_data_date = current_date.isoformat()

        print(
            f"Crawling NSMO snapshot for "
            f"data_date={source_data_date} "
            f"(API param: {day_str_api})"
        )

        try:
            response = session.get(
                f"{BASE_URL}{API_PATH}",
                params={"day": day_str_api},
                verify=False,
                timeout=30,
            )

            response.raise_for_status()

            # Kiểm tra session cookie có còn hợp lệ không
            content_type = response.headers.get(
                "content-type",
                "",
            )

            if "application/json" not in content_type.lower():

                print(
                    "  -> Response is not JSON. "
                    "Re-warming session and retrying..."
                )

                session.get(
                    f"{BASE_URL}/HeThongDien",
                    verify=False,
                    timeout=30,
                )

                response = session.get(
                    f"{BASE_URL}{API_PATH}",
                    params={"day": day_str_api},
                    verify=False,
                    timeout=30,
                )

                response.raise_for_status()

                content_type = response.headers.get(
                    "content-type",
                    "",
                )

                if "application/json" not in content_type.lower():
                    raise RuntimeError(
                        f"Expected JSON but received "
                        f"{content_type}. "
                        f"URL: {response.url}"
                    )

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

            print(
                f"  -> HTTP 200, "
                f"length: {len(response.content)} bytes"
            )

        except (requests.RequestException, RuntimeError) as e:

            print(
                f"  -> [ERROR] Failed to fetch data for "
                f"data_date={source_data_date}: {e}"
            )

            failed_dates.append(current_date)

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

    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(
            encoding="utf-8",
            errors="replace",
        )

    if run_mode not in {"incremental", "backfill"}:
        raise ValueError(
            f"Unsupported run mode: {run_mode}"
        )

    ingestion_timestamp = datetime.now(timezone.utc)
    ingest_date = ingestion_timestamp.date().isoformat()
    batch_id = ingestion_timestamp.strftime("%Y%m%d%H%M%S")

    print(f"batch_id: {batch_id}")
    print(f"ingest_date: {ingest_date}")

    # =========================================================
    # Determine crawl date range
    # =========================================================

    if run_mode == "backfill":

        if start_date is None or end_date is None:
            raise ValueError(
                "Backfill requires both start_date and end_date."
            )

        crawl_start_date = date.fromisoformat(start_date)
        crawl_end_date = date.fromisoformat(end_date)

        if crawl_start_date > crawl_end_date:
            raise ValueError(
                "start_date must be less than or equal to end_date."
            )

        print("Run mode: backfill")
        print(
            f"Backfill date range: "
            f"{crawl_start_date} to {crawl_end_date}"
        )

        last_successful_data_date = None

    else:

        last_successful_data_date = get_watermark(
            SOURCE_NAME
        )

        if last_successful_data_date:

            crawl_start_date = (
                last_successful_data_date
                + timedelta(days=1)
            )

        else:

            crawl_start_date = START_DATE_DEFAULT

        crawl_end_date = (
            datetime.now(VN_TZ).date()
            - timedelta(days=1)
        )

        print("Run mode: incremental")
        print(
            f"Previous watermark: "
            f"{last_successful_data_date}"
        )
        print(
            f"Dataset date range: "
            f"{crawl_start_date} to {crawl_end_date}"
        )

    # =========================================================
    # Nothing to crawl
    # =========================================================

    if crawl_start_date > crawl_end_date:

        print(
            "No new dates to crawl. "
            "Dataset is up to date."
        )

        return

    # =========================================================
    # Crawl
    # =========================================================

    session = create_nsmo_session()

    records, failed_dates = crawl_nsmo_dates(
        session=session,
        start_date=crawl_start_date,
        end_date=crawl_end_date,
        ingestion_timestamp=ingestion_timestamp,
        ingest_date=ingest_date,
        batch_id=batch_id,
    )

    # =========================================================
    # Handle failed dates
    # =========================================================

    if failed_dates:

        print("\n=== SUMMARY OF FAILED DATES ===")

        for failed_date in failed_dates:
            print(f"- {failed_date.isoformat()}")

        print("===============================\n")

        raise RuntimeError(
            f"NSMO crawl failed for "
            f"{len(failed_dates)} date(s). "
            "Watermark will not be updated."
        )

    # =========================================================
    # Write Bronze
    # =========================================================

    if not records:

        print(
            "No NSMO data found for the requested range. "
            "Nothing to write."
        )

        return

    spark = None

    try:

        spark = create_spark_session(
            "ingest_nsmo"
        )

        print(
            f"Creating namespace if needed: "
            f"{NAMESPACE}"
        )

        spark.sql(
            f"CREATE NAMESPACE IF NOT EXISTS "
            f"{NAMESPACE}"
        )

        write_bronze(
            spark=spark,
            records=records,
            ingest_date=ingest_date,
            last_successful_data_date=(
                crawl_end_date
                if run_mode == "incremental"
                else None
            ),
            update_watermark_after_write=(
                run_mode == "incremental"
            ),
        )

    finally:

        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()