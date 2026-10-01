import time
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests
from bronze.bronze_utils import write_raw_bronze
from bronze.watermark import get_watermark
from bronze.http_client import create_legacy_tls_session
from utils.spark import create_spark_session


# URL trang nhúng bảng hồ chứa thủy điện của EVN.
# Tham số td (thời điểm) được truyền vào dưới dạng dd/MM/yyyy HH:mm.
BASE_URL = "https://hochuathuydien.evn.com.vn/PageHoChuaThuyDienEmbedEVN.aspx"

# Tên nguồn dữ liệu — khớp với convention source_name của evn.py
SOURCE_NAME = "evn_hydro"

# Dataset bắt đầu từ 01/01/2023
START_DATE_DEFAULT = date(2023, 1, 1)
VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

# Thời điểm cuối ngày dùng để request snapshot của ngày đó.
# Sử dụng 23:00 thay vì 23:59 vì đây là múi giờ chốt thường gặp của hệ thống EVN.
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

# Nessie catalog — namespace và tên bảng Bronze Hydro
NAMESPACE = "nessie.bronze"
TABLE_NAME = "nessie.bronze.evn_hydro"


def create_hydro_session() -> requests.Session:
    return create_legacy_tls_session(BASE_URL)


def build_end_of_day_td_param(data_date: date) -> str:
    """
    Tạo chuỗi tham số td theo format website EVN Hydro yêu cầu:
    dd/MM/yyyy HH:mm — đại diện cho thời điểm cuối ngày của data_date.
    Ví dụ: date(2026, 9, 22) → "22/09/2026 23:00"
    """
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
    """
    Thực hiện HTTP GET để lấy raw HTML snapshot hồ chứa thủy điện
    cho từng ngày từ start_date đến end_date.

    Trả về list chứa các raw records. Nếu một ngày lỗi, throw exception
    để dừng pipeline, không tạo record giả.
    """
    records = []
    current_date = start_date
    index = 1

    while current_date <= end_date:
        td_param = build_end_of_day_td_param(current_date)
        source_url = f"{BASE_URL}?td={td_param}"

        print(f"Crawling Hydro snapshot for data_date={current_date.isoformat()} (URL: {source_url})")

        try:
            response = session.get(
                BASE_URL,
                params={"td": td_param},
                headers=HEADERS,
                timeout=60,
            )
            response.raise_for_status()
            
            print(f"  HTTP status: {response.status_code}, Length: {len(response.content)} bytes")

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
            print(f"Error crawling data_date={current_date.isoformat()}: {e}")
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

    if run_mode not in {"incremental", "backfill"}:
        raise ValueError(
            f"Unsupported run mode: {run_mode}"
        )

    # Thời điểm thực thi ingestion (UTC)
    ingestion_timestamp = datetime.now(timezone.utc)
    ingest_date = ingestion_timestamp.date().isoformat()
    batch_id = ingestion_timestamp.strftime("%Y%m%d%H%M%S")

    print(f"batch_id: {batch_id}")
    print(f"ingest_date: {ingest_date}")

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

        # Hydro chốt dữ liệu lúc 23:00,
        # nên chỉ lấy đến ngày hôm qua ở Việt Nam.
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

    if crawl_start_date > crawl_end_date:
        print("No new dates to crawl. Dataset is up to date.")
        return

    spark = None

    try:
        spark = create_spark_session(
            "ingest_evn_hydro"
        )

        print(
            f"Creating namespace if needed: {NAMESPACE}"
        )

        spark.sql(
            f"CREATE NAMESPACE IF NOT EXISTS {NAMESPACE}"
        )

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
            print(
                "No Hydro data found for the requested range. "
                "Nothing to write."
            )
            return

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
