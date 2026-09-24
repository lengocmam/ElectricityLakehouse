import sys
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import time

import requests

from pyspark.sql.types import StructType, StructField, StringType
from utils.spark import create_spark_session


BASE_URL = "https://archive-api.open-meteo.com/v1/archive"

SOURCE_NAME = "open-meteo"
NAMESPACE = "nessie.bronze"
TABLE_NAME = "nessie.bronze.open_meteo"

VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

START_DATE_DEFAULT = date(2026, 9, 20)

# ============================================================
# 63 TỈNH/THÀNH PHỐ - TỌA ĐỘ ĐẠI DIỆN
# ============================================================

LOCATIONS = [
    {"location_name": "Ha Noi", "latitude": 21.0278, "longitude": 105.8342},
    {"location_name": "Hai Phong", "latitude": 20.8449, "longitude": 106.6881},
    {"location_name": "Quang Ninh", "latitude": 21.0064, "longitude": 107.2925},
    {"location_name": "Bac Giang", "latitude": 21.2731, "longitude": 106.1946},
    {"location_name": "Bac Ninh", "latitude": 21.1861, "longitude": 106.0763},
    {"location_name": "Hai Duong", "latitude": 20.9373, "longitude": 106.3146},
    {"location_name": "Hung Yen", "latitude": 20.6464, "longitude": 106.0511},
    {"location_name": "Thai Binh", "latitude": 20.4463, "longitude": 106.3366},
    {"location_name": "Nam Dinh", "latitude": 20.4388, "longitude": 106.1621},
    {"location_name": "Ha Nam", "latitude": 20.5835, "longitude": 105.9230},
    {"location_name": "Ninh Binh", "latitude": 20.2506, "longitude": 105.9745},
    {"location_name": "Vinh Phuc", "latitude": 21.3609, "longitude": 105.5474},
    {"location_name": "Phu Tho", "latitude": 21.3227, "longitude": 105.4020},
    {"location_name": "Bac Kan", "latitude": 22.1470, "longitude": 105.8348},
    {"location_name": "Cao Bang", "latitude": 22.6666, "longitude": 106.2639},
    {"location_name": "Lang Son", "latitude": 21.8537, "longitude": 106.7610},
    {"location_name": "Thai Nguyen", "latitude": 21.5944, "longitude": 105.8482},
    {"location_name": "Tuyen Quang", "latitude": 21.8233, "longitude": 105.2140},
    {"location_name": "Ha Giang", "latitude": 22.8233, "longitude": 104.9836},
    {"location_name": "Yen Bai", "latitude": 21.7168, "longitude": 104.8986},
    {"location_name": "Lao Cai", "latitude": 22.4856, "longitude": 103.9707},
    {"location_name": "Dien Bien", "latitude": 21.3860, "longitude": 103.0230},
    {"location_name": "Lai Chau", "latitude": 22.3964, "longitude": 103.4582},
    {"location_name": "Son La", "latitude": 21.3256, "longitude": 103.9188},
    {"location_name": "Hoa Binh", "latitude": 20.8172, "longitude": 105.3376},

    {"location_name": "Thanh Hoa", "latitude": 19.8067, "longitude": 105.7852},
    {"location_name": "Nghe An", "latitude": 18.6796, "longitude": 105.6813},
    {"location_name": "Ha Tinh", "latitude": 18.3559, "longitude": 105.8877},
    {"location_name": "Quang Binh", "latitude": 17.4677, "longitude": 106.6220},
    {"location_name": "Quang Tri", "latitude": 16.7943, "longitude": 107.0458},
    {"location_name": "Thua Thien Hue", "latitude": 16.4637, "longitude": 107.5909},
    {"location_name": "Da Nang", "latitude": 16.0544, "longitude": 108.2022},
    {"location_name": "Quang Nam", "latitude": 15.5394, "longitude": 108.0191},
    {"location_name": "Quang Ngai", "latitude": 15.1214, "longitude": 108.8044},
    {"location_name": "Binh Dinh", "latitude": 13.7820, "longitude": 109.2196},
    {"location_name": "Phu Yen", "latitude": 13.0882, "longitude": 109.0929},
    {"location_name": "Khanh Hoa", "latitude": 12.2388, "longitude": 109.1967},
    {"location_name": "Ninh Thuan", "latitude": 11.5753, "longitude": 108.9899},
    {"location_name": "Binh Thuan", "latitude": 10.9805, "longitude": 108.2615},

    {"location_name": "Kon Tum", "latitude": 14.3545, "longitude": 108.0076},
    {"location_name": "Gia Lai", "latitude": 13.9716, "longitude": 108.0151},
    {"location_name": "Dak Lak", "latitude": 12.6667, "longitude": 108.0500},
    {"location_name": "Dak Nong", "latitude": 12.0042, "longitude": 107.6907},
    {"location_name": "Lam Dong", "latitude": 11.9404, "longitude": 108.4583},

    {"location_name": "Binh Phuoc", "latitude": 11.7512, "longitude": 106.7235},
    {"location_name": "Tay Ninh", "latitude": 11.3352, "longitude": 106.1099},
    {"location_name": "Binh Duong", "latitude": 11.3254, "longitude": 106.4770},
    {"location_name": "Dong Nai", "latitude": 10.9453, "longitude": 106.8243},
    {"location_name": "Ba Ria - Vung Tau", "latitude": 10.4114, "longitude": 107.1362},
    {"location_name": "Ho Chi Minh City", "latitude": 10.8231, "longitude": 106.6297},

    {"location_name": "Long An", "latitude": 10.6956, "longitude": 106.2431},
    {"location_name": "Tien Giang", "latitude": 10.4493, "longitude": 106.3420},
    {"location_name": "Ben Tre", "latitude": 10.2434, "longitude": 106.3756},
    {"location_name": "Tra Vinh", "latitude": 9.8127, "longitude": 106.2993},
    {"location_name": "Vinh Long", "latitude": 10.2537, "longitude": 105.9722},
    {"location_name": "Dong Thap", "latitude": 10.4938, "longitude": 105.6882},
    {"location_name": "An Giang", "latitude": 10.5216, "longitude": 105.1259},
    {"location_name": "Kien Giang", "latitude": 9.8249, "longitude": 105.1259},
    {"location_name": "Can Tho", "latitude": 10.0452, "longitude": 105.7469},
    {"location_name": "Hau Giang", "latitude": 9.7579, "longitude": 105.6413},
    {"location_name": "Soc Trang", "latitude": 9.6025, "longitude": 105.9739},
    {"location_name": "Bac Lieu", "latitude": 9.2940, "longitude": 105.7278},
    {"location_name": "Ca Mau", "latitude": 9.1527, "longitude": 105.1961},
]

LATITUDES = ",".join(
    str(location["latitude"])
    for location in LOCATIONS
)

LONGITUDES = ",".join(
    str(location["longitude"])
    for location in LOCATIONS
)


# ============================================================
# OPEN-METEO VARIABLES
# ============================================================

HOURLY_VARIABLES = [
    "temperature_2m",
    "apparent_temperature",
    "relative_humidity_2m",
    "dew_point_2m",
    "wet_bulb_temperature_2m",
    "precipitation",
    "rain",
    "weather_code",
    "cloud_cover",
    "shortwave_radiation",
    "wind_speed_10m",
    "wind_gusts_10m",
]

DAILY_VARIABLES = [
    "weather_code",
    "temperature_2m_mean",
    "temperature_2m_max",
    "temperature_2m_min",
    "apparent_temperature_mean",
    "apparent_temperature_max",
    "apparent_temperature_min",
    "daylight_duration",
    "sunshine_duration",
    "precipitation_sum",
    "rain_sum",
    "precipitation_hours",
    "wind_speed_10m_max",
    "wind_gusts_10m_max",
    "shortwave_radiation_sum",
    "cloud_cover_mean",
    "dew_point_2m_mean",
    "relative_humidity_2m_mean",
    "wind_speed_10m_mean",
    "wet_bulb_temperature_2m_mean",
]


# ============================================================
# SESSION
# ============================================================

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/128.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}


def create_open_meteo_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(HEADERS)
    return session

# ============================================================
# SINGLE REQUEST
# ============================================================

def fetch_open_meteo(
    source_data_date: date,
    ingestion_timestamp: datetime,
    ingest_date: str,
    batch_id: str,
    task_index: int,
    session: requests.Session,
) -> tuple[dict | None, tuple[date, str] | None]:

    source_data_date_str = source_data_date.isoformat()

    params = {
        "latitude": LATITUDES,
        "longitude": LONGITUDES,
        "start_date": source_data_date_str,
        "end_date": source_data_date_str,
        "hourly": ",".join(HOURLY_VARIABLES),
        "daily": ",".join(DAILY_VARIABLES),
        "timezone": "Asia/Ho_Chi_Minh",
    }

    print(
        f"Crawling Open-Meteo: "
        f"data_date={source_data_date_str}, "
        f"locations={len(LOCATIONS)}"
    )

    MAX_RETRIES = 5
    INITIAL_BACKOFF = 2

    for attempt in range(MAX_RETRIES):

        try:
            response = session.get(
                BASE_URL,
                params=params,
                timeout=120,
            )

            if response.status_code == 429:
                error_message = response.text
                return None, (
                    source_data_date,
                    f"HTTP 429: {error_message}",
                )

            response.raise_for_status()

            content_type = response.headers.get(
                "Content-Type",
                "",
            )

            if "application/json" not in content_type:
                raise RuntimeError(
                    f"Unexpected Content-Type: {content_type}"
                )

            record = {
                "bronze_key": f"{batch_id}_{task_index:06d}",
                "source_name": SOURCE_NAME,
                "source_url": response.url,
                "source_data_date": source_data_date_str,
                "batch_id": batch_id,
                "ingestion_timestamp": (
                    ingestion_timestamp.isoformat()
                ),
                "ingest_date": ingest_date,
                "raw": response.text,
            }

            return record, None

        except (
            requests.RequestException,
            RuntimeError,
        ) as exc:

            if attempt == MAX_RETRIES - 1:
                return None, (
                    source_data_date,
                    str(exc),
                )

            wait_seconds = INITIAL_BACKOFF * (2 ** attempt)

            print(
                f"-> [RETRY] Request failed: "
                f"data_date={source_data_date_str}, "
                f"retry={attempt + 1}/{MAX_RETRIES}, "
                f"sleep={wait_seconds}s, "
                f"error={exc}"
            )

            time.sleep(wait_seconds)

    return None, (
        source_data_date,
        "Max retries exceeded",
    )


# ============================================================
# CRAWLER - CONCURRENT
# ============================================================

def crawl_open_meteo_dates(
    start_date: date,
    end_date: date,
    ingestion_timestamp: datetime,
    ingest_date: str,
    batch_id: str,
) -> tuple[list[dict], list[tuple[date, str]]]:

    records = []
    failed_dates = []

    total_dates = (
        end_date - start_date
    ).days + 1

    total_requests = total_dates

    print(
        f"Total dates: {total_dates}"
    )

    print(
        f"Total API requests: {total_requests}"
    )

    print(
        f"Locations per request: {len(LOCATIONS)}"
    )

    session = create_open_meteo_session()

    completed_requests = 0

    try:

        current_date = start_date

        while current_date <= end_date:

            print(
                f"\n===== DATE: "
                f"{current_date.isoformat()} ====="
            )

            task_index = (
                current_date - start_date
            ).days + 1

            record, error = fetch_open_meteo(
                source_data_date=current_date,
                ingestion_timestamp=ingestion_timestamp,
                ingest_date=ingest_date,
                batch_id=batch_id,
                task_index=task_index,
                session=session,
            )

            completed_requests += 1

            if record is not None:
                records.append(record)

            if error is not None:
                failed_dates.append(error)

            print(
                f"Progress: "
                f"{completed_requests}/"
                f"{total_requests}"
            )

            current_date += timedelta(days=1)

    finally:
        session.close()

    records.sort(
        key=lambda x: x["bronze_key"]
    )

    return records, failed_dates


# ============================================================
# WRITE BRONZE
# ============================================================

def write_bronze(
    spark,
    records: list[dict],
    ingest_date: str,
) -> None:

    if not records:
        print("No records to write.")
        return

    schema = StructType(
        [
            StructField("bronze_key", StringType(), False),
            StructField("source_name", StringType(), False),
            StructField("source_url", StringType(), False),
            StructField("source_data_date", StringType(), True),
            StructField("batch_id", StringType(), False),
            StructField("ingestion_timestamp", StringType(), False),
            StructField("ingest_date", StringType(), False),
            StructField("raw", StringType(), False),
        ]
    )

    df = spark.createDataFrame(
        records,
        schema=schema,
    )

    print(f"Records to write: {len(records)}")
    print(f"Ingest date: {ingest_date}")

    print(f"Creating namespace if needed: {NAMESPACE}")

    spark.sql(
        f"CREATE NAMESPACE IF NOT EXISTS {NAMESPACE}"
    )

    if spark.catalog.tableExists(TABLE_NAME):

        print("Table exists, overwriting partitions...")

        df.writeTo(TABLE_NAME).overwritePartitions()

    else:

        print(
            "Table does not exist, "
            "creating and writing..."
        )

        (
            df.writeTo(TABLE_NAME)
            .using("iceberg")
            .partitionedBy("ingest_date")
            .create()
        )
        print("Table created successfully.")

    print("\n=== VERIFY TABLE ===")

    spark.sql(
        "SHOW TABLES IN nessie.bronze"
    ).show(truncate=False)

    print("====================\n")

    print(
        "Open-Meteo Bronze ingestion "
        "completed successfully."
    )


# ============================================================
# MAIN
# ============================================================

def main() -> None:

    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(
            encoding="utf-8",
            errors="replace",
        )

    ingestion_timestamp = datetime.now(timezone.utc)

    ingest_date = (
        ingestion_timestamp
        .astimezone(VN_TZ)
        .date()
        .isoformat()
    )

    batch_id = ingestion_timestamp.strftime(
        "%Y%m%d%H%M%S"
    )

    print(f"batch_id: {batch_id}")
    print(f"ingest_date: {ingest_date}")
    print(f"Locations per API request: {len(LOCATIONS)}")

    # Crawl full range
    start_date = START_DATE_DEFAULT

    end_date = (
        datetime.now(VN_TZ).date()
        - timedelta(days=1)
    )

    print(
        f"Dataset date range: "
        f"{start_date.isoformat()} "
        f"to {end_date.isoformat()}"
    )

    if start_date > end_date:
        print("No dates to crawl.")
        return

    records, failed_dates = crawl_open_meteo_dates(
        start_date=start_date,
        end_date=end_date,
        ingestion_timestamp=ingestion_timestamp,
        ingest_date=ingest_date,
        batch_id=batch_id,
    )

    if failed_dates:

        print(
            "\n=== SUMMARY OF FAILED REQUESTS ==="
        )

        for failed_date, error_message in failed_dates:
            print(
                f"- {failed_date.isoformat()}: "
                f"{error_message}"
            )

        print(
            "===================================\n"
        )

    spark = None

    try:

        spark = create_spark_session(
            "ingest_open_meteo"
        )

        write_bronze(
            spark,
            records,
            ingest_date,
        )

    finally:

        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()