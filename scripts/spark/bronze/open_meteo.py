import sys
from datetime import date, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
import random
from zoneinfo import ZoneInfo
import time

import requests

from bronze.bronze_utils import write_raw_bronze
from bronze.watermark import get_watermark
from utils.spark import create_spark_session
from utils.logging import create_logger


logger = create_logger(__name__)

BASE_URL = "https://archive-api.open-meteo.com/v1/archive"

SOURCE_NAME = "open-meteo"
NAMESPACE = "nessie.bronze"
TABLE_NAME = "nessie.bronze.open_meteo"
OPEN_METEO_DATE_COLUMNS = ("source_data_start_date", "source_data_end_date")

VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

START_DATE_DEFAULT = date(2023, 5, 28)
REQUEST_WINDOW_DAYS = 14
MAX_RETRIES = 5
INITIAL_BACKOFF_SECONDS = 2
DEFAULT_429_WAIT_SECONDS = 60

LOCATIONS = [
    {"location_name": "Ha Noi", "latitude": 21.0278, "longitude": 105.8342},
    {"location_name": "Cao Bang", "latitude": 22.6666, "longitude": 106.2639},
    {"location_name": "Tuyen Quang", "latitude": 21.8233, "longitude": 105.2140},
    {"location_name": "Dien Bien", "latitude": 21.3860, "longitude": 103.0230},
    {"location_name": "Lai Chau", "latitude": 22.3964, "longitude": 103.4582},
    {"location_name": "Son La", "latitude": 21.3256, "longitude": 103.9188},
    {"location_name": "Lao Cai", "latitude": 21.7168, "longitude": 104.8986},
    {"location_name": "Thai Nguyen", "latitude": 21.5944, "longitude": 105.8482},
    {"location_name": "Lang Son", "latitude": 21.8537, "longitude": 106.7610},
    {"location_name": "Quang Ninh", "latitude": 21.0064, "longitude": 107.2925},
    {"location_name": "Bac Ninh", "latitude": 21.2731, "longitude": 106.1946},
    {"location_name": "Phu Tho", "latitude": 21.3227, "longitude": 105.4020},
    {"location_name": "Hung Yen", "latitude": 20.6464, "longitude": 106.0511},
    {"location_name": "Hai Phong", "latitude": 20.8449, "longitude": 106.6881},
    {"location_name": "Ninh Binh", "latitude": 20.2506, "longitude": 105.9745},
    {"location_name": "Thanh Hoa", "latitude": 19.8067, "longitude": 105.7852},
    {"location_name": "Nghe An", "latitude": 18.6796, "longitude": 105.6813},
    {"location_name": "Ha Tinh", "latitude": 18.3559, "longitude": 105.8877},

    {"location_name": "Quang Tri", "latitude": 17.4677, "longitude": 106.6220},
    {"location_name": "Hue", "latitude": 16.4637, "longitude": 107.5909},
    {"location_name": "Da Nang", "latitude": 16.0544, "longitude": 108.2022},
    {"location_name": "Quang Ngai", "latitude": 15.1214, "longitude": 108.8044},
    {"location_name": "Gia Lai", "latitude": 13.7820, "longitude": 109.2196},
    {"location_name": "Khanh Hoa", "latitude": 12.2388, "longitude": 109.1967},
    {"location_name": "Lam Dong", "latitude": 11.9404, "longitude": 108.4583},
    {"location_name": "Dak Lak", "latitude": 12.6667, "longitude": 108.0500},

    {"location_name": "Dong Nai", "latitude": 10.9453, "longitude": 106.8243},
    {"location_name": "Ho Chi Minh City", "latitude": 10.8231, "longitude": 106.6297},
    {"location_name": "Tay Ninh", "latitude": 10.6956, "longitude": 106.2431},
    {"location_name": "Can Tho", "latitude": 10.0452, "longitude": 105.7469},
    {"location_name": "Vinh Long", "latitude": 10.2537, "longitude": 105.9722},
    {"location_name": "Dong Thap", "latitude": 10.4493, "longitude": 106.3420},
    {"location_name": "An Giang", "latitude": 10.0125, "longitude": 105.0809},
    {"location_name": "Ca Mau", "latitude": 9.1527, "longitude": 105.1961},
]

LATITUDES = ",".join(str(location["latitude"]) for location in LOCATIONS)

LONGITUDES = ",".join(str(location["longitude"]) for location in LOCATIONS)

HOURLY_VARIABLES = [
    "temperature_2m",
    "apparent_temperature",
    "relative_humidity_2m",
    "precipitation",
    "cloud_cover",
    "shortwave_radiation",
    "wind_speed_10m",
    "weather_code",
]

DAILY_VARIABLES = [
    "weather_code",
    "temperature_2m_mean",
    "temperature_2m_max",
    "temperature_2m_min",
    "precipitation_sum",
    "precipitation_hours",
    "sunshine_duration",
    "cloud_cover_mean",
    "shortwave_radiation_sum",
    "wind_speed_10m_max",
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/128.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}


def parse_date(value: str, argument_name: str) -> date:
    """Parse a date string into a date object."""
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(
            f"{argument_name} must use YYYY-MM-DD format. "
            f"Received: {value}"
        ) from exc


def create_open_meteo_session() -> requests.Session:
    """Create and configure a requests session."""
    session = requests.Session()
    session.headers.update(HEADERS)
    return session


def get_retry_after_seconds(response: requests.Response) -> float | None:
    """Calculate the wait time for rate limiting."""
    retry_after = response.headers.get("Retry-After")

    if not retry_after:
        return None

    try:
        return max(float(retry_after), 0)
    except ValueError:
        try:
            retry_at = parsedate_to_datetime(retry_after)

            if retry_at.tzinfo is None:
                retry_at = retry_at.replace(tzinfo=timezone.utc)

            return max((retry_at - datetime.now(timezone.utc)).total_seconds(), 0)
        except (TypeError, ValueError):
            return None
        

def fetch_open_meteo(
    window_start_date: date,
    window_end_date: date,
    ingestion_timestamp: datetime,
    ingest_date: str,
    batch_id: str,
    task_index: int,
    session: requests.Session,
) -> tuple[dict | None, tuple[date, str] | None]:
    """Fetch raw weather data from the Open-Meteo API."""
    window_start_date_str = window_start_date.isoformat()
    window_end_date_str = window_end_date.isoformat()

    params = {
        "latitude": LATITUDES,
        "longitude": LONGITUDES,
        "start_date": window_start_date_str,
        "end_date": window_end_date_str,
        "hourly": ",".join(HOURLY_VARIABLES),
        "daily": ",".join(DAILY_VARIABLES),
        "timezone": "Asia/Ho_Chi_Minh",
    }

    minutely_attempt = 0
    general_attempt = 0

    while True:
        try:
            response = session.get(BASE_URL, params=params, timeout=120)

            if response.status_code == 429:
                error_message = response.text
                error_lower = error_message.lower()

                if "daily api request limit exceeded" in error_lower:
                    logger.warning(
                        f"Daily limit exceeded for window "
                        f"{window_start_date_str} to {window_end_date_str}"
                    )
                    return None, (window_start_date, error_message)

                if "hourly api request limit exceeded" in error_lower:
                    wait_seconds = 60 * 60

                    logger.warning(
                        f"Hourly limit exceeded for window "
                        f"{window_start_date_str} to {window_end_date_str}, "
                        f"sleeping for 1 hour"
                    )

                    time.sleep(wait_seconds)

                    logger.info("Retrying the same window after hourly limit")
                    continue

                minutely_attempt += 1

                if minutely_attempt >= MAX_RETRIES:
                    return None, (window_start_date, error_message)

                retry_after_seconds = get_retry_after_seconds(response)

                wait_seconds = (
                    retry_after_seconds
                    if retry_after_seconds is not None
                    else DEFAULT_429_WAIT_SECONDS
                    * (2 ** (minutely_attempt - 1))
                )

                wait_seconds += random.uniform(0, 1)

                logger.warning(
                    f"Minutely rate limit hit for window "
                    f"{window_start_date_str} to {window_end_date_str}, "
                    f"retry {minutely_attempt}/{MAX_RETRIES}, "
                    f"sleeping {wait_seconds:.1f}s"
                )

                time.sleep(wait_seconds)
                continue

            response.raise_for_status()

            content_type = response.headers.get("Content-Type", "")

            if "application/json" not in content_type:
                raise RuntimeError(f"Unexpected Content-Type: {content_type}")

            record = {
                "bronze_key": f"{batch_id}_{task_index:06d}",
                "source_name": SOURCE_NAME,
                "source_url": response.url,
                "source_data_start_date": window_start_date_str,
                "source_data_end_date": window_end_date_str,
                "batch_id": batch_id,
                "ingestion_timestamp": (ingestion_timestamp.isoformat()),
                "ingest_date": ingest_date,
                "raw": response.text,
            }

            return record, None

        except (requests.RequestException, RuntimeError) as exc:

            general_attempt += 1

            if general_attempt >= MAX_RETRIES:
                return None, (window_start_date, str(exc))

            wait_seconds = (INITIAL_BACKOFF_SECONDS * (2 ** (general_attempt - 1)))

            logger.error(
                f"Request failed for window {window_start_date_str} to "
                f"{window_end_date_str}, retry {general_attempt}/{MAX_RETRIES}, "
                f"sleeping {wait_seconds}s, error: {exc}"
            )

            time.sleep(wait_seconds)
    

def crawl_open_meteo_dates(
    start_date: date,
    end_date: date,
    ingestion_timestamp: datetime,
    ingest_date: str,
    batch_id: str,
) -> tuple[list[dict], list[tuple[date, str]], date | None]:
    """Crawl Open-Meteo data for a given date range."""
    records = []
    failed_dates = []

    total_dates = (end_date - start_date).days + 1

    total_requests = (total_dates + REQUEST_WINDOW_DAYS - 1) // REQUEST_WINDOW_DAYS

    logger.info(
        f"Starting crawl for {total_dates} dates using {total_requests} requests "
        f"and {len(LOCATIONS)} locations."
    )

    session = create_open_meteo_session()

    completed_requests = 0
    last_successful_window_end_date = None

    try:
        window_start_date = start_date

        while window_start_date <= end_date:
            window_end_date = min(
                window_start_date + timedelta(days=REQUEST_WINDOW_DAYS - 1),
                end_date,
            )

            task_index = completed_requests + 1

            record, error = fetch_open_meteo(
                window_start_date=window_start_date,
                window_end_date=window_end_date,
                ingestion_timestamp=ingestion_timestamp,
                ingest_date=ingest_date,
                batch_id=batch_id,
                task_index=task_index,
                session=session,
            )

            if record is not None:
                records.append(record)

                last_successful_window_end_date = window_end_date

                completed_requests += 1

                window_start_date = (window_end_date + timedelta(days=1))

                continue

            if error is not None:
                failed_dates.append(error)

                logger.error(
                    f"Stopping crawl at window {window_start_date.isoformat()} "
                    f"to {window_end_date.isoformat()}"
                )

                break

    finally:
        session.close()

    records.sort(key=lambda x: x["bronze_key"])

    return (records, failed_dates, last_successful_window_end_date)


def ensure_open_meteo_date_columns(spark) -> None:
    """Ensure that required date columns exist in the bronze table."""
    if not spark.catalog.tableExists(TABLE_NAME):
        return

    existing_columns = {field.name for field in spark.table(TABLE_NAME).schema.fields}
    missing_columns = [column for column in OPEN_METEO_DATE_COLUMNS if column not in existing_columns]

    if missing_columns:
        columns_sql = ", ".join(f"{column} STRING" for column in missing_columns)
        spark.sql(
            f"ALTER TABLE {TABLE_NAME} "
            f"ADD COLUMNS ({columns_sql})"
        )


def write_bronze(
    spark,
    records: list[dict],
    ingest_date: str,
    last_successful_data_date: date | None,
    update_watermark_after_write: bool,
) -> None:
    """Write raw data records into the target bronze table."""
    ensure_open_meteo_date_columns(spark)

    if write_raw_bronze(
        spark,
        records,
        TABLE_NAME,
        ingest_date,
        last_successful_data_date,
        namespace=NAMESPACE,
        dataset_name=SOURCE_NAME,
        update_watermark_after_write=update_watermark_after_write,
    ):
        logger.info("Verifying tables in nessie.bronze")
        spark.sql("SHOW TABLES IN nessie.bronze").show(truncate=False)


def main(
    run_mode: str = "incremental",
    start_date: str | None = None,
    end_date: str | None = None,
) -> None:
    """Run the main extraction and load process for Open-Meteo data."""
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    ingestion_timestamp = datetime.now(timezone.utc)

    ingest_date = ingestion_timestamp.astimezone(VN_TZ).date().isoformat()

    batch_id = ingestion_timestamp.strftime("%Y%m%d%H%M%S")

    logger.info(f"Starting Open-Meteo crawl: batch_id={batch_id}, ingest_date={ingest_date}")

    if run_mode not in {"incremental", "backfill"}:
        raise ValueError(f"Unsupported run_mode: {run_mode}")

    if run_mode == "backfill":
        if start_date is None or end_date is None:
            raise ValueError("Backfill requires both start_date and end_date.")

        crawl_start_date = parse_date(start_date, "start_date")

        crawl_end_date = parse_date(end_date, "end_date")

        if crawl_start_date > crawl_end_date:
            raise ValueError("start_date must be less than or equal to end_date.")

        logger.info(f"Run mode: backfill, date range: {crawl_start_date.isoformat()} to {crawl_end_date.isoformat()}")

        last_successful_data_date = None

    else:

        watermark = get_watermark(SOURCE_NAME)

        logger.info(f"Run mode: incremental, previous watermark: {watermark}")

        if watermark is None:
            crawl_start_date = START_DATE_DEFAULT
        else:
            crawl_start_date = (watermark + timedelta(days=1))

        crawl_end_date = (datetime.now(VN_TZ).date() - timedelta(days=1))

        logger.info(f"Dataset date range: {crawl_start_date.isoformat()} to {crawl_end_date.isoformat()}")

    if crawl_start_date > crawl_end_date:
        logger.info("No dates to crawl.")
        return

    records, failed_dates, last_successful_window_end_date = (
        crawl_open_meteo_dates(
            start_date=crawl_start_date,
            end_date=crawl_end_date,
            ingestion_timestamp=ingestion_timestamp,
            ingest_date=ingest_date,
            batch_id=batch_id,
        )
    )

    if failed_dates:
        for failed_date, error_message in failed_dates:
            logger.error(f"Failed request on {failed_date.isoformat()}: {error_message}")

    if not records:
        logger.warning("No successful Open-Meteo windows. Nothing to write.")
        return

    if last_successful_window_end_date is None:
        raise RuntimeError("Records exist but no successful window end date was found.")

    logger.info(f"Last successful window end date: {last_successful_window_end_date.isoformat()}")

    spark = None

    try:
        spark = create_spark_session("ingest_open_meteo")

        write_bronze(
            spark=spark,
            records=records,
            ingest_date=ingest_date,
            last_successful_data_date=(last_successful_window_end_date if run_mode == "incremental" else None),
            update_watermark_after_write=(run_mode == "incremental"),
        )

    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
