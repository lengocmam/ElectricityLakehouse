import re
import time
from datetime import date, datetime, timezone
from urllib.parse import urljoin
from utils.logging import create_logger

import requests
from lxml import html

from bronze.bronze_utils import write_raw_bronze
from bronze.control import get_watermark, start_ingestion_log, finish_ingestion_log
from bronze.http_client import create_legacy_tls_session
from utils.spark import create_spark_session


logger = create_logger(__name__)

BASE_URL = "https://www.evn.com.vn"

FIRST_URL = (
    f"{BASE_URL}/vi-VN/news-l/"
    "Thong-tin-tom-tat-van-hanh-HTD-Quoc-gia-60-2015"
)

TARGET_PREFIX = (
    "/d/vi-VN/news/"
    "Thong-tin-chung-ve-van-hanh-he-thong-dien-Quoc-gia-ngay-"
)

NAMESPACE = "nessie.bronze"
TABLE_NAME = "nessie.bronze.evn"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/151.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8",
}

DATE_RE = re.compile(
    r"ngay-(\d+)"
)

CONTENT_DATE_RE = re.compile(
    r"ngày\s+(\d{1,2})[/-](\d{1,2})[/-](\d{4})"
)


def create_evn_session() -> requests.Session:
    """Create and return a legacy TLS session for EVN."""
    return create_legacy_tls_session(BASE_URL)


def parse_date(day: str, month: str, year: str) -> date | None:
    """Parse day, month, and year into a date object."""
    try:
        return date(int(year), int(month), int(day))
    except ValueError:
        return None


def extract_source_data_date_from_url(url: str) -> date | None:
    """Extract source data date from a URL."""
    match = DATE_RE.search(url)

    if not match:
        return None

    value = match.group(1)

    if len(value) <= 4:
        return None

    year = int(value[-4:])
    day_month = value[:-4]

    if len(day_month) == 4:
        day = int(day_month[:2])
        month = int(day_month[2:])

        return parse_date(str(day), str(month), str(year))

    if len(day_month) == 3:

        result = parse_date(day_month[:2], day_month[2:], str(year))

        if result:
            return result

        return parse_date(day_month[:1], day_month[1:], str(year))

    if len(day_month) == 2:
        return parse_date(day_month[:1], day_month[1:], str(year))

    return None


def extract_source_data_date_from_content(tree) -> date | None:
    """Extract source data date from HTML content."""
    title_text = (tree.xpath("string(//h1)") or tree.xpath("string(//title)"))

    match = CONTENT_DATE_RE.search(title_text)

    if not match:
        return None

    day, month, year = match.groups()

    return parse_date(day, month, year)


def extract_source_data_date(url: str, tree) -> date | None:
    """Extract source data date from either URL or HTML content."""
    return (
        extract_source_data_date_from_url(url)
        or extract_source_data_date_from_content(tree)
    )


def crawl_listing_pages(
    session: requests.Session,
    watermark: date | None,
    run_mode: str,
    start_date: date | None = None,
    end_date: date | None = None,
) -> set[str]:
    """Crawl listing pages to collect article links."""
    links = set()
    page = 1

    while True:

        response = session.get(FIRST_URL, params={"page": page}, headers=HEADERS, timeout=20,)

        response.raise_for_status()

        tree = html.fromstring(response.content)
        page_links = set()
        page_article_count = 0
        reached_boundary = False

        for a_tag in tree.xpath(
            '//div[@id="ContentPlaceHolder1_ctl00_row2_container_col1"]'
            '//a[@href]'
        ):
            url = a_tag.get("href")

            if not url:
                continue

            full_url = urljoin(BASE_URL, url)

            if not full_url.startswith(
                BASE_URL + TARGET_PREFIX
            ):
                continue

            source_data_date = extract_source_data_date_from_url(full_url)

            if source_data_date is None:
                continue

            page_article_count += 1

            if run_mode == "incremental":

                if (watermark is not None and source_data_date <= watermark):
                    logger.info(
                        f"Reached watermark at page {page}: "
                        f"{source_data_date} <= {watermark}"
                    )
                    reached_boundary = True
                    break

                page_links.add(full_url)

            else:
                if source_data_date < start_date:
                    logger.info(
                        f"Reached backfill start date at page {page}: "
                        f"{source_data_date} < {start_date}"
                    )
                    reached_boundary = True
                    break

                if source_data_date <= end_date:
                    page_links.add(full_url)

        links.update(page_links)

        if reached_boundary:
            if run_mode == "incremental":
                logger.info("Reached watermark. Stop pagination.")
            else:
                logger.info(
                    "Reached backfill start date. "
                    "Stop pagination."
                )
            break

        if (run_mode == "backfill" and page_article_count > 0 and not page_links):
            page += 1
            time.sleep(0.5)
            continue

        if page_article_count == 0:
            if page == 1:
                raise RuntimeError(
                    "No article links found on the first page. "
                    "The EVN HTML structure or XPath may have changed."
                )

            logger.info(
                "No article links found. "
                "Stop pagination."
            )
            break

        if not page_links:
            logger.info("No matching links. Stop pagination.")
            break

        page += 1
        time.sleep(0.5)

    return links


def validate_evn_payload(text: str) -> tuple[bool, str | None]:
    """Validate EVN article HTML payload for empty response, maintenance, or captcha."""
    if not text or not text.strip() or len(text.strip()) < 50:
        return False, "PAYLOAD_EMPTY"
    lower_text = text.lower()
    if "cloudflare" in lower_text or "captcha" in lower_text or "just a moment..." in lower_text:
        return False, "CAPTCHA_PAGE"
    if (
        "hệ thống đang bảo trì" in lower_text
        or "đang bảo trì" in lower_text
        or "hệ thống bảo trì" in lower_text
        or "maintenance" in lower_text
    ):
        return False, "MAINTENANCE_PAGE"
    return True, None


def crawl_articles(
    session: requests.Session,
    links: set[str],
    ingestion_timestamp: datetime,
    ingest_date: str,
    batch_id: str,
    watermark: date | None,
    run_mode: str,
    start_date: date | None = None,
    end_date: date | None = None,
) -> tuple[list[dict], list[tuple[str, str]]]:
    """Crawl details of provided article links."""
    records = []
    failed_items: list[tuple[str, str]] = []

    for index, link in enumerate(
        sorted(links),
        start=1
    ):

        try:
            response = session.get(link, headers=HEADERS, timeout=20)

            response.raise_for_status()

            is_valid, error_reason = validate_evn_payload(response.text)
            if not is_valid:
                logger.warning(f"Invalid payload for {link}: {error_reason}")
                failed_items.append((link, error_reason))
                continue

            detail_tree = html.fromstring(response.content)

            source_data_date = extract_source_data_date(link, detail_tree)

            if source_data_date is None:
                logger.warning(
                    f"Warning: could not extract "
                    f"source_data_date: {link}"
                )
                continue

            if run_mode == "incremental":
                if (watermark is not None and source_data_date <= watermark):
                    continue

            else:
                if not (start_date <= source_data_date <= end_date):
                    continue

            records.append(
                {
                    "bronze_key": f"{batch_id}_{index}",
                    "source_name": "evn",
                    "source_url": link,
                    "source_data_date": (
                        source_data_date.isoformat()
                    ),
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

            time.sleep(0.1)

        except requests.RequestException as error:
            logger.error(f"Failed: {link} - {error}")
            failed_items.append((link, f"REQUEST_ERROR: {error}"))

    return records, failed_items


def write_bronze(
    spark,
    records: list[dict],
    ingest_date: str,
    last_successful_data_date: date | None,
    update_watermark_after_write: bool,
):
    """Write records to the bronze layer."""
    write_raw_bronze(
        spark,
        records,
        TABLE_NAME,
        ingest_date,
        last_successful_data_date,
        namespace=NAMESPACE,
        fail_on_empty=True,
        dataset_name="evn",
        update_watermark_after_write=update_watermark_after_write,
    )


def main(
    run_mode: str = "incremental",
    start_date: str | None = None,
    end_date: str | None = None,
):
    """Execute the main EVN crawling and ingestion process."""
    if run_mode not in {"incremental", "backfill"}:
        raise ValueError(
            f"Unsupported run mode: {run_mode}"
        )

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

        watermark = None

        logger.info("Run mode: backfill")
        logger.info(
            f"Backfill date range: "
            f"{crawl_start_date} to {crawl_end_date}"
        )

    else:
        watermark = get_watermark("evn")

        logger.info("Run mode: incremental")
        logger.info(f"Previous watermark: {watermark}")

        crawl_start_date = None
        crawl_end_date = None

    ingestion_timestamp = datetime.now(timezone.utc)

    ingest_date = (ingestion_timestamp.date().isoformat())

    batch_id = ingestion_timestamp.strftime("%Y%m%d%H%M%S")

    start_ingestion_log(
        run_id=batch_id,
        dataset_name="evn",
        run_mode=run_mode,
        start_date=crawl_start_date,
        end_date=crawl_end_date,
        started_at=ingestion_timestamp,
    )

    start_time = time.time()
    status = "SUCCESS"
    error_msg = None
    records_count = 0

    try:
        session = create_evn_session()

        links = crawl_listing_pages(
            session=session,
            watermark=watermark,
            run_mode=run_mode,
            start_date=crawl_start_date,
            end_date=crawl_end_date,
        )

        logger.info(f"Total unique links: {len(links)}")

        records, failed_items = crawl_articles(
            session=session,
            links=links,
            ingestion_timestamp=ingestion_timestamp,
            ingest_date=ingest_date,
            batch_id=batch_id,
            watermark=watermark,
            run_mode=run_mode,
            start_date=crawl_start_date,
            end_date=crawl_end_date,
        )

        records_count = len(records)

        if failed_items:
            error_msg = "; ".join(f"{item} - {reason}" for item, reason in failed_items)
            for item, reason in failed_items:
                logger.error(f"Failed item {item}: {reason}")

        if records_count > 0 and failed_items:
            status = "PARTIAL"
        elif records_count > 0 and not failed_items:
            status = "SUCCESS"
        else:
            if links:
                status = "FAILED"
                logger.warning("No EVN records were successfully crawled from found links. Nothing to write.")
                if not error_msg:
                    error_msg = "No EVN records were crawled."
            else:
                logger.info("No EVN links found for the requested range. Dataset is up to date.")
            return

        successful_dates = [
            date.fromisoformat(record["source_data_date"])
            for record in records
            if record["source_data_date"] is not None
        ]

        if not successful_dates:
            status = "FAILED"
            raise RuntimeError("Records were crawled, but no valid source_data_date was found.")

        last_successful_data_date = max(successful_dates)

        logger.info(
            f"Last successful source data date: "
            f"{last_successful_data_date.isoformat()}"
        )

        spark = None

        try:
            spark = create_spark_session("ingest_evn")

            write_bronze(
                spark=spark,
                records=records,
                ingest_date=ingest_date,
                last_successful_data_date=(
                    last_successful_data_date
                    if run_mode == "incremental"
                    else None
                ),
                update_watermark_after_write=(run_mode == "incremental"),
            )

        finally:
            if spark is not None:
                spark.stop()

    except Exception as exc:
        status = "FAILED"
        error_msg = str(exc)
        raise

    finally:
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
