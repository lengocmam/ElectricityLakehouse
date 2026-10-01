import re
import time
from datetime import date, datetime, timezone
from urllib.parse import urljoin

import requests
from lxml import html

from bronze.bronze_utils import write_raw_bronze
from bronze.watermark import get_watermark
from bronze.http_client import create_legacy_tls_session
from utils.spark import create_spark_session


BASE_URL = "https://www.evn.com.vn"

FIRST_URL = (
    f"{BASE_URL}/vi-VN/news-l/"
    "Thong-tin-tom-tat-van-hanh-HTD-Quoc-gia-60-2015"
)

TARGET_PREFIX = (
    "/d/vi-VN/news/"
    "Thong-tin-chung-ve-van-hanh-he-thong-dien-Quoc-gia-ngay-"
)

# Nessie catalog
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
    return create_legacy_tls_session(BASE_URL)


def parse_date(day: str, month: str, year: str) -> date | None:
    try:
        return date(
            int(year),
            int(month),
            int(day)
        )
    except ValueError:
        return None


def extract_source_data_date_from_url(url: str) -> date | None:

    match = DATE_RE.search(url)

    if not match:
        return None

    value = match.group(1)

    if len(value) <= 4:
        return None

    year = int(value[-4:])
    day_month = value[:-4]

    # Format: DDMMYYYY
    if len(day_month) == 4:
        day = int(day_month[:2])
        month = int(day_month[2:])

        return parse_date(
            str(day),
            str(month),
            str(year)
        )

    # Format: DMMYYYY or DDMYYYY
    if len(day_month) == 3:

        # Try DD + M
        result = parse_date(
            day_month[:2],
            day_month[2:],
            str(year)
        )

        if result:
            return result

        # Try D + MM
        return parse_date(
            day_month[:1],
            day_month[1:],
            str(year)
        )

    # Format: DMMYYYY
    if len(day_month) == 2:
        return parse_date(
            day_month[:1],
            day_month[1:],
            str(year)
        )

    return None


def extract_source_data_date_from_content(tree) -> date | None:
    title_text = (
        tree.xpath("string(//h1)")
        or tree.xpath("string(//title)")
    )

    match = CONTENT_DATE_RE.search(title_text)

    if not match:
        return None

    day, month, year = match.groups()

    return parse_date(day, month, year)


def extract_source_data_date(url: str, tree) -> date | None:

    return (
        extract_source_data_date_from_url(url)
        or extract_source_data_date_from_content(tree)
    )


def crawl_listing_pages(
    session: requests.Session,
    watermark: date | None,
) -> set[str]:
    links = set()
    page = 1

    while True:
        print(f"Scanning page {page}")

        response = session.get(
            FIRST_URL,
            params={"page": page},
            headers=HEADERS,
            timeout=20,
        )

        response.raise_for_status()

        tree = html.fromstring(response.content)
        page_links = set()

        reached_watermark = False

        for a_tag in tree.xpath(
            '//div[@id="ContentPlaceHolder1_ctl00_row2_container_col1"]'
            '//a[@href]'
        ):
            url = a_tag.get("href")

            if not url or not url.startswith(TARGET_PREFIX):
                continue

            full_url = urljoin(BASE_URL, url)

            source_data_date = extract_source_data_date_from_url(
                full_url
            )

            # Gặp bài cũ -> dừng pagination
            if (
                watermark is not None
                and source_data_date is not None
                and source_data_date <= watermark
            ):
                print(
                    f"Reached watermark at page {page}: "
                    f"{source_data_date} <= {watermark}"
                )
                reached_watermark = True
                break

            page_links.add(full_url)

        print(f"Found {len(page_links)} new links")

        links.update(page_links)

        # Đã chạm watermark -> không cần page tiếp
        if reached_watermark:
            print("Reached watermark. Stop pagination.")
            break

        # Không có link nào -> hết pagination
        if not page_links:
            if page == 1:
                raise RuntimeError(
                    "No article links found on the first page. "
                    "The EVN HTML structure or XPath may have changed."
                )

            print("No new links. Stop pagination.")
            break

        page += 1
        time.sleep(0.5)

    return links


def crawl_articles(
    session: requests.Session,
    links: set[str],
    ingestion_timestamp: datetime,
    ingest_date: str,
    batch_id: str,
    watermark: date | None,
) -> list[dict]:

    records = []

    for index, link in enumerate(
        sorted(links),
        start=1
    ):

        try:
            print(
                f"Crawling {index}/{len(links)}: {link}"
            )

            response = session.get(
                link,
                headers=HEADERS,
                timeout=20
            )

            response.raise_for_status()

            detail_tree = html.fromstring(
                response.content
            )

            source_data_date = extract_source_data_date(
                link,
                detail_tree
            )

            if source_data_date is None:
                print(
                    f"Warning: could not extract "
                    f"source_data_date: {link}"
                )
                continue

            if (
                watermark is not None
                and source_data_date <= watermark
            ):
                print(
                    f"Skip old article: "
                    f"{source_data_date} <= {watermark}"
                )
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
            print(f"Failed: {link}")
            print(error)

    return records


def write_bronze(
    spark,
    records: list[dict],
    ingest_date: str,
    last_successful_data_date: date,
):
    write_raw_bronze(
        spark,
        records,
        TABLE_NAME,
        ingest_date,
        last_successful_data_date,
        namespace=NAMESPACE,
        fail_on_empty=True,
        dataset_name="evn",
    )


def main():

    ingestion_timestamp = datetime.now(timezone.utc)

    ingest_date = (
        ingestion_timestamp
        .date()
        .isoformat()
    )

    batch_id = ingestion_timestamp.strftime(
        "%Y%m%d%H%M%S"
    )
    
    watermark = get_watermark("evn")

    print(
        f"Previous watermark: {watermark}"
    )

    session = create_evn_session()

    links = crawl_listing_pages(session, watermark)

    print(
        f"Total unique links: {len(links)}"
    )

    print(
        "Sample links:",
        sorted(links)[:5]
    )

    records = crawl_articles(
        session=session,
        links=links,
        ingestion_timestamp=ingestion_timestamp,
        ingest_date=ingest_date,
        batch_id=batch_id,
        watermark=watermark,
    )

    if not records:
        print("No new EVN data found. Nothing to write.")
        return

    successful_dates = [
        date.fromisoformat(record["source_data_date"])
        for record in records
        if record["source_data_date"] is not None
    ]

    if not successful_dates:
        raise RuntimeError(
            "Records were crawled, but no valid source_data_date was found."
        )

    last_successful_data_date = max(successful_dates)

    print(
        f"Watermark candidate: "
        f"{last_successful_data_date.isoformat()}"
    )

    spark = None

    try:

        spark = create_spark_session(
            "ingest_evn"
        )

        write_bronze(
            spark=spark,
            records=records,
            ingest_date=ingest_date,
            last_successful_data_date=last_successful_data_date,
        )

    finally:

        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
