"""Raw NSMO BCSX generation mix reports (one Bronze row per report request)."""

from __future__ import annotations

import hashlib
import json
import math
import os
import random
import re
import ssl
import time
from datetime import date, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from uuid import uuid4
from zoneinfo import ZoneInfo

from utils.logging import create_logger

logger = create_logger(__name__)
DATASET = "nsmo_power"
TABLE = "nessie.bronze.nsmo_power"
URL = "https://www.nsmo.vn/api/services/app/Dashboard/GetTongHopCongSuatBcsxTheoLoaiHinh"
# NSMO currently sends the leaf certificate without its Sectigo intermediate.
# Fetch only this CA certificate, pin its bytes, then keep normal hostname and
# trust-chain verification. Fail closed if Sectigo rotates the intermediate.
INTERMEDIATE_URL = "http://crt.sectigo.com/SectigoPublicServerAuthenticationCADVR36.crt"
INTERMEDIATE_SHA256 = "8c54c334b66ba4e426772af4a3f9136c19a1aec729fdb28c535c07a5a4ef22e0"
VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")
EXPECTED_SOURCES = frozenset({
    "Thủy điện", "Nhiệt điện than", "Tuabin khí (DO)", "Tuabin khí (GAS)",
    "Nhiệt điện dầu DO", "Nhiệt điện dầu FO", "Sinh khối", "Gió",
    "Mặt trời farm", "ĐMTMN (ĐC)", "Mua TQ", "Nhiệt điện khác",
})
_STAMP = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.0+)?$")


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"DUPLICATE_JSON_KEY:{key}")
        result[key] = value
    return result


def parse_report_payload(raw: str):
    """Return (source, timestamp, value) rows and any parsing issue.

    Shared by Bronze and the Silver extractor; row quality is checked in Silver.
    """
    try:
        payload = json.loads(raw, object_pairs_hook=_unique_object)
    except (TypeError, ValueError) as exc:
        return [], f"INVALID_JSON:{exc}"
    if not isinstance(payload, dict) or payload.get("success") is False:
        return [], "API_ENVELOPE_ERROR"
    result = payload.get("result")
    if not isinstance(result, dict) or result.get("status") is not True:
        return [], "API_STATUS_INVALID"
    data = result.get("data")
    if not isinstance(data, list) or not data:
        return [], "REPORT_DATA_INVALID"
    rows = []
    for item in data:
        if not isinstance(item, dict):
            return [], "SOURCE_RECORD_INVALID"
        source = item.get("tenDuLieu")
        values = item.get("congSuat")
        if not isinstance(source, str) or not source.strip() or not isinstance(values, dict):
            return [], "SOURCE_SCHEMA_INVALID"
        for timestamp, value in values.items():
            if not _STAMP.fullmatch(timestamp):
                return [], "TIMESTAMP_FORMAT_INVALID"
            try:
                parsed = datetime.fromisoformat(timestamp)
            except ValueError:
                return [], "TIMESTAMP_INVALID"
            if parsed.tzinfo is not None or parsed.second or parsed.microsecond:
                return [], "TIMESTAMP_FORMAT_INVALID"
            if isinstance(value, bool) or not isinstance(value, (float, int)):
                return [], "POWER_NOT_NUMBER"
            try:
                finite = math.isfinite(value)
            except (ValueError, OverflowError):
                finite = False
            if not finite:
                return [], "POWER_NOT_FINITE"
            rows.append((source, timestamp, float(value)))
    return rows, None


def create_generation_mix_session() -> requests.Session:
    """Verify NSMO TLS with its pinned, otherwise omitted intermediate CA."""
    import certifi
    import requests
    from requests.adapters import HTTPAdapter

    response = requests.get(INTERMEDIATE_URL, timeout=(5, 15))
    response.raise_for_status()
    intermediate = response.content
    if hashlib.sha256(intermediate).hexdigest() != INTERMEDIATE_SHA256:
        raise RuntimeError("NSMO intermediate CA fingerprint changed")
    context = ssl.create_default_context(cafile=certifi.where())
    context.load_verify_locations(cadata=ssl.DER_cert_to_PEM_cert(intermediate))

    class NsmoVerifiedAdapter(HTTPAdapter):
        def init_poolmanager(self, *args, **kwargs):
            return super().init_poolmanager(*args, ssl_context=context, **kwargs)

    session = requests.Session()
    session.mount("https://www.nsmo.vn/", NsmoVerifiedAdapter())
    return session


def fetch_report(session: requests.Session, report_date: date) -> requests.Response:
    """Bounded transient retry; 4xx other than 429 is terminal."""
    import requests

    for attempt in range(3):
        try:
            response = session.get(URL, params={"ngay": report_date.isoformat()}, timeout=(10, 30))
            if response.status_code == 429 or response.status_code >= 500:
                if attempt == 2:
                    response.raise_for_status()
                retry_after = response.headers.get("Retry-After")
                wait = min(2 ** attempt, 8)
                if retry_after:
                    try:
                        wait = float(retry_after)
                    except ValueError:
                        try:
                            wait = max(0.0, (parsedate_to_datetime(retry_after) -
                                             datetime.now(timezone.utc)).total_seconds())
                        except (TypeError, ValueError):
                            raise RuntimeError("INVALID_RETRY_AFTER")
                if wait > 30:
                    raise RuntimeError(f"RETRY_AFTER_OUTSIDE_BUDGET:{wait}")
                time.sleep(wait + random.uniform(0, 0.25))
                continue
            response.raise_for_status()
            return response
        except (requests.Timeout, requests.ConnectionError):
            if attempt == 2:
                raise
            time.sleep(min(2 ** attempt, 8) + random.uniform(0, 0.25))
    raise RuntimeError("HTTP_RETRY_EXHAUSTED")


def _run_ingestion(metrics, run_mode="incremental", start_date=None, end_date=None):
    import requests

    from bronze.bronze_utils import write_raw_bronze
    from bronze.control import get_watermark
    from utils.spark import create_spark_session
    if run_mode not in {"incremental", "backfill"}:
        raise ValueError("Unsupported run mode")
    checkpoint = get_watermark(DATASET)
    today = datetime.now(VN_TZ).date()
    if run_mode == "backfill":
        if not start_date or not end_date:
            raise ValueError("Backfill requires both dates")
        first, last = date.fromisoformat(start_date), date.fromisoformat(end_date)
    else:
        initial = date.fromisoformat(os.environ.get("NSMO_MIX_START_DATE", "2026-11-04"))
        first = checkpoint + timedelta(days=1) if checkpoint else initial
        last = date.fromisoformat(end_date) if end_date else today - timedelta(days=1)
    if first > last or last >= today:
        if first > last and run_mode == "incremental":
            logger.info("NO_DATA: no report date due")
            return "NO_DATA", first, last
        raise ValueError("Invalid or future report date range")
    batch_id = metrics["run_id"]
    now = datetime.now(timezone.utc)
    records = []
    failures = []
    with create_generation_mix_session() as session:
        current = first
        while current <= last:
            try:
                response = fetch_report(session, current)
                metrics["records_read"] += 1
                raw = response.text
                rows, issue = parse_report_payload(raw)
                if issue:
                    failures.append(f"{current}:{issue}")
                else:
                    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()
                    records.append({
                        "bronze_key": hashlib.sha256(f"{DATASET}:{current}:{batch_id}:{digest}".encode()).hexdigest(),
                        "source_name": "nsmo", "source_url": response.url,
                        "source_data_date": current.isoformat(),
                        "source_data_start_date": current.isoformat(),
                        "source_data_end_date": current.isoformat(),
                        "batch_id": batch_id, "ingestion_timestamp": now.isoformat(),
                        "ingest_date": now.date().isoformat(), "raw": raw,
                    })
                    new_sources = sorted({source for source, _, _ in rows} - EXPECTED_SOURCES)
                    if new_sources:
                        logger.warning("NEW_SOURCE: Report %s has new sources: %s", current, new_sources)
            except (requests.RequestException, RuntimeError) as exc:
                failures.append(f"{current}:{exc}")
            current += timedelta(days=1)
    spark = None
    try:
        if records:
            latest_written_date = max(date.fromisoformat(r["source_data_date"]) for r in records)
            watermark_date = max(checkpoint, latest_written_date) if checkpoint else latest_written_date
            spark = create_spark_session("ingest_nsmo_power")
            write_raw_bronze(spark, records, TABLE, now.date().isoformat(), watermark_date,
                             namespace="nessie.bronze", dataset_name=DATASET,
                             update_watermark_after_write=True)
            metrics["records_written"] = len(records)
    finally:
        if spark is not None:
            spark.stop()
    if failures:
        raise RuntimeError("Generation mix ingestion incomplete: " + "; ".join(failures))
    logger.info("Fetched %d generation mix reports; Bronze watermark updated after write", len(records))
    return "SUCCESS", first, last


def main(run_mode="incremental", start_date=None, end_date=None):
    from utils.audit import record_task_metrics
    metrics = {"run_id": uuid4().hex, "records_read": 0, "records_written": 0}
    state, first, last = _run_ingestion(metrics, run_mode, start_date, end_date)
    record_task_metrics(
        dataset_name=DATASET, records_read=metrics["records_read"],
        records_written=metrics["records_written"], records_rejected=0,
        mode=run_mode, start_date=first, end_date=last,
        status="NO_DATA" if state == "NO_DATA" else None,
    )


if __name__ == "__main__":
    main()
