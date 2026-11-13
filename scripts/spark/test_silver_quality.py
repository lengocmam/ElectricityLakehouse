"""Unit test for Quality Circuit Breaker and Volume Skew Detection."""
import os
import sys
import json
from datetime import date, datetime, timedelta

# Ensure UTF-8 output on Windows
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

# Point PySpark to active Python executable
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

from silver.quality_control import (
    enforce_quality_circuit_breaker,
    detect_volume_skew,
    QualityCircuitBreakerError,
)


def test_circuit_breaker():
    print("[TEST 1/2] Testing Quality Circuit Breaker...")

    # Case 1: Healthy batch (5% reject) -> Should PASS
    rate = enforce_quality_circuit_breaker("test_dataset", valid_count=95, reject_count=5, max_reject_ratio=0.10)
    assert abs(rate - 0.05) < 1e-4, f"Expected 0.05, got {rate}"
    print("  ✓ Healthy batch (5% reject): PASSED")

    # Case 2: Exact boundary (10% reject) -> Should PASS
    rate = enforce_quality_circuit_breaker("test_dataset", valid_count=90, reject_count=10, max_reject_ratio=0.10)
    assert abs(rate - 0.10) < 1e-4, f"Expected 0.10, got {rate}"
    print("  ✓ Boundary batch (10% reject): PASSED")

    # Case 3: Breached batch (15% reject) -> Should RAISE QualityCircuitBreakerError
    tripped = False
    try:
        enforce_quality_circuit_breaker("test_dataset", valid_count=85, reject_count=15, max_reject_ratio=0.10)
    except QualityCircuitBreakerError as e:
        tripped = True
        print(f"  ✓ Breached batch (15% reject): TRIPPED AS EXPECTED -> {e}")

    assert tripped, "Circuit breaker failed to trip on 15% reject rate!"
    print("  ✓ Circuit Breaker Test 100% SUCCESS")


def test_nsmo_power_parser():
    from bronze.nsmo_power import EXPECTED_SOURCES, parse_report_payload
    day = date(2026, 11, 5)
    first = datetime.combine(day, datetime.min.time()) + timedelta(minutes=30)
    stamps = [(first + timedelta(minutes=30 * i)).isoformat(timespec="seconds") for i in range(48)]
    payload = {"success": True, "result": {"status": True, "data": [
        {"tenDuLieu": name, "congSuat": {stamp: float(i) for i, stamp in enumerate(stamps)}}
        for name in sorted(EXPECTED_SOURCES)
    ]}}
    raw = json.dumps(payload, ensure_ascii=False)
    rows, issue = parse_report_payload(raw)
    assert issue is None and len(rows) == 576
    assert len({(source, stamp) for source, stamp, _ in rows}) == 576
    assert parse_report_payload(raw.replace('"success": true', '"success": true, "success": true', 1))[1].startswith("INVALID_JSON")
    payload["result"]["data"][0]["congSuat"].pop(stamps[0])
    partial_rows, issue = parse_report_payload(json.dumps(payload, ensure_ascii=False))
    assert issue is None and len(partial_rows) == 575
    payload["result"]["data"][0]["congSuat"][stamps[0]] = 0.0
    for item in payload["result"]["data"]:
        item["congSuat"] = {stamp: 0.0 for stamp in stamps}
    zero_rows, issue = parse_report_payload(json.dumps(payload, ensure_ascii=False))
    assert issue is None and len(zero_rows) == 576
    payload["result"]["data"][0]["congSuat"][stamps[0]] = -0.5
    assert parse_report_payload(json.dumps(payload, ensure_ascii=False))[1] is None


def test_nsmo_power_contract_and_grain():
    from bronze.nsmo_power import EXPECTED_SOURCES, parse_report_payload
    from silver.extract_power_hourly import extract_power_hourly
    from silver.clean_power_hourly import clean_power_hourly
    from gold.fact_generation_power_hourly import transform_fact_generation_power_hourly
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    report_date = date(2026, 11, 5)
    first = datetime.combine(report_date, datetime.min.time()) + timedelta(minutes=30)
    stamps = [(first + timedelta(minutes=30 * i)).isoformat(timespec="seconds") for i in range(48)]
    assert len(stamps) == 48
    payload = {"success": True, "result": {"status": True, "data": [
        {"tenDuLieu": name, "congSuat": {stamp: float(i) for i, stamp in enumerate(stamps)}}
        for name in sorted(EXPECTED_SOURCES)
    ]}}
    raw = json.dumps(payload, ensure_ascii=False)
    rows, issue = parse_report_payload(raw)
    assert issue is None and len(rows) == 576
    assert parse_report_payload(raw.replace('"success": true', '"success": true, "success": true', 1))[1].startswith("INVALID_JSON")
    payload["result"]["data"][0]["congSuat"].pop(stamps[0])
    partial_rows, issue = parse_report_payload(json.dumps(payload, ensure_ascii=False))
    assert issue is None and len(partial_rows) == 575
    payload["result"]["data"][0]["congSuat"][stamps[0]] = 0.0

    spark = SparkSession.builder.master("local[1]").appName("nsmo_mix_test").getOrCreate()
    try:
        bronze = spark.createDataFrame(
            [("bronze-test", report_date.isoformat(), raw)],
            ["bronze_key", "source_data_date", "raw"],
        )
        valid, rejected = clean_power_hourly(extract_power_hourly(bronze))
        assert rejected.count() == 0
        assert valid.count() == 576
        assert valid.where(F.col("data_date") == F.lit(report_date + timedelta(days=1))).count() == 12
        fact = transform_fact_generation_power_hourly(valid)
        assert fact.count() == 288
        assert fact.select("energy_source_key").distinct().count() == 12
        boundary = fact.where((F.col("date_key") == 20261106) & (F.col("time_key") == 0))
        assert boundary.count() == 12
        assert boundary.select("date_key", "time_key").first() == (20261106, 0)
        assert {row.date_key for row in fact.select("date_key").distinct().collect()} == {
            20261105, 20261106
        }
        assert fact.where((F.col("date_key") == 20261105) & (F.col("time_key") == 1)).select(
            "generation_power_mw").first()[0] == 1.0

        payload["result"]["data"][0]["congSuat"][stamps[1]] = -0.5
        negative_bronze = spark.createDataFrame(
            [("bronze-negative", report_date.isoformat(), json.dumps(payload, ensure_ascii=False))],
            ["bronze_key", "source_data_date", "raw"],
        )
        negative_valid, negative_reject = clean_power_hourly(
            extract_power_hourly(negative_bronze)
        )
        assert negative_valid.count() == 575
        assert negative_reject.count() == 1
        assert negative_reject.select("dq_error_code").first()[0] == "POWER_NEGATIVE"
        assert negative_reject.select("dq_warning_code").first()[0] is None
    finally:
        spark.stop()


def test_nsmo_load_half_hour_silver_exact_hour_gold():
    from silver.extract_nsmo_load import extract_nsmo_load
    from silver.clean_load import clean_load
    from gold.fact_load_hourly import transform_fact_load_hourly
    from gold.dim_region import generate_dim_region
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = SparkSession.builder.master("local[1]").appName("nsmo_load_test").getOrCreate()
    try:
        readings = [
            ("2026-11-05T00:30:00", 900.0),
            ("2026-11-05T01:00:00", 100.0),
            ("2026-11-06T00:00:00", 200.0),
        ]
        raw = json.dumps({"result": {"data": {"phuTais": [
            {"thoiGian": stamp, "congSuatMB": value, "congSuatMT": value,
             "congSuatMN": value, "congSuatHT": value}
            for stamp, value in readings
        ]}}})
        bronze = spark.createDataFrame([("load-bronze", "2026-11-05", raw)],
                                       ["bronze_key", "source_data_date", "raw"])
        valid, reject = clean_load(extract_nsmo_load(bronze))
        assert reject.count() == 0
        assert valid.count() == 12
        gold = transform_fact_load_hourly(valid, generate_dim_region(spark))
        assert gold.count() == 8
        assert gold.where(F.col("load_mw") == 900.0).count() == 0
        assert gold.where(F.col("date_key") == 20261106).count() == 4
    finally:
        spark.stop()


if __name__ == "__main__":
    test_circuit_breaker()
    test_nsmo_power_parser()
    test_nsmo_power_contract_and_grain()
    test_nsmo_load_half_hour_silver_exact_hour_gold()
    print("\n>>> ALL QUALITY CONTROL UNIT TESTS PASSED! <<<")
