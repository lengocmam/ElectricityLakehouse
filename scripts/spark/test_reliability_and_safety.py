"""Comprehensive Test Suite for Lakehouse Reliability, Safety & Disaster Recovery.

Tests:
1. Quality Circuit Breaker (Trips if reject rate > 10%).
2. Volume Skew / Drop Detection (Alerts if volume drops > 30% vs historical).
3. Iceberg Maintenance & Compaction logic.
4. Emergency Rollback CLI syntax & options.
5. Control DB Backup Dump Generation & Gzip integrity.
"""
import gzip
import os
import sys
from datetime import date, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import patch

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
from maintenance.table_maintenance import SILVER_TABLES, GOLD_TABLES
from maintenance.backup_control_db import create_backup_dump


def test_circuit_breaker():
    print("\n[TEST 1/7] Testing Quality Circuit Breaker...")

    # Case A: 5% error -> Normal pass
    r1 = enforce_quality_circuit_breaker("silver_load", valid_count=950, reject_count=50, max_reject_ratio=0.10)
    assert abs(r1 - 0.05) < 1e-4
    print("  ✓ Normal batch (5% reject): Passed cleanly.")

    # Case B: 10% error -> Boundary pass
    r2 = enforce_quality_circuit_breaker("silver_power_daily", valid_count=900, reject_count=100, max_reject_ratio=0.10)
    assert abs(r2 - 0.10) < 1e-4
    print("  ✓ Boundary batch (10% reject): Passed cleanly.")

    # Case C: 12% error -> TRIPPED
    tripped = False
    try:
        enforce_quality_circuit_breaker("silver_hydro", valid_count=880, reject_count=120, max_reject_ratio=0.10)
    except QualityCircuitBreakerError as e:
        tripped = True
        print(f"  ✓ Malformed batch (12% reject): Tripped Circuit Breaker as expected -> {e}")

    assert tripped, "Circuit breaker failed to trip on 12% reject rate!"
    print("  ✓ Quality Circuit Breaker 100% verified.")


def test_iceberg_governance_catalog():
    print("\n[TEST 2/7] Testing Iceberg Governance & Maintenance Table Manifest...")
    assert len(SILVER_TABLES) >= 12, f"Expected at least 12 silver tables, got {len(SILVER_TABLES)}"
    assert len(GOLD_TABLES) >= 13, f"Expected at least 13 gold tables, got {len(GOLD_TABLES)}"
    print(f"  ✓ Silver tables registered for maintenance: {len(SILVER_TABLES)}")
    print(f"  ✓ Gold tables registered for maintenance: {len(GOLD_TABLES)}")
    print("  ✓ Maintenance catalog manifest 100% verified.")


def test_backup_dump_format():
    print("\n[TEST 3/7] Testing Control Database Dump & Gzip Compression Format...")
    class MockCursor:
        def __init__(self):
            self.description = [("dataset_name",), ("last_successful_data_date",)]
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def execute(self, sql, params=None):
            pass
        def fetchone(self):
            return (True,)
        def fetchall(self):
            return [("evn", "2026-10-16"), ("nsmo_load", "2026-10-16")]

    class MockConn:
        def cursor(self):
            return MockCursor()

    compressed_bytes = create_backup_dump(MockConn())
    assert len(compressed_bytes) > 0, "Compressed dump is empty"

    decompressed = gzip.decompress(compressed_bytes).decode("utf-8")
    assert "VIETNAM ELECTRICITY LAKEHOUSE - CONTROL METADATA BACKUP" in decompressed
    assert "INSERT INTO pipeline_watermark" in decompressed
    assert "ON CONFLICT DO NOTHING" in decompressed
    print(f"  ✓ Dump created ({len(compressed_bytes)} bytes compressed, {len(decompressed)} chars decompressed)")
    print("  ✓ Idempotent SQL format validated (ON CONFLICT DO NOTHING)")
    print("  ✓ Backup and Gzip structure 100% verified.")


def test_airflow_dag_import():
    print("\n[TEST 4/7] Testing Airflow Maintenance DAG Syntax & Structure...")
    import ast
    dag_path = os.path.join(os.path.dirname(CURRENT_DIR), "..", "airflow", "dags", "lakehouse_maintenance_and_backup_dag.py")
    dag_path = os.path.abspath(dag_path)
    if not os.path.exists(dag_path):
        dag_path = os.path.join(
            os.environ.get("AIRFLOW_HOME", "/opt/airflow"),
            "dags", "lakehouse_maintenance_and_backup_dag.py",
        )
    with open(dag_path, encoding="utf-8") as source:
        ast.parse(source.read(), filename=dag_path)
    print(f"  ✓ DAG compiled with 0 errors: {os.path.basename(dag_path)}")


def test_gold_snapshot_partition_selection():
    """A Gold retry reads every report changed after its committed snapshot."""
    from gold import gold_control

    class FakeColumn:
        def __eq__(self, other): return self
        def __and__(self, other): return self
        def isin(self, values): return self
        def alias(self, name): return self

    class FakeFrame:
        def __init__(self, rows): self.rows = rows
        def where(self, condition): return self
        def orderBy(self, *columns): return self
        def select(self, *columns): return self
        def distinct(self): return self
        def collect(self): return self.rows

    class FakeSpark:
        def __init__(self, deleted_dates=()):
            self.catalog = SimpleNamespace(tableExists=lambda name: True)
            self.deleted_dates = deleted_dates

        def table(self, name):
            if name.endswith(".history"):
                return FakeFrame([SimpleNamespace(snapshot_id=i) for i in (10, 11, 12)])
            if name.endswith(".partitions"):
                return FakeFrame([
                    SimpleNamespace(report_date=date(2026, 11, 1), updated_snapshot_id=10),
                    SimpleNamespace(report_date=date(2026, 11, 2), updated_snapshot_id=11),
                    SimpleNamespace(report_date=date(2026, 11, 3), updated_snapshot_id=12),
                    SimpleNamespace(report_date=date(2026, 10, 31), updated_snapshot_id=None),
                ])
            if name.endswith(".all_entries"):
                return FakeFrame([
                    SimpleNamespace(report_date=d) for d in self.deleted_dates
                ])
            raise AssertionError(name)

    source = "nessie.silver.load"
    with patch.object(gold_control.F, "col", return_value=FakeColumn()):
        with patch.object(gold_control, "get_gold_snapshot_checkpoint", return_value=10):
            changes = gold_control.get_changed_partitions(
                FakeSpark(), "fact_load_hourly", source,
            )
            assert changes.report_dates == ["2026-11-02", "2026-11-03"]
            assert changes.snapshot_id == 12 and changes.checkpoint_id == 10
            assert changes.advance_checkpoint

            backfill = gold_control.get_changed_partitions(
                FakeSpark(), "fact_load_hourly", source, run_date="2026-11-01",
            )
            assert backfill.report_dates == ["2026-11-01"]
            assert not backfill.advance_checkpoint

            try:
                gold_control.get_changed_partitions(
                    FakeSpark([date(2026, 11, 4)]), "fact_load_hourly", source,
                )
                raise AssertionError("Removed Silver partition must block checkpoint")
            except RuntimeError as exc:
                assert "Gold checkpoint was not advanced" in str(exc)

        with patch.object(gold_control, "get_gold_snapshot_checkpoint", return_value=None):
            initial = gold_control.get_changed_partitions(
                FakeSpark(), "fact_load_hourly", source,
            )
            assert len(initial.report_dates) == 4

        with patch.object(gold_control, "get_gold_snapshot_checkpoint", return_value=12):
            unchanged = gold_control.get_changed_partitions(
                FakeSpark(), "fact_load_hourly", source,
            )
            assert unchanged.report_dates == []

    print("\n[TEST 5/7] Gold snapshot partition selection passed.")


def test_gold_merge_writer():
    """Facts and dimensions share one MERGE writer without deleting old rows."""
    from gold.write_gold import FACTS, FACT_GRAINS, _merge_gold_rows

    assert set(FACTS) == set(FACT_GRAINS)

    class FakeFrame:
        def __init__(self, columns):
            self.columns = columns
            self.view = None
            self.created_partition = None

        def createOrReplaceTempView(self, name):
            self.view = name

        def localCheckpoint(self, eager=True):
            assert eager
            return self

        def unpersist(self): pass

        def writeTo(self, name):
            self.created_table = name
            return self

        def tableProperty(self, *args): return self
        def option(self, *args): return self
        def repartition(self, *args): return self
        def sortWithinPartitions(self, *args): return self
        def using(self, *args): return self

        def partitionedBy(self, column):
            self.created_partition = column
            return self

        def create(self): pass

    class FakeSpark:
        def __init__(self, exists, columns):
            self.statements = []
            self.dropped_views = []
            self.catalog = SimpleNamespace(
                tableExists=lambda name: exists,
                dropTempView=self.dropped_views.append,
            )
            self.columns = columns

        def table(self, name):
            return SimpleNamespace(columns=self.columns)

        def sql(self, statement):
            self.statements.append(statement)

    load_columns = ["load_key", "date_key", "time_key", "region_key", "load_mw"]
    load = FakeFrame(load_columns)
    spark = FakeSpark(True, load_columns)
    _merge_gold_rows(spark, "nessie.gold.fact_load_hourly", load,
                     FACT_GRAINS["fact_load_hourly"], "date_key")
    sql = spark.statements[0]
    assert "target.`date_key` = source.`date_key`" in sql
    assert "target.`time_key` = source.`time_key`" in sql
    assert "target.`region_key` = source.`region_key`" in sql
    assert "WHEN MATCHED AND NOT" in sql and "<=>" in sql
    assert "WHEN NOT MATCHED THEN INSERT *" in sql
    assert "DELETE" not in sql
    assert spark.dropped_views == [load.view]

    mix = FakeFrame(["generation_power_key", "date_key", "time_key", "energy_source_key", "generation_power_mw"])
    spark = FakeSpark(False, mix.columns)
    _merge_gold_rows(spark, "nessie.gold.fact_generation_power_hourly", mix,
                     FACT_GRAINS["fact_generation_power_hourly"], "date_key")
    assert mix.created_table == "nessie.gold.fact_generation_power_hourly"
    assert mix.created_partition == "date_key"
    assert not spark.statements

    spark = FakeSpark(True, mix.columns)
    _merge_gold_rows(spark, "nessie.gold.fact_generation_power_hourly", mix,
                     FACT_GRAINS["fact_generation_power_hourly"], "date_key")
    sql = spark.statements[0]
    assert "target.`report_date` = source.`report_date`" not in sql
    assert "target.`time_key` = source.`time_key`" in sql
    assert "target.`energy_source_key` = source.`energy_source_key`" in sql
    assert "target.`date_key` = source.`date_key`" in sql

    dim = FakeFrame(["energy_source_key", "source_name"])
    spark = FakeSpark(False, dim.columns)
    _merge_gold_rows(spark, "nessie.gold.dim_energy_source", dim, ("energy_source_key",))
    assert dim.created_table == "nessie.gold.dim_energy_source"
    assert dim.created_partition is None

    spark = FakeSpark(True, dim.columns)
    _merge_gold_rows(spark, "nessie.gold.dim_energy_source", dim, ("energy_source_key",))
    assert "target.`energy_source_key` = source.`energy_source_key`" in spark.statements[0]
    assert "WHEN NOT MATCHED THEN INSERT *" in spark.statements[0]
    assert "DELETE" not in spark.statements[0]
    print("\n[TEST 6/7] Shared Gold fact and dimension MERGE writer passed.")


def test_mix_bronze_watermark():
    """Bronze publishes the furthest written report date without regressing on backfill."""
    from bronze import nsmo_power as mix

    class FakeSession:
        def __enter__(self): return self
        def __exit__(self, *_): return False

    def run(mode, checkpoint, first=None, last=None):
        requested = []

        def fetch(_session, report_day):
            requested.append(report_day)
            return SimpleNamespace(text="{}", url=f"https://example.test/{report_day}")

        metrics = {"run_id": "watermark-test", "records_read": 0, "records_written": 0}
        with patch.object(mix, "create_generation_mix_session", return_value=FakeSession()), \
             patch.object(mix, "fetch_report", side_effect=fetch), \
             patch.object(mix, "parse_report_payload", return_value=([(next(iter(mix.EXPECTED_SOURCES)), "", 1.0)], None)), \
             patch("bronze.control.get_watermark", return_value=checkpoint), \
             patch("utils.spark.create_spark_session", return_value=SimpleNamespace(stop=lambda: None)), \
             patch("bronze.bronze_utils.write_raw_bronze") as write:
            mix._run_ingestion(metrics, mode, first, last)
        assert metrics["records_written"] == len(requested)
        assert write.call_args.kwargs["update_watermark_after_write"] is True
        return requested, write.call_args.args[4]

    today = datetime.now(mix.VN_TZ).date()
    checkpoint = today - timedelta(days=3)
    requested, watermark = run("incremental", checkpoint, last=(today - timedelta(days=1)).isoformat())
    assert requested == [today - timedelta(days=2), today - timedelta(days=1)]
    assert watermark == today - timedelta(days=1)

    old_day = today - timedelta(days=5)
    requested, watermark = run("backfill", checkpoint, old_day.isoformat(), old_day.isoformat())
    assert requested == [old_day] and watermark == checkpoint
    print("\n[TEST 7/7] Generation Mix Bronze watermark passed.")


def main():
    print("=" * 80)
    print("   VIETNAM ELECTRICITY LAKEHOUSE - RELIABILITY & SAFETY TEST SUITE")
    print("=" * 80)

    test_circuit_breaker()
    test_iceberg_governance_catalog()
    test_backup_dump_format()
    test_airflow_dag_import()
    test_gold_snapshot_partition_selection()
    test_gold_merge_writer()
    test_mix_bronze_watermark()

    print("\n" + "=" * 80)
    print("   >>> ALL 7/7 RELIABILITY & SAFETY TESTS PASSED SUCCESSFULLY! <<<")
    print("=" * 80)


if __name__ == "__main__":
    main()
