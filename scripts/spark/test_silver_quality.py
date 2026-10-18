"""Unit test for Quality Circuit Breaker and Volume Skew Detection."""
import os
import sys

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


if __name__ == "__main__":
    test_circuit_breaker()
    print("\n>>> ALL QUALITY CONTROL UNIT TESTS PASSED! <<<")
