#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
load_dispatcher_trino_data.py
Nạp dữ liệu hỗ trợ Điều độ viên vào Trino Iceberg Gold:
  1. iceberg.gold.fact_dispatcher_forecast_7d (Dự báo 7 ngày, dải bất định, cảnh báo nhiệt)
  2. iceberg.gold.fact_dispatcher_monitoring_30d (Giám sát 30 ngày, sai số, rolling MAPE)
  3. iceberg.gold.fact_dispatcher_what_if_sensitivity (Độ nhạy nhiệt độ & 4 kịch bản NWP)
  4. View ngữ nghĩa: iceberg.gold.v_dashboard3_dispatcher_operational
"""

import os
import sys
import json
import urllib.request
import numpy as np
import pandas as pd

TRINO_URL = "http://localhost:8082/v1/statement"

def execute_trino(sql: str):
    sql_clean = sql.strip().rstrip(";")
    req = urllib.request.Request(
        TRINO_URL,
        data=sql_clean.encode("utf-8"),
        headers={
            "X-Trino-User": "trino",
            "X-Trino-Catalog": "iceberg",
            "X-Trino-Schema": "gold",
        },
    )
    resp = urllib.request.urlopen(req)
    data = json.loads(resp.read().decode("utf-8"))
    rows = []
    while "nextUri" in data:
        if "data" in data:
            rows.extend(data["data"])
        if "error" in data:
            raise RuntimeError(f"Trino Error: {data['error']['message']}")
        req2 = urllib.request.Request(data["nextUri"], headers={"X-Trino-User": "trino"})
        data = json.loads(urllib.request.urlopen(req2).read().decode("utf-8"))
    if "data" in data:
        rows.extend(data["data"])
    if "error" in data:
        raise RuntimeError(f"Trino Error: {data['error']['message']}")
    return rows


def main():
    print("=" * 80)
    print("NẠP DỮ LIỆU ĐIỀU ĐỘ VIÊN VÀO TRINO ICEBERG GOLD")
    print("=" * 80)

    json_path = os.path.join("scripts", "dispatcher_operational_data.json")
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # 1. Bảng fact_dispatcher_forecast_7d
    print("[+] Tạo bảng iceberg.gold.fact_dispatcher_forecast_7d...")
    execute_trino("DROP TABLE IF EXISTS iceberg.gold.fact_dispatcher_forecast_7d")
    ddl1 = """
    CREATE TABLE iceberg.gold.fact_dispatcher_forecast_7d (
        horizon_step INTEGER,
        horizon_name VARCHAR,
        forecast_date DATE,
        day_name VARCHAR,
        pred_load_mw DOUBLE,
        ci_lower_mw DOUBLE,
        ci_upper_mw DOUBLE,
        uncertainty_band_mw DOUBLE,
        expected_mape_pct DOUBLE,
        expected_mae_mw DOUBLE,
        is_weekend BOOLEAN
    ) WITH (format = 'PARQUET')
    """
    execute_trino(ddl1)

    h_vals = []
    for h in data["zone1_forecast"]["horizon_days"]:
        val = (
            f"({h['horizon_step']}, '{h['horizon_name']}', DATE '{h['date']}', '{h['day_name']}', "
            f"{h['pred_mw']}, {h['ci_lower_mw']}, {h['ci_upper_mw']}, {h['uncertainty_band_mw']}, "
            f"{h['expected_mape_pct']}, {h['expected_mae_mw']}, {str(h['is_weekend']).lower()})"
        )
        h_vals.append(val)
    ins1 = "INSERT INTO iceberg.gold.fact_dispatcher_forecast_7d VALUES\n" + ",\n".join(h_vals)
    execute_trino(ins1)
    print(f"[✓] Đã nạp {len(h_vals)} ngày dự báo vào fact_dispatcher_forecast_7d!")

    # 2. Bảng fact_dispatcher_monitoring_30d
    print("[+] Tạo bảng iceberg.gold.fact_dispatcher_monitoring_30d...")
    execute_trino("DROP TABLE IF EXISTS iceberg.gold.fact_dispatcher_monitoring_30d")
    ddl2 = """
    CREATE TABLE iceberg.gold.fact_dispatcher_monitoring_30d (
        record_date DATE,
        actual_load_mw DOUBLE,
        pred_load_mw DOUBLE,
        error_mw DOUBLE,
        abs_error_mw DOUBLE,
        mape_pct DOUBLE,
        rolling_mape_7d DOUBLE,
        temp_c DOUBLE,
        alarm_flag VARCHAR
    ) WITH (format = 'PARQUET')
    """
    execute_trino(ddl2)

    m_vals = []
    for s in data["zone4_monitoring"]["series_30d"]:
        abs_err = abs(s["error_mw"])
        flag = "ĐẠT CHUẨN A+" if s["mape_pct"] <= 3.0 else ("CẢNH BÁO" if s["mape_pct"] <= 4.5 else "LỆCH PHÂN PHỐI")
        val = (
            f"(DATE '{s['date']}', {s['actual_mw']}, {s['pred_mw']}, {s['error_mw']}, "
            f"{abs_err}, {s['mape_pct']}, {s['rolling_mape_7d']}, {s['temp_c']}, '{flag}')"
        )
        m_vals.append(val)
    ins2 = "INSERT INTO iceberg.gold.fact_dispatcher_monitoring_30d VALUES\n" + ",\n".join(m_vals)
    execute_trino(ins2)
    print(f"[✓] Đã nạp {len(m_vals)} ngày giám sát vào fact_dispatcher_monitoring_30d!")

    # 3. Bảng fact_dispatcher_nwp_stress_test
    print("[+] Tạo bảng iceberg.gold.fact_dispatcher_nwp_stress_test...")
    execute_trino("DROP TABLE IF EXISTS iceberg.gold.fact_dispatcher_nwp_stress_test")
    ddl3 = """
    CREATE TABLE iceberg.gold.fact_dispatcher_nwp_stress_test (
        scenario_code VARCHAR,
        scenario_name VARCHAR,
        temp_error VARCHAR,
        radiation_error VARCHAR,
        mae_mw DOUBLE,
        mape_pct DOUBLE,
        delta_mape_pt DOUBLE,
        operational_status VARCHAR,
        badge VARCHAR
    ) WITH (format = 'PARQUET')
    """
    execute_trino(ddl3)

    nwp_vals = []
    for n in data["zone3_what_if"]["nwp_stress_scenarios"]:
        val = (
            f"('{n['scenario_code']}', '{n['name']}', '{n['temp_error']}', '{n['radiation_error']}', "
            f"{n['mae_mw']}, {n['mape_pct']}, {n['delta_mape_pt']}, '{n['status']}', '{n['badge']}')"
        )
        nwp_vals.append(val)
    ins3 = "INSERT INTO iceberg.gold.fact_dispatcher_nwp_stress_test VALUES\n" + ",\n".join(nwp_vals)
    execute_trino(ins3)
    print(f"[✓] Đã nạp {len(nwp_vals)} kịch bản stress-test vào fact_dispatcher_nwp_stress_test!")

    # 4. View Ngữ Nghĩa Tổng Hợp Dashboard 3
    print("[+] Tạo View Ngữ Nghĩa iceberg.gold.v_dashboard3_dispatcher_operational...")
    v_sql = """
    CREATE OR REPLACE VIEW iceberg.gold.v_dashboard3_dispatcher_operational AS
    SELECT 
        f.horizon_step,
        f.horizon_name,
        f.forecast_date,
        f.day_name,
        f.pred_load_mw,
        f.ci_lower_mw,
        f.ci_upper_mw,
        f.expected_mape_pct,
        43609.5 AS historical_peak_record_mw,
        ROUND((f.pred_load_mw / 43609.5) * 100.0, 1) AS pct_of_historical_peak,
        CASE 
            WHEN f.pred_load_mw >= 43609.5 THEN 'BÁO ĐỘNG ĐỎ: VƯỢT ĐỈNH LỊCH SỬ'
            WHEN f.pred_load_mw >= 40000.0 THEN 'CẢNH BÁO VÀNG: TIỆM CẬN ĐỈNH'
            ELSE 'AN TOÀN: TRONG GIỚI HẠN VẬN HÀNH'
        END AS dispatch_risk_alert
    FROM iceberg.gold.fact_dispatcher_forecast_7d f
    """
    execute_trino(v_sql)
    print("[✓] Đã tạo thành công View iceberg.gold.v_dashboard3_dispatcher_operational!")
    print("=" * 80)

if __name__ == "__main__":
    main()
