#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scripts/ml/sync_gold_data.py
Đồng bộ dữ liệu mới nhất từ tầng Gold (Trino Lakehouse) vào unified_daily_v2.csv:
- Tự động truy vấn MAX date từ iceberg.gold.v_load_national_hourly và iceberg.gold.fact_weather_daily
- Lấy toàn bộ giá trị thực tế phụ tải (mean, peak, min, vùng miền) và thời tiết (nhiệt độ, bức xạ, mưa, mây, gió)
- Cập nhật vào unified_daily_v2.csv để mô hình học máy luôn được huấn luyện & dự báo đến ngày max trong tầng Gold.
"""

import os
import sys
import json
import urllib.request
import pandas as pd
import numpy as np

TRINO_URL = "http://localhost:8082/v1/statement"
CSV_PATH = "s4_3thuat_toan/code/unified_daily_v2.csv"


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
    columns = []
    while "nextUri" in data:
        if "columns" in data and not columns:
            columns = [c["name"] for c in data["columns"]]
        if "data" in data:
            rows.extend(data["data"])
        if "error" in data:
            raise RuntimeError(f"Trino Error: {data['error']['message']}")
        req2 = urllib.request.Request(data["nextUri"], headers={"X-Trino-User": "trino"})
        data = json.loads(urllib.request.urlopen(req2).read().decode("utf-8"))
    if "columns" in data and not columns:
        columns = [c["name"] for c in data["columns"]]
    if "data" in data:
        rows.extend(data["data"])
    if "error" in data:
        raise RuntimeError(f"Trino Error: {data['error']['message']}")
    return pd.DataFrame(rows, columns=columns)


def main():
    print("=" * 80)
    print("ĐỒNG BỘ DỮ LIỆU TỪ TẦNG GOLD (TRINO ICEBERG) VÀO UNIFIED DATASET")
    print("=" * 80)

    # 1. Truy vấn ngày max tầng Gold
    sql_max = """
    SELECT 
        CAST(MAX(l.date_value) AS VARCHAR) as max_load_date,
        CAST(MAX(d.full_date) AS VARCHAR) as max_weather_date
    FROM (
        SELECT date_value FROM iceberg.gold.v_load_national_hourly
    ) l
    CROSS JOIN (
        SELECT d.full_date 
        FROM iceberg.gold.fact_weather_daily f 
        JOIN iceberg.gold.dim_date d ON f.date_key = d.date_key
    ) d
    """
    df_max = execute_trino(sql_max)
    max_load_date = df_max.iloc[0]["max_load_date"]
    max_weather_date = df_max.iloc[0]["max_weather_date"]
    print(f"[+] Ngày max trong tầng Gold: Load={max_load_date} | Weather={max_weather_date}")

    # 2. Truy vấn dữ liệu phụ tải quốc gia và thời tiết từ tầng Gold
    sql_gold = f"""
    WITH load_nat AS (
        SELECT 
            l.date_value AS record_date,
            ROUND(AVG(l.load_mw), 4) AS load_mean_mw,
            ROUND(MAX(l.load_mw), 4) AS load_peak_mw,
            ROUND(MIN(l.load_mw), 4) AS load_min_mw
        FROM iceberg.gold.v_load_national_hourly l
        GROUP BY l.date_value
    ),
    load_reg AS (
        SELECT 
            d.full_date AS record_date,
            ROUND(AVG(CASE WHEN f.region_key = 1 THEN f.load_mw END), 4) as load_mean_mw_north,
            ROUND(AVG(CASE WHEN f.region_key = 2 THEN f.load_mw END), 4) as load_mean_mw_central,
            ROUND(AVG(CASE WHEN f.region_key = 3 THEN f.load_mw END), 4) as load_mean_mw_south
        FROM iceberg.gold.fact_load_hourly f
        JOIN iceberg.gold.dim_date d ON f.date_key = d.date_key
        GROUP BY d.full_date
    ),
    weather_nat AS (
        SELECT 
            d.full_date AS record_date,
            ROUND(AVG(w.temperature_2m_mean), 4) AS wx_temp_c,
            ROUND(AVG(w.shortwave_radiation_sum), 4) AS wx_solar_rad_wm2,
            ROUND(AVG(w.precipitation_sum), 4) AS wx_precip_mm,
            ROUND(AVG(w.wind_speed_10m_max), 4) AS wx_wind_ms,
            ROUND(AVG(w.cloud_cover_mean), 4) AS wx_cloud_pct
        FROM iceberg.gold.fact_weather_daily w
        JOIN iceberg.gold.dim_date d ON w.date_key = d.date_key
        GROUP BY d.full_date
    )
    SELECT 
        CAST(ln.record_date AS VARCHAR) AS date,
        ln.load_mean_mw,
        ln.load_peak_mw,
        ln.load_min_mw,
        lr.load_mean_mw_north,
        lr.load_mean_mw_central,
        lr.load_mean_mw_south,
        wn.wx_temp_c,
        wn.wx_solar_rad_wm2,
        wn.wx_precip_mm,
        wn.wx_wind_ms,
        wn.wx_cloud_pct
    FROM load_nat ln
    LEFT JOIN load_reg lr ON ln.record_date = lr.record_date
    LEFT JOIN weather_nat wn ON ln.record_date = wn.record_date
    ORDER BY ln.record_date
    """
    print("[+] Đang truy vấn toàn bộ dữ liệu phụ tải & thời tiết từ tầng Gold...")
    df_gold = execute_trino(sql_gold)
    print(f"[✓] Đã lấy {len(df_gold)} ngày từ tầng Gold (từ {df_gold['date'].min()} đến {df_gold['date'].max()})")

    # 3. Đọc tệp unified_daily_v2.csv hiện tại
    df_csv = pd.read_csv(CSV_PATH)
    print(f"[+] Tệp CSV hiện tại: {len(df_csv)} dòng, ngày cuối: {df_csv['date'].max()}")

    # 4. Cập nhật các cột từ tầng Gold vào CSV
    df_gold_dict = df_gold.set_index("date").to_dict(orient="index")
    updated_count = 0

    for idx, row in df_csv.iterrows():
        d_str = str(row["date"])
        if d_str in df_gold_dict:
            g_row = df_gold_dict[d_str]
            # Nếu CSV bị NaN hoặc giá trị cũ thì cập nhật
            if pd.isna(row.get("load_mean_mw")) or row.get("has_load") == 0:
                df_csv.at[idx, "load_mean_mw"] = g_row["load_mean_mw"]
                df_csv.at[idx, "load_peak_mw"] = g_row["load_peak_mw"]
                df_csv.at[idx, "load_min_mw"] = g_row["load_min_mw"]
                df_csv.at[idx, "load_mean_mw_north"] = g_row["load_mean_mw_north"]
                df_csv.at[idx, "load_mean_mw_central"] = g_row["load_mean_mw_central"]
                df_csv.at[idx, "load_mean_mw_south"] = g_row["load_mean_mw_south"]
                df_csv.at[idx, "wx_temp_c"] = g_row["wx_temp_c"]
                df_csv.at[idx, "wx_solar_rad_wm2"] = g_row["wx_solar_rad_wm2"]
                df_csv.at[idx, "wx_precip_mm"] = g_row["wx_precip_mm"]
                df_csv.at[idx, "wx_wind_ms"] = g_row["wx_wind_ms"]
                df_csv.at[idx, "wx_cloud_pct"] = g_row["wx_cloud_pct"]
                df_csv.at[idx, "has_load"] = 1
                updated_count += 1

    # Lưu lại CSV đã đồng bộ
    df_csv.to_csv(CSV_PATH, index=False)
    print(f"[✓] Đã cập nhật thành công {updated_count} ngày mới từ tầng Gold vào {CSV_PATH}")
    print(f"[✓] Số ngày có đủ phụ tải (has_load=1): {(df_csv['has_load'] == 1).sum()} ngày")
    print(f"[✓] Ngày max có dữ liệu thực tế: {df_csv[df_csv['has_load'] == 1]['date'].max()}")


if __name__ == "__main__":
    main()
