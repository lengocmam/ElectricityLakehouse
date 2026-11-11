#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
load_full_dispatcher_trino_data.py
Nạp toàn bộ 284 ngày kiểm thử (2026-01-01 đến 2026-10-11) vào Trino Iceberg Gold
và xuất tệp JSON đầy đủ phục vụ chế độ 'Chọn Mỗi Ngày' (Interactive Date Filter):
  - Khi chọn bất kỳ ngày nào, toàn bộ 4 khu vực tự động cập nhật:
    1. Dự báo ngày đó & 7 ngày tiếp theo (Ensemble + dải bất định).
    2. Cảnh báo nhiệt độ ngày đó & so sánh đỉnh lịch sử.
    3. What-if thời tiết xoay quanh nhiệt độ ngày đó.
    4. Giám sát 30 ngày tính ngược từ ngày đó & rolling MAPE tại thời điểm đó.
"""

import os
import sys
import json
import urllib.request
import numpy as np
import pandas as pd

CODE_DIR = "s4_3thuat_toan/code"
RESULTS_DIR = os.path.join(CODE_DIR, "results", "focused_4models")
DATA_CSV = os.path.join(CODE_DIR, "unified_daily_v2.csv")
OUT_JSON = os.path.join("scripts", "dispatcher_operational_data_all_days.json")
TRINO_URL = "http://localhost:8082/v1/statement"

HISTORICAL_PEAK = 43609.5  # Đỉnh lịch sử 08/05/2025
SLOPE_HEAT = 795.8         # MW/°C

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
    print("XÂY DỰNG DỮ LIỆU ĐIỀU ĐỘ ĐA NGÀY (CHỌN MỖI NGÀY - 284 NGÀY KIỂM THỬ)")
    print("=" * 80)

    # 1. Đọc dữ liệu
    df = pd.read_csv(DATA_CSV)
    df["date"] = pd.to_datetime(df["date"])
    
    p1 = pd.read_csv(os.path.join(RESULTS_DIR, "preds_load_H1.csv"))
    p1["date"] = pd.to_datetime(p1["date"])
    
    m = pd.merge(p1, df[["date", "wx_temp_c", "wx_solar_rad_wm2", "is_working_day", "is_public_holiday"]], on="date")
    m = m.sort_values("date").reset_index(drop=True)

    # Tính sai số
    m["error_mw"] = m["actual"] - m["pred_ensemble"]
    m["abs_error_mw"] = np.abs(m["error_mw"])
    m["mape_pct"] = (m["abs_error_mw"] / m["actual"]) * 100.0

    # Dải bất định 95% (dựa trên RMSE 1,177 MW)
    RMSE_VAL = 1177.7
    m["ci_band_mw"] = 1.96 * RMSE_VAL
    m["ci_lower_mw"] = np.maximum(0.0, m["pred_ensemble"] - m["ci_band_mw"])
    m["ci_upper_mw"] = m["pred_ensemble"] + m["ci_band_mw"]

    # Rolling 30 ngày (tính ngược từ mỗi ngày)
    m["rolling_30d_mape"] = m["mape_pct"].rolling(30, min_periods=1).mean()
    m["rolling_30d_mae"] = m["abs_error_mw"].rolling(30, min_periods=1).mean()

    # Cảnh báo nhiệt độ và rủi ro đỉnh tải
    def get_heat_status(temp, pred):
        if temp >= 30.0 or pred >= HISTORICAL_PEAK:
            return "BÁO ĐỘNG ĐỎ: VƯỢT ĐỈNH LỊCH SỬ"
        elif temp >= 28.0 or pred >= 40000.0:
            return "CẢNH BÁO VÀNG: TIỆM CẬN ĐỈNH"
        return "AN TOÀN: TRONG GIỚI HẠN VẬN HÀNH"

    m["heat_risk_status"] = [get_heat_status(t, p) for t, p in zip(m["wx_temp_c"], m["pred_ensemble"])]
    m["pct_of_historical_peak"] = (m["pred_ensemble"] / HISTORICAL_PEAK) * 100.0

    # 2. Nạp vào Trino bảng fact_dispatcher_daily_predictions
    print(f"[+] Tạo và nạp bảng iceberg.gold.fact_dispatcher_daily_predictions ({len(m)} ngày)...")
    execute_trino("DROP TABLE IF EXISTS iceberg.gold.fact_dispatcher_daily_predictions")
    ddl = """
    CREATE TABLE iceberg.gold.fact_dispatcher_daily_predictions (
        record_date DATE,
        day_of_week VARCHAR,
        actual_load_mw DOUBLE,
        pred_ensemble_mw DOUBLE,
        pred_s4d_mw DOUBLE,
        pred_smamba_mw DOUBLE,
        pred_ridge_mw DOUBLE,
        control_lightgbm_mw DOUBLE,
        ci_lower_mw DOUBLE,
        ci_upper_mw DOUBLE,
        uncertainty_band_mw DOUBLE,
        error_mw DOUBLE,
        abs_error_mw DOUBLE,
        mape_pct DOUBLE,
        rolling_30d_mape DOUBLE,
        temp_c DOUBLE,
        solar_rad_wm2 DOUBLE,
        heat_risk_status VARCHAR,
        pct_of_historical_peak DOUBLE,
        is_working_day BOOLEAN,
        is_public_holiday BOOLEAN
    ) WITH (format = 'PARQUET')
    """
    execute_trino(ddl)

    # Nạp theo lô 50 dòng
    dow_vn = ["Thứ Hai", "Thứ Ba", "Thứ Tư", "Thứ Năm", "Thứ Sáu", "Thứ Bảy", "Chủ Nhật"]
    batch_size = 50
    for start_idx in range(0, len(m), batch_size):
        sub = m.iloc[start_idx:start_idx + batch_size]
        vals = []
        for _, r in sub.iterrows():
            d_str = r["date"].strftime("%Y-%m-%d")
            d_name = dow_vn[r["date"].dayofweek]
            is_wd = "true" if r["is_working_day"] else "false"
            is_hol = "true" if r["is_public_holiday"] else "false"
            val = (
                f"(DATE '{d_str}', '{d_name}', {r['actual']:.1f}, {r['pred_ensemble']:.1f}, "
                f"{r['pred_s4d']:.1f}, {r['pred_smamba']:.1f}, {r['pred_ridge']:.1f}, "
                f"{r['control_lightgbm']:.1f}, {r['ci_lower_mw']:.1f}, {r['ci_upper_mw']:.1f}, "
                f"{r['ci_band_mw']:.1f}, {r['error_mw']:.1f}, {r['abs_error_mw']:.1f}, "
                f"{r['mape_pct']:.2f}, {r['rolling_30d_mape']:.2f}, {r['wx_temp_c']:.1f}, "
                f"{r['wx_solar_rad_wm2']:.1f}, '{r['heat_risk_status']}', {r['pct_of_historical_peak']:.1f}, "
                f"{is_wd}, {is_hol})"
            )
            vals.append(val)
        ins = "INSERT INTO iceberg.gold.fact_dispatcher_daily_predictions VALUES\n" + ",\n".join(vals)
        execute_trino(ins)

    print(f"[✓] Đã nạp thành công toàn bộ {len(m)} ngày vào iceberg.gold.fact_dispatcher_daily_predictions!")

    # 3. Xuất tệp JSON phục vụ ứng dụng tương tác giao diện chọn mỗi ngày
    print("[+] Đang xuất cấu trúc dữ liệu đa ngày ra JSON...")
    all_days_dict = {}
    date_list = []

    for i in range(len(m)):
        row = m.iloc[i]
        d_str = row["date"].strftime("%Y-%m-%d")
        date_list.append(d_str)

        # 7 ngày tiếp theo tính từ ngày i (nếu có trong dữ liệu)
        next_7d = []
        step_mapes = [2.11, 2.45, 2.80, 3.15, 3.50, 3.80, 4.07]
        for step in range(1, 8):
            target_idx = i + step
            if target_idx < len(m):
                nxt_row = m.iloc[target_idx]
                nxt_date = nxt_row["date"].strftime("%Y-%m-%d")
                nxt_dow = dow_vn[nxt_row["date"].dayofweek]
                pred_val = round(float(nxt_row["pred_ensemble"]), 1)
                act_val = round(float(nxt_row["actual"]), 1)
                lower_val = round(float(nxt_row["ci_lower_mw"]), 1)
                upper_val = round(float(nxt_row["ci_upper_mw"]), 1)
            else:
                # Nếu vượt quá ngày cuối cùng của tập dữ liệu
                nxt_date = (row["date"] + pd.Timedelta(days=step)).strftime("%Y-%m-%d")
                nxt_dow = dow_vn[(row["date"] + pd.Timedelta(days=step)).dayofweek]
                pred_val = round(float(row["pred_ensemble"] * (0.92 if (row["date"] + pd.Timedelta(days=step)).dayofweek >= 5 else 1.01)), 1)
                act_val = None
                lower_val = round(pred_val - 1.96 * 1177.7, 1)
                upper_val = round(pred_val + 1.96 * 1177.7, 1)

            next_7d.append({
                "step": step,
                "name": f"Ngày +{step}" if step > 1 else "Ngày mai (H=1)",
                "date": nxt_date,
                "dow": nxt_dow,
                "pred_mw": pred_val,
                "actual_mw": act_val,
                "ci_lower_mw": lower_val,
                "ci_upper_mw": upper_val,
                "expected_mape": step_mapes[step - 1]
            })

        # 30 ngày giám sát tính ngược từ ngày i
        start_monitor_idx = max(0, i - 29)
        monitor_sub = m.iloc[start_monitor_idx:i + 1]
        monitor_30d = []
        for _, mr in monitor_sub.iterrows():
            monitor_30d.append({
                "date": mr["date"].strftime("%Y-%m-%d"),
                "actual_mw": round(float(mr["actual"]), 1),
                "pred_mw": round(float(mr["pred_ensemble"]), 1),
                "error_mw": round(float(mr["error_mw"]), 1),
                "mape_pct": round(float(mr["mape_pct"]), 2),
                "temp_c": round(float(mr["wx_temp_c"]), 1)
            })

        all_days_dict[d_str] = {
            "date": d_str,
            "day_name": dow_vn[row["date"].dayofweek],
            "actual_mw": round(float(row["actual"]), 1),
            "pred_mw": round(float(row["pred_ensemble"]), 1),
            "pred_s4d_mw": round(float(row["pred_s4d"]), 1),
            "pred_smamba_mw": round(float(row["pred_smamba"]), 1),
            "pred_ridge_mw": round(float(row["pred_ridge"]), 1),
            "ci_lower_mw": round(float(row["ci_lower_mw"]), 1),
            "ci_upper_mw": round(float(row["ci_upper_mw"]), 1),
            "uncertainty_band_mw": round(float(row["ci_band_mw"]), 1),
            "error_mw": round(float(row["error_mw"]), 1),
            "mape_pct": round(float(row["mape_pct"]), 2),
            "temp_c": round(float(row["wx_temp_c"]), 1),
            "heat_status": row["heat_risk_status"],
            "pct_of_historical_peak": round(float(row["pct_of_historical_peak"]), 1),
            "rolling_30d_mape": round(float(row["rolling_30d_mape"]), 2),
            "rolling_30d_mae": round(float(row["rolling_30d_mae"]), 1),
            "next_7d": next_7d,
            "monitor_30d": monitor_30d
        }

    payload = {
        "dates_available": date_list,
        "default_date": date_list[-1],
        "historical_peak_mw": HISTORICAL_PEAK,
        "slope_heat_mw_per_c": SLOPE_HEAT,
        "days": all_days_dict
    }

    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    print(f"[✓] Đã xuất thành công: {OUT_JSON} (Chứa {len(date_list)} ngày điều độ!)")
    print("=" * 80)


if __name__ == "__main__":
    main()
