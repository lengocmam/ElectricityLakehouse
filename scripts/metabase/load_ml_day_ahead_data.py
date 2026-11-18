# -*- coding: utf-8 -*-
"""Nạp dữ liệu Dự báo Điều độ Ngày tới (Day-Ahead Dispatch) & Cơ cấu Nguồn vào Iceberg Gold.

Nguồn dữ liệu:
- hourly_ml_test_predictions.csv (Dự báo S-Mamba, S4D, XGBoost trên Test 2026-07-01 -> 2026-11-16)
- hourly_feature_store.csv (Phụ tải thực tế, Cơ cấu phát Than/Thủy điện/Khí/Mặt trời/Gió, Khí tượng)
- Conformal Prediction Quantile 90% (q = 1,459.9 MW cho S-Mamba)
- Seasonal-Naive Week (t-168)
"""
import sys
import os
import urllib.request
import json
import time
import pandas as pd
import numpy as np

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

TRINO_URL = "http://localhost:8082/v1/statement"
TRINO_USER = "admin"


def execute_trino_query(sql, catalog="iceberg", schema="gold"):
    cleaned_sql = sql.strip().rstrip(";")
    req = urllib.request.Request(
        TRINO_URL,
        data=cleaned_sql.encode('utf-8'),
        headers={
            "X-Trino-User": TRINO_USER,
            "X-Trino-Catalog": catalog,
            "X-Trino-Schema": schema,
            "Content-Type": "text/plain; charset=utf-8"
        }
    )
    with urllib.request.urlopen(req) as response:
        res = json.loads(response.read().decode('utf-8'))
    
    columns = [col['name'] for col in res.get('columns', [])]
    all_data = []
    if 'data' in res:
        all_data.extend(res['data'])
        
    next_uri = res.get("nextUri")
    while next_uri:
        time.sleep(0.05)
        req = urllib.request.Request(next_uri, headers={"X-Trino-User": TRINO_USER})
        with urllib.request.urlopen(req) as response:
            res = json.loads(response.read().decode('utf-8'))
            if not columns and 'columns' in res:
                columns = [col['name'] for col in res.get('columns', [])]
            if 'data' in res:
                all_data.extend(res['data'])
            next_uri = res.get("nextUri")
            
    if "error" in res:
        raise Exception(f"Trino Query Error: {res['error']['message']}\nFull Error: {res['error']}")
        
    return columns, all_data


def main():
    print("="*75)
    print("NẠP BẢNG DỰ BÁO ĐIỀU ĐỘ NGÀY TỚI VÀO ICEBERG GOLD")
    print("="*75)
    
    # 1. Nạp và xử lý dữ liệu
    df_feat = pd.read_csv('data_exports/hourly_feature_store.csv')
    df_feat['datetime'] = pd.to_datetime(df_feat['datetime'])
    df_feat = df_feat.sort_values('datetime').reset_index(drop=True)
    df_feat['load_national_naive_week'] = df_feat['load_national'].shift(168)
    
    df_pred = pd.read_csv('data_exports/ml_results/hourly_ml_test_predictions.csv')
    df_pred['datetime'] = pd.to_datetime(df_pred['datetime'])
    
    merged = pd.merge(df_pred, df_feat, on='datetime', how='inner')
    merged = merged.sort_values('datetime').reset_index(drop=True)
    print(f">>> Đã khớp dữ liệu: {len(merged):,} giờ ({merged['datetime'].min()} -> {merged['datetime'].max()})")
    
    Q90 = 1459.9  # Ngưỡng Conformal 90% của S-Mamba
    
    merged['date_value'] = merged['datetime'].dt.strftime('%Y-%m-%d')
    merged['hour_of_day'] = merged['datetime'].dt.hour
    merged['hour_str'] = merged['datetime'].dt.strftime('%H:00')
    
    merged['load_actual'] = merged['load_national_actual'].round(1)
    merged['load_pred_smamba'] = merged['load_national_pred_smamba'].round(1)
    merged['load_pred_s4d'] = merged['load_national_pred_s4d'].round(1)
    merged['load_pred_xgb'] = merged['load_national_pred_xgb'].round(1)
    merged['load_pred_naive'] = merged['load_national_naive_week'].round(1)
    
    merged['conformal_lower_90'] = np.maximum(0, merged['load_pred_smamba'] - Q90).round(1)
    merged['conformal_upper_90'] = (merged['load_pred_smamba'] + Q90).round(1)
    
    merged['err_smamba'] = np.abs(merged['load_actual'] - merged['load_pred_smamba']).round(1)
    merged['err_naive'] = np.abs(merged['load_actual'] - merged['load_pred_naive']).round(1)
    
    merged['net_load_actual'] = merged['net_load_actual'].round(1)
    merged['net_load_pred_smamba'] = merged['net_load_pred_smamba'].round(1)
    
    merged['p_coal'] = merged['p_coal'].round(1)
    merged['p_hydro'] = merged['p_hydro'].round(1)
    merged['p_gas'] = merged['p_gas_total'].round(1)
    merged['p_solar'] = merged['p_solar_total'].round(1)
    merged['p_wind'] = merged['p_wind'].round(1)
    merged['p_others'] = (merged['p_biomass'] + merged['p_import'] + merged['p_oil_fo'] + merged['p_oil_do'] + merged['p_thermal_other']).round(1)
    merged['p_total_gen'] = merged['gen_total'].round(1)
    
    merged['wx_temp_c'] = merged['wx_temp_c'].round(1)
    merged['wx_solar_rad_wm2'] = merged['wx_solar_rad_wm2'].round(1)
    
    cols_to_keep = [
        'datetime', 'date_value', 'hour_of_day', 'hour_str',
        'load_actual', 'load_pred_smamba', 'load_pred_s4d', 'load_pred_xgb', 'load_pred_naive',
        'conformal_lower_90', 'conformal_upper_90', 'err_smamba', 'err_naive',
        'net_load_actual', 'net_load_pred_smamba',
        'p_coal', 'p_hydro', 'p_gas', 'p_solar', 'p_wind', 'p_others', 'p_total_gen',
        'wx_temp_c', 'wx_solar_rad_wm2'
    ]
    df_out = merged[cols_to_keep].copy()
    
    # 2. Xóa bảng cũ nếu có và tạo bảng mới trong Trino Iceberg
    print(">>> Đang khởi tạo bảng iceberg.gold.fact_ml_day_ahead_dispatch trong Trino...")
    execute_trino_query("DROP TABLE IF EXISTS iceberg.gold.fact_ml_day_ahead_dispatch")
    
    create_sql = """
    CREATE TABLE iceberg.gold.fact_ml_day_ahead_dispatch (
        datetime VARCHAR,
        date_value DATE,
        hour_of_day INTEGER,
        hour_str VARCHAR,
        load_actual DOUBLE,
        load_pred_smamba DOUBLE,
        load_pred_s4d DOUBLE,
        load_pred_xgb DOUBLE,
        load_pred_naive DOUBLE,
        conformal_lower_90 DOUBLE,
        conformal_upper_90 DOUBLE,
        err_smamba DOUBLE,
        err_naive DOUBLE,
        net_load_actual DOUBLE,
        net_load_pred_smamba DOUBLE,
        p_coal DOUBLE,
        p_hydro DOUBLE,
        p_gas DOUBLE,
        p_solar DOUBLE,
        p_wind DOUBLE,
        p_others DOUBLE,
        p_total_gen DOUBLE,
        wx_temp_c DOUBLE,
        wx_solar_rad_wm2 DOUBLE
    )
    """
    execute_trino_query(create_sql)
    print(">>> Bảng fact_ml_day_ahead_dispatch đã được tạo thành công!")
    
    # 3. Nạp dữ liệu theo lô (Batch INSERT)
    BATCH_SIZE = 400
    total_rows = len(df_out)
    print(f">>> Đang nạp {total_rows:,} bản ghi theo từng lô {BATCH_SIZE}...")
    
    for start_idx in range(0, total_rows, BATCH_SIZE):
        chunk = df_out.iloc[start_idx : start_idx + BATCH_SIZE]
        val_rows = []
        for _, row in chunk.iterrows():
            dt_str = str(row['datetime'])
            dv_str = f"DATE '{row['date_value']}'"
            hr_int = int(row['hour_of_day'])
            hr_str = f"'{row['hour_str']}'"
            
            vals = [
                f"'{dt_str}'", dv_str, str(hr_int), hr_str,
                str(row['load_actual']), str(row['load_pred_smamba']), str(row['load_pred_s4d']),
                str(row['load_pred_xgb']), str(row['load_pred_naive']),
                str(row['conformal_lower_90']), str(row['conformal_upper_90']),
                str(row['err_smamba']), str(row['err_naive']),
                str(row['net_load_actual']), str(row['net_load_pred_smamba']),
                str(row['p_coal']), str(row['p_hydro']), str(row['p_gas']),
                str(row['p_solar']), str(row['p_wind']), str(row['p_others']),
                str(row['p_total_gen']), str(row['wx_temp_c']), str(row['wx_solar_rad_wm2'])
            ]
            val_rows.append(f"({', '.join(vals)})")
            
        insert_sql = f"INSERT INTO iceberg.gold.fact_ml_day_ahead_dispatch VALUES {', '.join(val_rows)}"
        execute_trino_query(insert_sql)
        print(f"  * Đã nạp {min(start_idx + BATCH_SIZE, total_rows):,}/{total_rows:,} dòng...", flush=True)
        
    print("\n>>> KIỂM TRA LẠI DỮ LIỆU:")
    cols, data = execute_trino_query("SELECT COUNT(*), MIN(date_value), MAX(date_value) FROM iceberg.gold.fact_ml_day_ahead_dispatch")
    print(f"  * Tổng số dòng trong Iceberg: {data[0][0]:,} dòng")
    print(f"  * Khoảng ngày có dữ liệu : {data[0][1]} -> {data[0][2]}")
    
    # 4. Tạo Semantic View v_ml_day_ahead_dispatch
    print("\n>>> Đang tạo view ngữ nghĩa iceberg.gold.v_ml_day_ahead_dispatch...")
    view_sql = """
    CREATE OR REPLACE VIEW iceberg.gold.v_ml_day_ahead_dispatch AS
    SELECT 
        datetime,
        date_value,
        hour_of_day,
        hour_str,
        load_actual AS phu_tai_thuc_te_mw,
        load_pred_smamba AS du_bao_smamba_mw,
        load_pred_s4d AS du_bao_s4d_mw,
        load_pred_xgb AS du_bao_xgboost_mw,
        load_pred_naive AS kinh_nghiem_tuan_truoc_mw,
        conformal_lower_90 AS gioi_han_duoi_90_mw,
        conformal_upper_90 AS gioi_han_tren_90_mw,
        err_smamba AS sai_so_smamba_mw,
        err_naive AS sai_so_naive_mw,
        net_load_actual AS phu_tai_rong_thuc_te_mw,
        net_load_pred_smamba AS phu_tai_rong_du_bao_mw,
        p_coal AS npt_nhiet_dien_than_mw,
        p_hydro AS npt_thuy_dien_mw,
        p_gas AS npt_tua_bin_khi_mw,
        p_solar AS npt_dien_mat_troi_mw,
        p_wind AS npt_dien_gio_mw,
        p_others AS npt_nguon_khac_mw,
        p_total_gen AS tong_phat_he_thong_mw,
        wx_temp_c AS nhiet_do_c,
        wx_solar_rad_wm2 AS buc_xa_wm2
    FROM iceberg.gold.fact_ml_day_ahead_dispatch
    """
    execute_trino_query(view_sql)
    print(">>> VIEW v_ml_day_ahead_dispatch ĐÃ ĐƯỢC TẠO THÀNH CÔNG VỚI ĐẦY ĐỦ CÁC CỘT NGỮ NGHĨA!")
    print("="*75)


if __name__ == "__main__":
    main()
