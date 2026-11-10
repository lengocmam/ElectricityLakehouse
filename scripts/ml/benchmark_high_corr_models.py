#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SCRIPT: benchmark_high_corr_models.py
Mục đích:
  Đánh giá hiệu năng và huấn luyện mô hình học máy trên bộ ĐẶC TRƯNG TƯƠNG QUAN CAO
  đã được chuẩn hóa từ kho dữ liệu Lakehouse (iceberg.gold).
  So sánh đối chuẩn (Apple-to-Apple) giữa:
    - Bộ đặc trưng ban đầu: 'cal,wx,hol' (Phụ tải) và 'cal,hydro' (Thủy điện)
    - Bộ đặc trưng tương quan cao: 'load_high_corr' và 'hydro_high_corr'
  Đánh giá 4 bài toán:
    1. load_H1: Phụ tải 1 ngày (MW)
    2. load_H7: Phụ tải 7 ngày (MW)
    3. hydro_H1: Thủy điện 1 ngày (MWh)
    4. hydro_H7: Thủy điện 7 ngày (MWh)
  Các mô hình:
    - LightGBM (Tuned)
    - S4D (Structured State Space Model)
    - TimeXer (NeurIPS 2024)
    - S-Mamba (Selective State Space)
    - Ensemble 3 bài báo (S4D + TimeXer + S-Mamba)
    - Ridge Regression
    - Persistence (Baseline)
    - Seasonal Naive 7 (Baseline)
  Tự động nạp kết quả vào Iceberg Gold trong Trino:
    - iceberg.gold.fact_forecast_benchmark
    - iceberg.gold.fact_forecast_horizon_mae
"""

import os
import sys
import json
import time
from types import SimpleNamespace
from datetime import datetime

# Windows console encoding
if hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

# Import lightgbm FIRST before anything else to ensure clean OpenMP loading on Windows
import lightgbm as lgb
import numpy as np
import pandas as pd
from scipy import stats
import torch
import urllib.request

CODE_DIR = "s4_3thuat_toan/code"
if CODE_DIR not in sys.path:
    sys.path.insert(0, CODE_DIR)

from data import prepare
from models import build_model
from baselines import persistence, seasonal_naive, ridge_fit_predict, lightgbm_fit_predict
from train2 import run_nn, dm_test


def execute_trino_query(sql: str) -> list:
    """Thực thi SQL statement lên Trino REST API (port 8082)."""
    sql_clean = sql.strip().rstrip(";")
    req = urllib.request.Request(
        "http://localhost:8082/v1/statement",
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


def calc_metrics(pred, true):
    """Tính các chỉ số đánh giá sai số và độ chính xác."""
    diff = pred - true
    mae = float(np.abs(diff).mean())
    rmse = float(np.sqrt((diff ** 2).mean()))
    denom = np.maximum(np.abs(true), 1e-9)
    mape = float((np.abs(diff) / denom).mean() * 100.0)
    wape = float(np.abs(diff).sum() / denom.sum() * 100.0)
    acc = round(max(0.0, 100.0 - wape), 2)
    
    ss_tot = float(((true - true.mean()) ** 2).sum())
    ss_res = float((diff ** 2).sum())
    r2 = round(1.0 - (ss_res / max(ss_tot, 1e-9)), 4)
    return {
        "mae": round(mae, 1),
        "rmse": round(rmse, 1),
        "mape_pct": round(mape, 2),
        "wape_pct": round(wape, 2),
        "accuracy_pct": acc,
        "r2_score": r2
    }


def load_best_cfg(task_code: str) -> dict:
    cfg_file = os.path.join(CODE_DIR, f"best_configs/{task_code}.json")
    if os.path.exists(cfg_file):
        with open(cfg_file, "r", encoding="utf-8") as f:
            return json.load(f).get("best_config", {})
    return {}


def main():
    print("=" * 80)
    print("CHƯƠNG TRÌNH ĐÁNH GIÁ MÔ HÌNH HỌC MÁY TRÊN ĐẶC TRƯNG TƯƠNG QUAN CAO LAKEHOUSE")
    print("=" * 80)
    start_total_time = time.time()

    csv_path = os.path.join(CODE_DIR, "unified_daily_v2.csv")
    tasks = [
        ("load_H1", "Phụ tải điện 1 ngày", "MW", "load_mean_mw", ["cal", "wx", "hol"], ["load_high_corr"], 1),
        ("load_H7", "Phụ tải điện 7 ngày", "MW", "load_mean_mw", ["cal", "wx", "hol"], ["load_high_corr"], 7),
        ("hydro_H1", "Sản lượng thủy điện 1 ngày", "MWh", "gen_hydro", ["cal", "hydro"], ["hydro_high_corr"], 1),
        ("hydro_H7", "Sản lượng thủy điện 7 ngày", "MWh", "gen_hydro", ["cal", "hydro"], ["hydro_high_corr"], 7),
    ]

    benchmark_records = []
    horizon_mae_records = []
    comparison_summary = []

    for task_code, task_name, unit, target, old_features, high_corr_features, horizon in tasks:
        print(f"\n>>> [BÀI TOÁN: {task_name.upper()} ({task_code}) - HORIZON {horizon} NGÀY]")
        print("-" * 80)
        
        # 1. Chuẩn bị dữ liệu
        print(f"  [+] Đang chuẩn bị dữ liệu: Cũ ({'+'.join(old_features)}) vs Mới ({'+'.join(high_corr_features)})...")
        prep_old = prepare(csv_path, target, old_features, lookback=28, horizon=horizon)
        prep_new = prepare(csv_path, target, high_corr_features, lookback=28, horizon=horizon)
        
        w_old = prep_old.win
        w_new = prep_new.win
        te_mask = w_new.split == 2
        
        true_old = w_old.Y[te_mask] * prep_old.t_std + prep_old.t_mean
        true_new = w_new.Y[te_mask] * prep_new.t_std + prep_new.t_mean
        
        n_feats_old = len(prep_old.feature_names)
        n_feats_new = len(prep_new.feature_names)
        print(f"  [✓] Số chiều đặc trưng: Cũ = {n_feats_old} biến -> Mới (Tương quan cao) = {n_feats_new} biến (Giảm {n_feats_old - n_feats_new} biến rác/nhiễu)")

        best_cfg = load_best_cfg(task_code)
        
        # 2. Huấn luyện Baselines & LightGBM
        print("  [+] Huấn luyện mô hình Baselines & LightGBM trên đặc trưng tương quan cao...")
        # Persistence
        p_pers = persistence(w_new.X, horizon)[te_mask] * prep_new.t_std + prep_new.t_mean
        # Seasonal Naive
        p_snaive = seasonal_naive(w_new.X, horizon, period=7)[te_mask] * prep_new.t_std + prep_new.t_mean
        
        # Ridge
        p_ridge_norm, _ = ridge_fit_predict(w_new.X, w_new.Y, w_new.split, horizon, lags=14, Fut=w_new.F)
        p_ridge = p_ridge_norm[te_mask] * prep_new.t_std + prep_new.t_mean

        # LightGBM Tuned
        lgb_over = best_cfg.get("lightgbm", {})
        lags_lgb = lgb_over.get("lags", 14)
        p_lgb_norm = lightgbm_fit_predict(w_new.X, w_new.Y, w_new.split, horizon, lags=lags_lgb, over=lgb_over, Fut=w_new.F)
        p_lgb = p_lgb_norm[te_mask] * prep_new.t_std + prep_new.t_mean
        
        # 3. Huấn luyện Mô hình Học sâu chuỗi thời gian (S4D, TimeXer, S-Mamba)
        nn_models = ["s4d", "timexer", "smamba"]
        nn_preds = {}
        
        for m_name in nn_models:
            print(f"  [+] Đang huấn luyện mạng học sâu: {m_name.upper()}...")
            m_cfg = best_cfg.get(m_name, {})
            args_nn = SimpleNamespace(
                horizon=horizon,
                lookback=m_cfg.get("lookback", 28),
                epochs=25,
                patience=8,
                batch=64,
                d_model=m_cfg.get("d_model", 64),
                n_layers=m_cfg.get("n_layers", 2),
                d_state=m_cfg.get("d_state", 16),
                ptst_layers=m_cfg.get("ptst_layers", 2),
                patch_len=m_cfg.get("patch_len", 7),
                dropout=m_cfg.get("dropout", 0.1),
                dt_min=m_cfg.get("dt_min", 0.01),
                dt_max=m_cfg.get("dt_max", 0.5),
                lr=m_cfg.get("lr", 2e-3),
                wd=m_cfg.get("wd", 0.05),
                future=1,
                anchor="none"
            )
            # Chạy seed 42 và seed 100 để tạo ensemble ổn định
            preds_seeds = []
            for seed in [42, 100]:
                p_te_norm, *_ = run_nn(prep_new, args_nn, m_name, seed=seed, device="cpu")
                preds_seeds.append(p_te_norm * prep_new.t_std + prep_new.t_mean)
            nn_preds[m_name] = np.mean(preds_seeds, axis=0)

        # 4. Tổ hợp Ensemble 3 bài báo (S4D + TimeXer + S-Mamba)
        p_ens = (nn_preds["s4d"] + nn_preds["timexer"] + nn_preds["smamba"]) / 3.0

        # Dự đoán của tất cả các mô hình
        all_preds = {
            "LightGBM": ("Machine Learning", p_lgb),
            "S4D": ("Deep Learning (SSM)", nn_preds["s4d"]),
            "TimeXer": ("Deep Learning (Informer)", nn_preds["timexer"]),
            "S-Mamba": ("Deep Learning (Selective SSM)", nn_preds["smamba"]),
            "Ensemble 3 bài báo": ("Ensemble Learning", p_ens),
            "Ridge Regression": ("Linear Baseline", p_ridge),
            "Persistence": ("Naive Baseline", p_pers),
            "Seasonal Naive": ("Seasonal Baseline", p_snaive),
        }

        # 5. Tính chỉ số đánh giá cho từng mô hình
        ranked_list = []
        for m_label, (m_type, p) in all_preds.items():
            m_metrics = calc_metrics(p, true_new)
            stat, pval = dm_test(
                (p - prep_new.t_mean) / prep_new.t_std,
                (p_lgb - prep_new.t_mean) / prep_new.t_std,
                (true_new - prep_new.t_mean) / prep_new.t_std,
                horizon
            )
            m_metrics.update({
                "model_name": m_label,
                "model_type": m_type,
                "dm_stat": round(stat, 2),
                "dm_pval": round(pval, 3),
            })
            ranked_list.append(m_metrics)

        # Xếp hạng theo MAE
        ranked_list.sort(key=lambda x: x["mae"])
        for rank, item in enumerate(ranked_list, start=1):
            item["rank_order"] = rank
            benchmark_records.append({
                "task_code": task_code,
                "task_name": task_name,
                "unit": unit,
                **item
            })
            print(f"    Top {rank}: {item['model_name']:<20} | MAE: {item['mae']:>8.1f} {unit} | RMSE: {item['rmse']:>8.1f} | Acc: {item['accuracy_pct']:>6.2f}% | R2: {item['r2_score']:>6.4f}")

        # 6. Ghi nhận Horizon MAE cho các bài toán 7 ngày (Day +1 .. Day +7)
        if horizon == 7:
            for step in range(1, 8):
                idx = step - 1
                mae_lgb_step = round(float(np.abs(p_lgb[:, idx] - true_new[:, idx]).mean()), 1)
                mae_s4d_step = round(float(np.abs(nn_preds["s4d"][:, idx] - true_new[:, idx]).mean()), 1)
                mae_tx_step = round(float(np.abs(nn_preds["timexer"][:, idx] - true_new[:, idx]).mean()), 1)
                mae_sm_step = round(float(np.abs(nn_preds["smamba"][:, idx] - true_new[:, idx]).mean()), 1)
                mae_ens_step = round(float(np.abs(p_ens[:, idx] - true_new[:, idx]).mean()), 1)
                horizon_mae_records.append({
                    "task_code": task_code,
                    "horizon_step": step,
                    "horizon_name": f"Ngày +{step}",
                    "lightgbm_mae": mae_lgb_step,
                    "s4d_mae": mae_s4d_step,
                    "timexer_mae": mae_tx_step,
                    "smamba_mae": mae_sm_step,
                    "ensemble_mae": mae_ens_step
                })

    # ==========================================================================
    # CẬP NHẬT DỮ LIỆU VÀO LAKEHOUSE GOLD TRINO
    # ==========================================================================
    print("\n" + "=" * 80)
    print("NẠP DỮ LIỆU ĐÁNH GIÁ MÔ HÌNH VÀO TRINO ICEBERG GOLD")
    print("=" * 80)

    # 1. Bảng fact_forecast_benchmark
    print("[+] Tạo / Làm mới bảng iceberg.gold.fact_forecast_benchmark...")
    execute_trino_query("DROP TABLE IF EXISTS iceberg.gold.fact_forecast_benchmark")
    ddl_bench = """
    CREATE TABLE iceberg.gold.fact_forecast_benchmark (
        task_code VARCHAR,
        task_name VARCHAR,
        unit VARCHAR,
        model_name VARCHAR,
        model_type VARCHAR,
        mae DOUBLE,
        rmse DOUBLE,
        mape_pct DOUBLE,
        wape_pct DOUBLE,
        accuracy_pct DOUBLE,
        r2_score DOUBLE,
        dm_stat_vs_lgb DOUBLE,
        dm_p_value_vs_lgb DOUBLE,
        rank_order INTEGER
    ) WITH (
        format = 'PARQUET'
    )
    """
    execute_trino_query(ddl_bench)

    batch_vals = []
    for r in benchmark_records:
        val_str = (
            f"('{r['task_code']}', '{r['task_name']}', '{r['unit']}', '{r['model_name']}', "
            f"'{r['model_type']}', {r['mae']}, {r['rmse']}, {r['mape_pct']}, {r['wape_pct']}, "
            f"{r['accuracy_pct']}, {r['r2_score']}, {r['dm_stat']}, {r['dm_pval']}, {r['rank_order']})"
        )
        batch_vals.append(val_str)

    insert_bench_sql = "INSERT INTO iceberg.gold.fact_forecast_benchmark VALUES\n" + ",\n".join(batch_vals)
    execute_trino_query(insert_bench_sql)
    print(f"[✓] Đã nạp thành công {len(benchmark_records)} bản ghi vào iceberg.gold.fact_forecast_benchmark!")

    # 2. Bảng fact_forecast_horizon_mae
    print("[+] Tạo / Làm mới bảng iceberg.gold.fact_forecast_horizon_mae...")
    execute_trino_query("DROP TABLE IF EXISTS iceberg.gold.fact_forecast_horizon_mae")
    ddl_horizon = """
    CREATE TABLE iceberg.gold.fact_forecast_horizon_mae (
        task_code VARCHAR,
        horizon_step INTEGER,
        horizon_name VARCHAR,
        lightgbm_mae DOUBLE,
        s4d_mae DOUBLE,
        timexer_mae DOUBLE,
        smamba_mae DOUBLE,
        ensemble_mae DOUBLE
    ) WITH (
        format = 'PARQUET'
    )
    """
    execute_trino_query(ddl_horizon)

    h_vals = []
    for h in horizon_mae_records:
        val_str = (
            f"('{h['task_code']}', {h['horizon_step']}, '{h['horizon_name']}', "
            f"{h['lightgbm_mae']}, {h['s4d_mae']}, {h['timexer_mae']}, {h['smamba_mae']}, {h['ensemble_mae']})"
        )
        h_vals.append(val_str)

    insert_h_sql = "INSERT INTO iceberg.gold.fact_forecast_horizon_mae VALUES\n" + ",\n".join(h_vals)
    execute_trino_query(insert_h_sql)
    print(f"[✓] Đã nạp thành công {len(horizon_mae_records)} bản ghi vào iceberg.gold.fact_forecast_horizon_mae!")

    # 3. Lưu JSON & CSV
    out_json = os.path.join(CODE_DIR, "benchmark_high_corr_results.json")
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump({
            "generated_at": datetime.now().isoformat(),
            "benchmarks": benchmark_records,
            "horizon_mae": horizon_mae_records
        }, f, indent=2, ensure_ascii=False)
    print(f"[✓] Đã lưu kết quả chi tiết ra {out_json}")

    print("\n" + "=" * 80)
    print(f"HOÀN TẤT TOÀN BỘ CHƯƠNG TRÌNH TRONG {time.time() - start_total_time:.1f} GIÂY!")
    print("=" * 80)


if __name__ == "__main__":
    main()
