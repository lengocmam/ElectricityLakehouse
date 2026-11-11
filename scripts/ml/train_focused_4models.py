#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
train_focused_4models.py
Huấn luyện và đánh giá tập trung 4 MÔ HÌNH theo yêu cầu luận văn:
  1. Ensemble (Tổ hợp đa mô hình)
  2. S4D (Deep Learning - Structured State Space Model)
  3. S-Mamba (Deep Learning - Selective State Space Model)
  4. Ridge Regression (Tuyến tính chính quy hóa)
Mô hình đối chứng: LightGBM (dùng làm cơ sở kiểm định Diebold-Mariano, không đưa vào bảng 4 mô hình).

Đánh giá trên tập kiểm thử 288 ngày (2026-01-01 đến 2026-10-15), không rò rỉ dữ liệu.
4 bài toán:
  - load_H1: Phụ tải ngày tới (MW)
  - load_H7: Phụ tải 7 ngày tới (MW)
  - hydro_H1: Thủy điện ngày tới (MWh)
  - hydro_H7: Thủy điện 7 ngày tới (MWh)
"""

import os
import sys

# Khắc phục lỗi đa luồng và đếm nhân CPU trên môi trường Windows sandbox
os.environ["LOKY_MAX_CPU_COUNT"] = "1"
os.environ["OMP_NUM_THREADS"] = "1"
os.environ["MKL_NUM_THREADS"] = "1"

# BẮT BUỘC: Import torch đầu tiên trước numpy, pandas, scipy, lightgbm để tránh xung đột DLL c10.dll trên Windows
import torch

if hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

import json
import time
from types import SimpleNamespace
from datetime import datetime

import numpy as np
import pandas as pd
from scipy import stats

CODE_DIR = "s4_3thuat_toan/code"
if CODE_DIR not in sys.path:
    sys.path.insert(0, CODE_DIR)

from data import prepare
from models import build_model
from baselines import ridge_fit_predict, lightgbm_fit_predict
from train2 import run_nn, dm_test


def calc_metrics(pred, true):
    """Tính các chỉ số đánh giá sai số và độ chính xác."""
    diff = pred - true
    mae = float(np.abs(diff).mean())
    rmse = float(np.sqrt((diff ** 2).mean()))
    denom = np.maximum(np.abs(true), 1e-9)
    mape = float((np.abs(diff) / denom).mean() * 100.0)
    wape = float(np.abs(diff).sum() / denom.sum() * 100.0)
    
    ss_tot = float(((true - true.mean()) ** 2).sum())
    ss_res = float((diff ** 2).sum())
    r2 = float(1.0 - (ss_res / max(ss_tot, 1e-9)))
    return {
        "mae": round(mae, 1),
        "rmse": round(rmse, 1),
        "mape": round(mape, 2),
        "r2": round(r2, 4)
    }


def load_best_cfg(task_code: str) -> dict:
    cfg_file = os.path.join(CODE_DIR, f"best_configs/{task_code}.json")
    if os.path.exists(cfg_file):
        with open(cfg_file, "r", encoding="utf-8") as f:
            return json.load(f).get("best_config", {})
    return {}


def main():
    print("=" * 85)
    print("CHƯƠNG TRÌNH HUẤN LUYỆN & ĐÁNH GIÁ 4 MÔ HÌNH HỌC MÁY / HỌC SÂU TRỌNG TÂM")
    print("Danh mục 4 mô hình: Ensemble | S4D | S-Mamba | Ridge Regression")
    print("Mô hình chuẩn đối chứng kiểm định Diebold-Mariano: LightGBM")
    print("=" * 85)
    t_start = time.time()

    csv_path = os.path.join(CODE_DIR, "unified_daily_v2.csv")
    out_dir = os.path.join(CODE_DIR, "results", "focused_4models")
    os.makedirs(out_dir, exist_ok=True)

    tasks = [
        ("load_H1", "Phụ tải ngày tới (H = 1)", "MW", "load_mean_mw", ["load_high_corr"], 1),
        ("load_H7", "Phụ tải 7 ngày tới (H = 7)", "MW", "load_mean_mw", ["load_high_corr"], 7),
        ("hydro_H1", "Thủy điện ngày tới (H = 1)", "MWh", "gen_hydro", ["hydro_high_corr"], 1),
        ("hydro_H7", "Thủy điện 7 ngày tới (H = 7)", "MWh", "gen_hydro", ["hydro_high_corr"], 7),
    ]

    results_all = {}

    for task_code, task_title, unit, target, feature_group, horizon in tasks:
        print(f"\n>>> [BÀI TOÁN: {task_title.upper()} - ĐƠN VỊ: {unit}]")
        print("-" * 85)

        # 1. Chuẩn bị dữ liệu (Train: 761 ngày, Val: 184 ngày, Test: 288 ngày)
        prep = prepare(csv_path, target, feature_group, lookback=28, horizon=horizon)
        W = prep.win
        te_mask = (W.split == 2)
        true_test = W.Y[te_mask] * prep.t_std + prep.t_mean
        n_test = int(np.sum(te_mask))
        te_dates = W.dates[te_mask]
        print(f"  [+] Tập Test: {n_test} ngày | Số chiều đặc trưng: {len(prep.feature_names)}")

        best_cfg = load_best_cfg(task_code)

        # 2. Huấn luyện LightGBM (mô hình đối chứng DM)
        lgb_cfg = best_cfg.get("lightgbm", {})
        lags_lgb = lgb_cfg.get("lags", 14)
        print(f"  [+] Đang chạy mô hình đối chứng LightGBM (lags={lags_lgb})...")
        p_lgb_norm = lightgbm_fit_predict(W.X, W.Y, W.split, horizon, lags=lags_lgb, over=lgb_cfg, Fut=W.F)
        p_lgb = p_lgb_norm[te_mask] * prep.t_std + prep.t_mean
        m_lgb = calc_metrics(p_lgb, true_test)
        print(f"      -> LightGBM Baseline: MAE={m_lgb['mae']} {unit}, MAPE={m_lgb['mape']}%, R2={m_lgb['r2']}")

        # 3. Huấn luyện Mô hình 1: Ridge Regression
        print("  [+] Huấn luyện Mô hình 1/4: Ridge Regression...")
        p_ridge_norm, _ = ridge_fit_predict(W.X, W.Y, W.split, horizon, lags=14, Fut=W.F)
        p_ridge = p_ridge_norm[te_mask] * prep.t_std + prep.t_mean

        # 4. Huấn luyện Mô hình 2: S4D (Structured State Space Model)
        print("  [+] Huấn luyện Mô hình 2/4: S4D (SSM)...")
        s4d_cfg = best_cfg.get("s4d", {})
        args_s4d = SimpleNamespace(
            horizon=horizon,
            lookback=s4d_cfg.get("lookback", 28),
            epochs=20,
            patience=6,
            batch=64,
            d_model=s4d_cfg.get("d_model", 64),
            n_layers=s4d_cfg.get("n_layers", 2),
            d_state=s4d_cfg.get("d_state", 16),
            dropout=s4d_cfg.get("dropout", 0.1),
            dt_min=s4d_cfg.get("dt_min", 0.01),
            dt_max=s4d_cfg.get("dt_max", 0.5),
            lr=s4d_cfg.get("lr", 2e-3),
            wd=s4d_cfg.get("wd", 0.05),
            future=1,
            anchor="none"
        )
        p_s4d_norm, *_ = run_nn(prep, args_s4d, "s4d", seed=42, device="cpu")
        p_s4d = p_s4d_norm * prep.t_std + prep.t_mean

        # 5. Huấn luyện Mô hình 3: S-Mamba (Selective State Space Model)
        print("  [+] Huấn luyện Mô hình 3/4: S-Mamba (Selective SSM)...")
        sm_cfg = best_cfg.get("smamba", {})
        args_sm = SimpleNamespace(
            horizon=horizon,
            lookback=sm_cfg.get("lookback", 28),
            epochs=20,
            patience=6,
            batch=64,
            d_model=sm_cfg.get("d_model", 64),
            n_layers=sm_cfg.get("n_layers", 2),
            d_state=sm_cfg.get("d_state", 16),
            dropout=sm_cfg.get("dropout", 0.1),
            lr=sm_cfg.get("lr", 2e-3),
            wd=sm_cfg.get("wd", 0.05),
            future=1,
            anchor="none"
        )
        p_sm_norm, *_ = run_nn(prep, args_sm, "smamba", seed=42, device="cpu")
        p_sm = p_sm_norm * prep.t_std + prep.t_mean

        # 6. Huấn luyện Mô hình 4: Ensemble (Tổ hợp đa mô hình)
        print("  [+] Tạo Mô hình 4/4: Ensemble Tổ hợp...")
        if "load" in task_code:
            # Đối với phụ tải: kết hợp trọng số tối ưu giữa cây (LightGBM) và mạng SSM (S4D, S-Mamba)
            p_ens = 0.40 * p_lgb + 0.35 * p_s4d + 0.25 * p_sm
        else:
            # Đối với thủy điện: kết hợp giữa S-Mamba, S4D và Ridge
            p_ens = 0.45 * p_sm + 0.35 * p_lgb + 0.20 * p_ridge

        # Tổng hợp 4 mô hình trọng tâm
        models_dict = {
            "Ensemble": ("Tổ hợp", p_ens),
            "S4D": ("Deep Learning (SSM)", p_s4d),
            "S-Mamba": ("Deep Learning (Selective SSM)", p_sm),
            "Ridge Regression": ("Tuyến tính chính quy hóa", p_ridge),
        }

        task_eval = []
        for name, (school, p) in models_dict.items():
            m = calc_metrics(p, true_test)
            stat, pval = dm_test(
                (p - prep.t_mean) / prep.t_std,
                (p_lgb - prep.t_mean) / prep.t_std,
                (true_test - prep.t_mean) / prep.t_std,
                horizon
            )
            sign = "+" if stat > 0 else ""
            p_str = "< 0.001" if pval < 0.001 else f"= {pval:.3f}"
            dm_str = f"DM = {sign}{stat:.2f}, p {p_str}"
            
            task_eval.append({
                "model": name,
                "school": school,
                "mae": m["mae"],
                "rmse": m["rmse"],
                "mape": m["mape"],
                "r2": m["r2"],
                "dm_stat": round(stat, 2),
                "dm_pval": round(pval, 4),
                "dm_text": dm_str
            })

        # Xếp hạng theo MAE
        task_eval.sort(key=lambda x: x["mae"])
        for idx, item in enumerate(task_eval, start=1):
            item["rank"] = idx

        results_all[task_code] = {
            "title": task_title,
            "unit": unit,
            "horizon": horizon,
            "n_test": n_test,
            "lightgbm_control": m_lgb,
            "models": task_eval
        }

        # Lưu file dự báo CSV
        df_preds = pd.DataFrame({
            "date": te_dates,
            "actual": true_test[:, 0] if horizon > 1 else true_test.squeeze(),
            "pred_ensemble": p_ens[:, 0] if horizon > 1 else p_ens.squeeze(),
            "pred_s4d": p_s4d[:, 0] if horizon > 1 else p_s4d.squeeze(),
            "pred_smamba": p_sm[:, 0] if horizon > 1 else p_sm.squeeze(),
            "pred_ridge": p_ridge[:, 0] if horizon > 1 else p_ridge.squeeze(),
            "control_lightgbm": p_lgb[:, 0] if horizon > 1 else p_lgb.squeeze(),
        })
        csv_out_path = os.path.join(out_dir, f"preds_{task_code}.csv")
        df_preds.to_csv(csv_out_path, index=False)
        print(f"  [✓] Đã lưu chuỗi dự báo ra: {csv_out_path}")

        # In bảng kết quả chuẩn hóa
        print(f"\n  BẢNG XẾP HẠNG 4 MÔ HÌNH ({task_title}):")
        print(f"  {'Hạng':<5} {'Mô hình':<18} {'Trường phái':<30} {'MAE':>10} {'RMSE':>10} {'MAPE%':>8} {'R2':>8}   {'Diebold-Mariano (vs LightGBM)'}")
        print("  " + "-" * 115)
        for r in task_eval:
            print(f"  {r['rank']:<5} {r['model']:<18} {r['school']:<30} {r['mae']:>10.1f} {r['rmse']:>10.1f} {r['mape']:>8.2f} {r['r2']:>8.4f}   {r['dm_text']}")

    # Lưu kết quả tổng hợp JSON
    json_path = os.path.join(out_dir, "focused_4models_results.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump({
            "generated_at": datetime.now().isoformat(),
            "tasks": results_all
        }, f, indent=2, ensure_ascii=False)

    print("\n" + "=" * 85)
    print(f"[✓] HOÀN TẤT HUẤN LUYỆN TOÀN BỘ 4 MÔ HÌNH TRONG {time.time() - t_start:.1f} GIÂY!")
    print(f"[✓] File kết quả tổng hợp: {json_path}")
    print("=" * 85)


if __name__ == "__main__":
    main()
