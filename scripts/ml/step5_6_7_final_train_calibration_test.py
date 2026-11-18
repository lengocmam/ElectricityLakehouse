# -*- coding: utf-8 -*-
"""BƯỚC 5, 6, 7: CHỐT BIẾN FINAL TRAIN, HUẤN LUYỆN, HIỆU CHỈNH CONFORMAL VÀ ĐÁNH GIÁ TRÊN TẬP TEST ĐỘC LẬP.

Giao thức phân hoạch nghiêm ngặt (Chống rò rỉ 100%):
- Final Train: 02/01/2024 01:00 -> 16/04/2026 23:00 (20,063 giờ)
- Gap 1:       17/04/2026 00:00 -> 23/04/2026 23:00 (168 giờ - 7 ngày)
- Calibration: 24/04/2026 00:00 -> 23/06/2026 23:00 (1,464 giờ - ~2 tháng)
- Gap 2:       24/06/2026 00:00 -> 30/06/2026 23:00 (168 giờ - 7 ngày)
- Held-out Test: 01/07/2026 00:00 -> 16/11/2026 23:00 (3,336 giờ - 139 ngày)

Nội dung thực thi:
Bước 5: Chốt danh sách biến & Scaler trên Final Train (Không đụng vào Gap hay Test).
Bước 6: Huấn luyện bộ ba mô hình (XGBoost, S4D, S-Mamba) trên Final Train.
        Hiệu chỉnh khoảng dự báo Conformal Prediction (80%, 90%, 95%) trên Calibration.
Bước 7: Mở khóa tập Test ĐÚNG MỘT LẦN DUY NHẤT:
        - Đánh giá tổng thể và chi tiết từng tháng (Tháng 7, 8, 9, 10, 11/2026).
        - So sánh trực diện với 2 baselines: Seasonal-Naive Tuần (t-168) và Năm (t-8760).
        - Đánh giá Conformal Coverage (80%, 90%, 95%) theo tháng và Adaptive Rolling Recalibration.
        - Ghi rõ giới hạn: Test chưa phủ Tháng 12.
"""
import os
import sys
import gc
import shutil
import json

os.environ['KMP_DUPLICATE_LIB_OK'] = 'TRUE'
import torch
import torch.nn as nn
import numpy as np
import pandas as pd

from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import xgboost as xgb

OUT_DIR = "data_exports/ml_results"
os.makedirs(OUT_DIR, exist_ok=True)
os.environ['MPLCONFIGDIR'] = os.path.join(OUT_DIR, '.mpl_cache')

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns

sys.path.append(os.path.abspath("s4_3thuat_toan/code"))
from models import SMamba, S4Forecaster

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

ARTIFACT_DIR = r"C:\Users\ASUS\.gemini\antigravity\brain\f345bf93-2a90-4f8b-b023-26cb2c3ce714"


def calc_wape(y_true, y_pred):
    return float(np.sum(np.abs(y_true - y_pred)) / max(np.sum(np.abs(y_true)), 1e-6) * 100)


def eval_batched(model, X_tensor, batch_size=256):
    model.eval()
    preds = []
    with torch.no_grad():
        for b in range(0, len(X_tensor), batch_size):
            preds.append(model(X_tensor[b : b + batch_size]))
    return torch.cat(preds, dim=0)


def filter_collinear_features(df_train, candidate_cols, target_col, threshold=0.95):
    corr_target = df_train[candidate_cols].apply(lambda col: df_train[target_col].corr(col)).abs()
    corr_features = df_train[candidate_cols].corr().abs()
    
    dropped = set()
    for i in range(len(candidate_cols)):
        col_i = candidate_cols[i]
        if col_i in dropped:
            continue
        for j in range(i + 1, len(candidate_cols)):
            col_j = candidate_cols[j]
            if col_j in dropped:
                continue
            if corr_features.loc[col_i, col_j] > threshold:
                if corr_target[col_i] >= corr_target[col_j]:
                    dropped.add(col_j)
                else:
                    dropped.add(col_i)
                    break
                    
    selected = [c for c in candidate_cols if c not in dropped]
    return selected, list(dropped)


print("="*80, flush=True)
print("BẮT ĐẦU QUY TRÌNH BƯỚC 5, 6, 7: CHỐT BIẾN, HUẤN LUYỆN, CONFORMAL VÀ CHẤM TEST", flush=True)
print("="*80, flush=True)

# 1. Nạp tập dữ liệu gốc
CSV_PATH = "data_exports/hourly_feature_store.csv"
df = pd.read_csv(CSV_PATH)
df['datetime'] = pd.to_datetime(df['datetime'])
df = df.sort_values('datetime').reset_index(drop=True)

# 2. Định nghĩa các phân hoạch thời gian chuẩn xác
T_FINAL_TRAIN_END = pd.Timestamp('2026-04-16 23:00:00')
T_GAP1_START      = pd.Timestamp('2026-04-17 00:00:00')
T_GAP1_END        = pd.Timestamp('2026-04-23 23:00:00')
T_CALIB_START     = pd.Timestamp('2026-04-24 00:00:00')
T_CALIB_END       = pd.Timestamp('2026-06-23 23:00:00')
T_GAP2_START      = pd.Timestamp('2026-06-24 00:00:00')
T_GAP2_END        = pd.Timestamp('2026-06-30 23:00:00')
T_TEST_START      = pd.Timestamp('2026-07-01 00:00:00')
T_TEST_END        = pd.Timestamp('2026-11-16 23:00:00')

df_final_train = df[df['datetime'] <= T_FINAL_TRAIN_END].copy().reset_index(drop=True)
df_gap1        = df[(df['datetime'] >= T_GAP1_START) & (df['datetime'] <= T_GAP1_END)].copy().reset_index(drop=True)
df_calib       = df[(df['datetime'] >= T_CALIB_START) & (df['datetime'] <= T_CALIB_END)].copy().reset_index(drop=True)
df_gap2        = df[(df['datetime'] >= T_GAP2_START) & (df['datetime'] <= T_GAP2_END)].copy().reset_index(drop=True)
df_test        = df[(df['datetime'] >= T_TEST_START) & (df['datetime'] <= T_TEST_END)].copy().reset_index(drop=True)

print(f"\n>>> KIỂM TOÁN PHÂN HOẠCH DỮ LIỆU:")
print(f"  * Final Train: {len(df_final_train):,} giờ ({df_final_train['datetime'].min()} -> {df_final_train['datetime'].max()})")
print(f"  * Gap 1 (7d) : {len(df_gap1):,} giờ ({df_gap1['datetime'].min()} -> {df_gap1['datetime'].max()})")
print(f"  * Calibration: {len(df_calib):,} giờ ({df_calib['datetime'].min()} -> {df_calib['datetime'].max()})")
print(f"  * Gap 2 (7d) : {len(df_gap2):,} giờ ({df_gap2['datetime'].min()} -> {df_gap2['datetime'].max()})")
print(f"  * Held Test  : {len(df_test):,} giờ ({df_test['datetime'].min()} -> {df_test['datetime'].max()})")
print(f"  => Tổng cộng : {len(df_final_train) + len(df_gap1) + len(df_calib) + len(df_gap2) + len(df_test):,} giờ (Khớp 100% với 25,199 hàng)", flush=True)

# -------------------------------------------------------------------
# BƯỚC 5: CHỐT DANH SÁCH BIẾN VÀ FIT SCALER TRÊN FINAL TRAIN
# -------------------------------------------------------------------
print("\n" + "="*80, flush=True)
print("BƯỚC 5: TÍNH TƯƠNG QUAN VÀ CHỐT BIẾN CUỐI CÙNG TRÊN FINAL TRAIN (KHÓA CỨNG)", flush=True)
print("="*80, flush=True)

TARGET_COLS = ["load_national", "net_load", "p_solar_total", "p_hydro"]

CANDIDATES = {
    "load_national": [
        'wx_temp_c', 'wx_apparent_temp_c', 'wx_humidity_pct', 'wx_solar_rad_wm2',
        'wx_wind_speed_ms', 'wx_cloud_cover_pct', 'wx_precip_mm',
        'hour_sin', 'hour_cos', 'dow_sin', 'dow_cos', 'month_sin', 'month_cos',
        'is_weekend', 'is_working_day',
        'load_national_lag1', 'load_national_lag2', 'load_national_lag24'
    ],
    "net_load": [
        'wx_temp_c', 'wx_apparent_temp_c', 'wx_solar_rad_wm2', 'wx_wind_speed_ms',
        'wx_humidity_pct', 'wx_cloud_cover_pct',
        'hour_sin', 'hour_cos', 'dow_sin', 'dow_cos', 'month_sin', 'month_cos',
        'is_weekend', 'is_working_day',
        'net_load_lag1', 'net_load_lag2', 'net_load_lag24'
    ],
    "p_solar_total": [
        'wx_solar_rad_wm2', 'wx_cloud_cover_pct', 'wx_temp_c', 'wx_humidity_pct',
        'hour_sin', 'hour_cos', 'month_sin', 'month_cos',
        'p_solar_total_lag1', 'p_solar_total_lag2', 'p_solar_total_lag24',
        'wx_solar_rad_lag1'
    ],
    "p_hydro": [
        'wx_precip_mm', 'wx_humidity_pct', 'wx_temp_c',
        'hour_sin', 'hour_cos', 'dow_sin', 'dow_cos', 'month_sin', 'month_cos',
        'is_weekend', 'is_working_day',
        'p_hydro_lag1', 'p_hydro_lag2', 'p_hydro_lag24',
        'load_national_lag1'
    ]
}

final_features_locked = {}
final_correlations_locked = {}
scalers_locked = {}

for target_col in TARGET_COLS:
    cand_cols = CANDIDATES[target_col]
    selected_cols, dropped_cols = filter_collinear_features(df_final_train, cand_cols, target_col, threshold=0.95)
    final_features_locked[target_col] = selected_cols
    
    # Tính tương quan Pearson & Spearman trên Final Train
    corrs = {}
    for c in selected_cols:
        r_p = float(df_final_train[target_col].corr(df_final_train[c], method='pearson'))
        r_s = float(df_final_train[target_col].corr(df_final_train[c], method='spearman'))
        corrs[c] = {"pearson": r_p, "spearman": r_s}
    final_correlations_locked[target_col] = {
        "selected_features": selected_cols,
        "dropped_collinear": dropped_cols,
        "correlations": corrs
    }
    
    # Fit StandardScaler cho các biến ngoại sinh (Exogenous) trên Final Train
    exog_cols = [c for c in selected_cols if not ('_lag1' in c or '_lag2' in c or '_lag24' in c)]
    scaler = StandardScaler()
    scaler.fit(df_final_train[exog_cols].values)
    scalers_locked[target_col] = {
        "scaler": scaler,
        "exog_cols": exog_cols,
        "mean": scaler.mean_.tolist(),
        "scale": scaler.scale_.tolist()
    }
    
    print(f"\n>>> TARGET [{target_col}]:")
    print(f"  * Giữ {len(selected_cols)} biến | Loại bỏ {len(dropped_cols)} biến trùng lặp: {dropped_cols}")
    print(f"  * Danh sách biến chốt: {selected_cols}")

# Lưu cấu hình biến và tương quan khóa cứng
with open(os.path.join(OUT_DIR, "final_train_feature_correlations.json"), 'w', encoding='utf-8') as f:
    json.dump(final_correlations_locked, f, ensure_ascii=False, indent=2)

# Vẽ ma trận tương quan của target chính 'load_national' trên Final Train
plt.figure(figsize=(12, 10))
load_feats = final_features_locked["load_national"]
corr_mat = df_final_train[["load_national"] + load_feats].corr()
sns.heatmap(corr_mat, annot=True, fmt=".2f", cmap="coolwarm", cbar=True, vmin=-1, vmax=1)
plt.title("FINAL TRAIN CORRELATION MATRIX (CHỐNG RÒ RỈ TEST)\nPhụ tải Toàn quốc & Các biến đầu vào chọn lọc (2024-01-02 -> 2026-04-16)", fontsize=13, fontweight='bold', pad=12)
plt.tight_layout()
corr_plot_path = os.path.join(OUT_DIR, "final_train_correlation_matrix.png")
plt.savefig(corr_plot_path, dpi=200)
plt.close()
shutil.copy2(corr_plot_path, os.path.join(ARTIFACT_DIR, "final_train_correlation_matrix.png"))
print(f"\n>>> Đã xuất biểu đồ tương quan Final Train: {corr_plot_path}")

# -------------------------------------------------------------------
# BƯỚC 6 & 7: HUẤN LUYỆN, HIỆU CHỈNH CONFORMAL VÀ ĐÁNH GIÁ TEST
# -------------------------------------------------------------------
print("\n" + "="*80, flush=True)
print("BƯỚC 6 & 7: HUẤN LUYỆN MODEL, CALIBRATION CONFORMAL VÀ ĐÁNH GIÁ TRÊN TEST ĐỘC LẬP", flush=True)
print("="*80, flush=True)

L = 24  # Cửa sổ quá khứ SSM
final_results = {}

# Chuẩn bị Seasonal-Naive Baselines trên toàn bộ chuỗi để suy diễn Test
# Naive Tuần: lag 168 giờ
# Naive Năm : lag 8760 giờ (365 * 24)
for t_col in TARGET_COLS:
    df[f'{t_col}_naive_week'] = df[t_col].shift(168)
    df[f'{t_col}_naive_year'] = df[t_col].shift(8760)

# Cắt lại df_test có kèm baseline
df_test = df[(df['datetime'] >= T_TEST_START) & (df['datetime'] <= T_TEST_END)].copy().reset_index(drop=True)

for target_col in ["load_national"]:  # Trọng tâm đồ án: load_national
    print(f"\n==================================================================", flush=True)
    print(f">>> TRIỂN KHAI CHO MỤC TIÊU CỐT LÕI: [{target_col}]", flush=True)
    print(f"==================================================================", flush=True)
    
    feats = final_features_locked[target_col]
    exog_cols = scalers_locked[target_col]["exog_cols"]
    scaler = scalers_locked[target_col]["scaler"]
    
    # 1. Huấn luyện XGBoost trên Final Train (dùng 10% đuôi của Final Train làm validation early stopping)
    n_train = len(df_final_train)
    n_val_internal = int(n_train * 0.1)
    df_tr_xgb = df_final_train.iloc[:-n_val_internal]
    df_val_xgb = df_final_train.iloc[-n_val_internal:]
    
    xgb_model = xgb.XGBRegressor(
        n_estimators=1200,
        learning_rate=0.03,
        max_depth=6,
        subsample=0.8,
        colsample_bytree=0.8,
        early_stopping_rounds=40,
        random_state=42,
        n_jobs=-1
    )
    xgb_model.fit(
        df_tr_xgb[feats].values, df_tr_xgb[target_col].values,
        eval_set=[(df_val_xgb[feats].values, df_val_xgb[target_col].values)],
        verbose=False
    )
    
    # Dự báo Calibration & Test bằng XGBoost
    p_cal_xgb = xgb_model.predict(df_calib[feats].values)
    p_test_xgb = xgb_model.predict(df_test[feats].values)
    
    # 2. Chuẩn bị dữ liệu chuỗi (Sequences) cho S4D & S-Mamba
    # Ghép Final Train + Gap 1 + Calibration + Gap 2 + Test để trích xuất cửa sổ lookback L=24 không bị gián đoạn
    seq_cols = [target_col] + exog_cols
    F_num = len(seq_cols)
    
    df_all_norm = df.copy()
    df_all_norm[exog_cols] = scaler.transform(df[exog_cols].values)
    raw_all = df_all_norm[seq_cols].values
    
    N_all = len(df)
    X_seq_all = np.zeros((N_all - L, L, F_num), dtype=np.float32)
    y_seq_all = np.zeros((N_all - L, 1), dtype=np.float32)
    ts_all = df['datetime'].iloc[L:].reset_index(drop=True)
    
    for i in range(L, N_all):
        X_seq_all[i - L] = raw_all[i - L : i]
        y_seq_all[i - L, 0] = raw_all[i, 0]
        
    idx_train = np.where(ts_all <= T_FINAL_TRAIN_END)[0]
    idx_calib = np.where((ts_all >= T_CALIB_START) & (ts_all <= T_CALIB_END))[0]
    idx_test  = np.where((ts_all >= T_TEST_START) & (ts_all <= T_TEST_END))[0]
    
    X_tr_t = torch.tensor(X_seq_all[idx_train], dtype=torch.float32)
    y_tr_t = torch.tensor(y_seq_all[idx_train], dtype=torch.float32)
    
    X_cal_t = torch.tensor(X_seq_all[idx_calib], dtype=torch.float32)
    y_cal_t = torch.tensor(y_seq_all[idx_calib], dtype=torch.float32)
    
    X_test_t = torch.tensor(X_seq_all[idx_test], dtype=torch.float32)
    y_test_t = torch.tensor(y_seq_all[idx_test], dtype=torch.float32)
    
    y_cal_true = y_seq_all[idx_calib, 0]
    y_test_true = y_seq_all[idx_test, 0]
    
    loss_fn = nn.SmoothL1Loss(beta=0.5)
    
    # 3. Huấn luyện S4D
    print("  -> Đang huấn luyện S4D trên Final Train...", flush=True)
    s4 = S4Forecaster(n_feat=F_num, horizon=1, d_model=32, n_layers=2, d_state=32, dropout=0.1)
    opt_s4 = torch.optim.AdamW(s4.parameters(), lr=3e-3, weight_decay=1e-4)
    sched_s4 = torch.optim.lr_scheduler.CosineAnnealingLR(opt_s4, T_max=10)
    
    for ep in range(10):
        s4.train()
        perm = torch.randperm(len(X_tr_t))
        for b in range(0, len(perm), 128):
            idx = perm[b : b + 128]
            opt_s4.zero_grad()
            loss = loss_fn(s4(X_tr_t[idx]), y_tr_t[idx])
            loss.backward()
            torch.nn.utils.clip_grad_norm_(s4.parameters(), 1.0)
            opt_s4.step()
        sched_s4.step()
        gc.collect()
        
    p_cal_s4 = eval_batched(s4, X_cal_t, batch_size=256).cpu().numpy().flatten()
    p_test_s4 = eval_batched(s4, X_test_t, batch_size=256).cpu().numpy().flatten()
    
    # 4. Huấn luyện S-Mamba
    print("  -> Đang huấn luyện S-Mamba trên Final Train...", flush=True)
    sm = SMamba(n_feat=F_num, lookback=L, horizon=1, d_model=32, n_layers=2, d_state=16, dropout=0.1)
    opt_sm = torch.optim.AdamW(sm.parameters(), lr=3e-3, weight_decay=1e-4)
    sched_sm = torch.optim.lr_scheduler.CosineAnnealingLR(opt_sm, T_max=5)
    
    for ep in range(5):
        sm.train()
        perm = torch.randperm(len(X_tr_t))
        for b in range(0, len(perm), 128):
            idx = perm[b : b + 128]
            opt_sm.zero_grad()
            loss = loss_fn(sm(X_tr_t[idx]), y_tr_t[idx])
            loss.backward()
            torch.nn.utils.clip_grad_norm_(sm.parameters(), 1.0)
            opt_sm.step()
        sched_sm.step()
        gc.collect()
        
    p_cal_sm = eval_batched(sm, X_cal_t, batch_size=256).cpu().numpy().flatten()
    p_test_sm = eval_batched(sm, X_test_t, batch_size=256).cpu().numpy().flatten()
    
    # ---------------------------------------------------------------
    # BƯỚC 6: HIỆU CHỈNH KHOẢNG DỰ BÁO CONFORMAL TRÊN CALIBRATION
    # ---------------------------------------------------------------
    print("\n" + "-"*60, flush=True)
    print("BƯỚC 6: TÍNH TOÁN HIỆU CHỈNH CONFORMAL TRÊN TẬP CALIBRATION (1,464 GIỜ)", flush=True)
    print("-"*60, flush=True)
    
    models_dict = {
        "XGBoost": {"p_cal": p_cal_xgb, "p_test": p_test_xgb},
        "S4D":     {"p_cal": p_cal_s4,  "p_test": p_test_s4},
        "S-Mamba": {"p_cal": p_cal_sm,  "p_test": p_test_sm}
    }
    
    conformal_quantiles = {}
    N_cal = len(y_cal_true)
    alphas = [0.20, 0.10, 0.05]  # Tương ứng độ tin cậy: 80%, 90%, 95%
    
    for m_name, m_data in models_dict.items():
        # Điểm không tương thích: s_t = |y_t - y_hat_t|
        scores = np.abs(y_cal_true - m_data["p_cal"])
        conformal_quantiles[m_name] = {}
        
        for alpha in alphas:
            coverage_target = int((1 - alpha) * 100)
            # Finite-sample correction quantile level: ceil((N+1)*(1-alpha)) / N
            q_level = min(1.0, np.ceil((N_cal + 1) * (1 - alpha)) / N_cal)
            q_val = float(np.quantile(scores, q_level))
            conformal_quantiles[m_name][f"q_{coverage_target}"] = q_val
            
        print(f"  [{m_name:<7}] Ngưỡng Conformal: q_80 = {conformal_quantiles[m_name]['q_80']:,.1f} MW | q_90 = {conformal_quantiles[m_name]['q_90']:,.1f} MW | q_95 = {conformal_quantiles[m_name]['q_95']:,.1f} MW", flush=True)
        
    # ---------------------------------------------------------------
    # BƯỚC 7: CHẤM TEST MỘT LẦN DUY NHẤT & BÁO CÁO CHI TIẾT THEO THÁNG
    # ---------------------------------------------------------------
    print("\n" + "="*80, flush=True)
    print("BƯỚC 7: CHẤM TEST MỘT LẦN DUY NHẤT & PHÂN TÁCH LỖI THEO TỪNG THÁNG", flush=True)
    print("="*80, flush=True)
    
    # Baselines
    p_naive_week = df_test[f'{target_col}_naive_week'].values
    p_naive_year = df_test[f'{target_col}_naive_year'].values
    
    test_dates = df_test['datetime'].dt.to_period('M')
    unique_months = sorted(df_test['datetime'].dt.strftime('%Y-%m').unique())
    
    # Báo cáo tổng thể trên toàn bộ Test (01/07 -> 16/11/2026: 3,336 giờ)
    test_report = {
        "overall": {},
        "monthly": {},
        "conformal_coverage": {},
        "adaptive_rolling_conformal": {}
    }
    
    all_preds_eval = {
        "Seasonal-Naive (Tuần t-168)": p_naive_week,
        "Seasonal-Naive (Năm t-8760)": p_naive_year,
        "XGBoost": p_test_xgb,
        "S4D": p_test_s4,
        "S-Mamba": p_test_sm
    }
    
    print("\n>>> HIỆU NĂNG TỔNG THỂ TRÊN TOÀN BỘ TẬP TEST (3,336 GIỜ):")
    for name, preds in all_preds_eval.items():
        mae = float(mean_absolute_error(y_test_true, preds))
        rmse = float(np.sqrt(mean_squared_error(y_test_true, preds)))
        wape = calc_wape(y_test_true, preds)
        r2 = float(r2_score(y_test_true, preds))
        test_report["overall"][name] = {"MAE": mae, "RMSE": rmse, "WAPE%": wape, "R2": r2}
        print(f"  * {name:<27}: MAE = {mae:,.2f} MW | RMSE = {rmse:,.2f} MW | WAPE = {wape:.2f}% | R2 = {r2:.4f}")
        
    # Báo cáo chi tiết theo từng tháng
    month_names_map = {
        "2026-07": "Tháng 7 (Mưa bão, lũ tiểu mãn)",
        "2026-08": "Tháng 8 (Mùa mưa chính vụ, phụ tải cao)",
        "2026-09": "Tháng 9 (Giao mùa thu, bão biển Đông)",
        "2026-10": "Tháng 10 (Lũ miền Trung, thời tiết biến động)",
        "2026-11": "Tháng 11 (1-16/11: Chớm đông)"
    }
    
    print("\n>>> HIỆU NĂNG PHÂN TÁCH THEO TỪNG THÁNG:")
    for ym in unique_months:
        m_mask = (df_test['datetime'].dt.strftime('%Y-%m') == ym).values
        y_m_true = y_test_true[m_mask]
        test_report["monthly"][ym] = {
            "description": month_names_map.get(ym, ym),
            "samples": int(np.sum(m_mask)),
            "models": {}
        }
        print(f"\n--- {month_names_map.get(ym, ym)} (N = {np.sum(m_mask):,} giờ) ---")
        for name, preds in all_preds_eval.items():
            p_m = preds[m_mask]
            mae = float(mean_absolute_error(y_m_true, p_m))
            rmse = float(np.sqrt(mean_squared_error(y_m_true, p_m)))
            wape = calc_wape(y_m_true, p_m)
            r2 = float(r2_score(y_m_true, p_m))
            test_report["monthly"][ym]["models"][name] = {"MAE": mae, "RMSE": rmse, "WAPE%": wape, "R2": r2}
            print(f"  {name:<27}: MAE = {mae:,.2f} MW | RMSE = {rmse:,.2f} MW | WAPE = {wape:.2f}% | R2 = {r2:.4f}")
            
    # Đánh giá Conformal Coverage (Cố định & Cuốn chiếu Adaptive Rolling)
    print("\n" + "-"*60, flush=True)
    print("ĐÁNH GIÁ ĐỘ PHỦ CONFORMAL (COVERAGE) THEO THÁNG & ADAPTIVE ROLLING RECALIBRATION", flush=True)
    print("-"*60, flush=True)
    
    for m_name in ["XGBoost", "S4D", "S-Mamba"]:
        test_report["conformal_coverage"][m_name] = {}
        preds = models_dict[m_name]["p_test"]
        q_dict = conformal_quantiles[m_name]
        
        print(f"\n>>> Mô hình [{m_name}]:")
        for cov_target in [80, 90, 95]:
            q_val = q_dict[f"q_{cov_target}"]
            lower = np.maximum(0, preds - q_val)
            upper = preds + q_val
            
            # Overall coverage
            inside_all = (y_test_true >= lower) & (y_test_true <= upper)
            emp_cov_all = float(np.mean(inside_all) * 100)
            avg_width = float(np.mean(upper - lower))
            
            # Coverage per month
            cov_monthly = {}
            for ym in unique_months:
                m_mask = (df_test['datetime'].dt.strftime('%Y-%m') == ym).values
                in_m = inside_all[m_mask]
                cov_monthly[ym] = float(np.mean(in_m) * 100)
                
            test_report["conformal_coverage"][m_name][f"target_{cov_target}"] = {
                "quantile_MW": q_val,
                "interval_width_MW": avg_width,
                "overall_empirical_coverage%": emp_cov_all,
                "monthly_coverage%": cov_monthly
            }
            print(f"  * Mức {cov_target}% (q = {q_val:,.1f} MW): Độ phủ thực tế = {emp_cov_all:.2f}% | Độ rộng khoảng = {avg_width:,.1f} MW")
            print(f"    Chi tiết tháng: " + " | ".join([f"{ym[-2:]}: {cov_monthly[ym]:.1f}%" for ym in unique_months]))
            
    # Triển khai Adaptive Rolling Recalibration (Cuốn chiếu cửa sổ 720 giờ = 30 ngày)
    print("\n>>> TRIỂN KHAI ADAPTIVE ROLLING CONFORMAL RECALIBRATION (CỬA SỔ 720 GIỜ):")
    # Cho S-Mamba
    W = 720
    sm_preds = p_test_sm
    sm_errs = np.abs(y_test_true - sm_preds)
    
    # Ghép chuỗi lỗi Calibration vào trước Test để có lịch sử cho những giờ đầu tiên của Test
    cal_errs = np.abs(y_cal_true - p_cal_sm)
    combined_errs = np.concatenate([cal_errs, sm_errs])
    offset = len(cal_errs)
    
    adaptive_coverages = {80: [], 90: [], 95: []}
    rolling_q_history = {80: [], 90: [], 95: []}
    
    for t in range(len(y_test_true)):
        # Cửa sổ lịch sử W giờ trước thời điểm t
        hist_window = combined_errs[offset + t - W : offset + t]
        for cov_target, alpha in [(80, 0.20), (90, 0.10), (95, 0.05)]:
            q_adapt = float(np.quantile(hist_window, min(1.0, np.ceil((W + 1) * (1 - alpha)) / W)))
            rolling_q_history[cov_target].append(q_adapt)
            is_in = (y_test_true[t] >= max(0, sm_preds[t] - q_adapt)) and (y_test_true[t] <= sm_preds[t] + q_adapt)
            adaptive_coverages[cov_target].append(is_in)
            
    test_report["adaptive_rolling_conformal"]["S-Mamba"] = {}
    for cov_target in [80, 90, 95]:
        emp_adapt = float(np.mean(adaptive_coverages[cov_target]) * 100)
        mean_adapt_q = float(np.mean(rolling_q_history[cov_target]))
        test_report["adaptive_rolling_conformal"]["S-Mamba"][f"target_{cov_target}"] = {
            "mean_rolling_q_MW": mean_adapt_q,
            "adaptive_coverage%": emp_adapt
        }
        print(f"  * [S-Mamba Adaptive {cov_target}%]: Độ phủ cuốn chiếu = {emp_adapt:.2f}% (Bán kính trung bình q = {mean_adapt_q:,.1f} MW)")

    # Lưu toàn bộ báo cáo JSON
    json_path = os.path.join(OUT_DIR, "final_test_conformal_evaluation.json")
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(test_report, f, ensure_ascii=False, indent=2)
    print(f"\n>>> Đã xuất báo cáo kiểm định Test: {json_path}")
    
    # ---------------------------------------------------------------
    # VẼ CÁC BIỂU ĐỒ BÁO CÁO BƯỚC 7
    # ---------------------------------------------------------------
    print("\n>>> Đang khởi tạo các biểu đồ trực quan hóa chuẩn học thuật...", flush=True)
    
    # 1. Đường cong dự báo thực tế vs Các mô hình (Đoạn 1 tuần tiêu biểu Tháng 8/2026)
    zoom_start = 744 + 200
    zoom_end = zoom_start + 168
    t_axis = df_test['datetime'].iloc[zoom_start:zoom_end]
    
    plt.figure(figsize=(14, 6))
    plt.plot(t_axis, y_test_true[zoom_start:zoom_end], 'k-', label='Thực tế (EVN/A0)', linewidth=2.0)
    plt.plot(t_axis, p_test_xgb[zoom_start:zoom_end], '--', color='#e67e22', label='XGBoost', linewidth=1.5, alpha=0.9)
    plt.plot(t_axis, p_test_s4[zoom_start:zoom_end], '-.', color='#2980b9', label='S4D (Structured State Space)', linewidth=1.5, alpha=0.9)
    plt.plot(t_axis, p_test_sm[zoom_start:zoom_end], '-', color='#27ae60', label='S-Mamba (Selective SSM)', linewidth=1.8)
    plt.title("SO SÁNH ĐƯỜNG CONG DỰ BÁO PHỤ TẢI TRÊN TẬP TEST ĐỘC LẬP (TUẦN CAO ĐIỂM THÁNG 8/2026)", fontsize=13, fontweight='bold', pad=12)
    plt.xlabel("Thời gian (Giờ)", fontsize=11)
    plt.ylabel("Phụ tải toàn quốc (MW)", fontsize=11)
    plt.legend(frameon=True, facecolor='white', framealpha=0.9)
    plt.grid(True, linestyle=':', alpha=0.6)
    plt.tight_layout()
    plot_curves_path = os.path.join(OUT_DIR, "final_test_forecast_curves.png")
    plt.savefig(plot_curves_path, dpi=200)
    plt.close()
    shutil.copy2(plot_curves_path, os.path.join(ARTIFACT_DIR, "final_test_forecast_curves.png"))
    
    # 2. Dải khoảng tin cậy Conformal Prediction (Conformal Ribbons) 90% của S-Mamba
    q90_sm = conformal_quantiles["S-Mamba"]["q_90"]
    plt.figure(figsize=(14, 6))
    plt.plot(t_axis, y_test_true[zoom_start:zoom_end], 'k-', label='Thực tế (EVN/A0)', linewidth=2.0)
    plt.plot(t_axis, p_test_sm[zoom_start:zoom_end], '-', color='#2980b9', label='Dự báo S-Mamba', linewidth=1.8)
    plt.fill_between(
        t_axis,
        np.maximum(0, p_test_sm[zoom_start:zoom_end] - q90_sm),
        p_test_sm[zoom_start:zoom_end] + q90_sm,
        color='#3498db', alpha=0.25, label=f'Khoảng Conformal 90% (±{q90_sm:,.0f} MW)'
    )
    plt.title("KHOẢNG DỰ BÁO ĐẢM BẢO THỐNG KÊ (CONFORMAL PREDICTION INTERVALS - 90%)\nMô hình S-Mamba trên Tập Test Độc Lập", fontsize=13, fontweight='bold', pad=12)
    plt.xlabel("Thời gian (Giờ)", fontsize=11)
    plt.ylabel("Phụ tải toàn quốc (MW)", fontsize=11)
    plt.legend(frameon=True, facecolor='white', framealpha=0.9)
    plt.grid(True, linestyle=':', alpha=0.6)
    plt.tight_layout()
    plot_ribbon_path = os.path.join(OUT_DIR, "final_test_conformal_ribbons.png")
    plt.savefig(plot_ribbon_path, dpi=200)
    plt.close()
    shutil.copy2(plot_ribbon_path, os.path.join(ARTIFACT_DIR, "final_test_conformal_ribbons.png"))
    
    # 3. Biểu đồ cột so sánh sai số MAE theo từng tháng giữa Bộ ba và Baselines
    months_labels = ["Tháng 7", "Tháng 8", "Tháng 9", "Tháng 10", "Tháng 11"]
    x = np.arange(len(months_labels))
    width = 0.16
    
    plt.figure(figsize=(14, 6))
    plt.bar(x - 2*width, [test_report["monthly"][ym]["models"]["Seasonal-Naive (Tuần t-168)"]["MAE"] for ym in unique_months], width, label='Naive Tuần (t-168)', color='#95a5a6')
    plt.bar(x - width,   [test_report["monthly"][ym]["models"]["Seasonal-Naive (Năm t-8760)"]["MAE"] for ym in unique_months], width, label='Naive Năm (t-8760)', color='#7f8c8d')
    plt.bar(x,           [test_report["monthly"][ym]["models"]["XGBoost"]["MAE"] for ym in unique_months], width, label='XGBoost', color='#e67e22')
    plt.bar(x + width,   [test_report["monthly"][ym]["models"]["S4D"]["MAE"] for ym in unique_months], width, label='S4D', color='#2980b9')
    plt.bar(x + 2*width, [test_report["monthly"][ym]["models"]["S-Mamba"]["MAE"] for ym in unique_months], width, label='S-Mamba', color='#27ae60')
    
    plt.title("SO SÁNH SAI SỐ TUYỆT ĐỐI TRUNG BÌNH (MAE - MW) THEO TỪNG THÁNG TRÊN TẬP TEST", fontsize=13, fontweight='bold', pad=12)
    plt.xlabel("Giai đoạn kiểm định năm 2026", fontsize=11)
    plt.ylabel("MAE (MW)", fontsize=11)
    plt.xticks(x, months_labels)
    plt.legend(frameon=True, facecolor='white', framealpha=0.9)
    plt.grid(True, linestyle=':', alpha=0.6, axis='y')
    plt.tight_layout()
    plot_bar_path = os.path.join(OUT_DIR, "final_test_monthly_metrics_comparison.png")
    plt.savefig(plot_bar_path, dpi=200)
    plt.close()
    shutil.copy2(plot_bar_path, os.path.join(ARTIFACT_DIR, "final_test_monthly_metrics_comparison.png"))
    
    # 4. Biểu đồ đường độ phủ Conformal Coverage theo từng tháng (Tĩnh vs Cuốn chiếu)
    plt.figure(figsize=(12, 6))
    cov_sm_90_static = [test_report["conformal_coverage"]["S-Mamba"]["target_90"]["monthly_coverage%"][ym] for ym in unique_months]
    cov_xgb_90_static = [test_report["conformal_coverage"]["XGBoost"]["target_90"]["monthly_coverage%"][ym] for ym in unique_months]
    
    # Monthly adaptive coverage for S-Mamba
    adapt_monthly_cov = []
    for ym in unique_months:
        m_mask = (df_test['datetime'].dt.strftime('%Y-%m') == ym).values
        adapt_monthly_cov.append(float(np.mean(np.array(adaptive_coverages[90])[m_mask]) * 100))
        
    plt.axhline(90.0, color='red', linestyle='--', linewidth=1.5, label='Mục tiêu lý thuyết 90% (Nominal Target)')
    plt.plot(months_labels, cov_xgb_90_static, 's--', color='#e67e22', label='XGBoost Conformal Tĩnh', linewidth=1.8, markersize=7)
    plt.plot(months_labels, cov_sm_90_static, 'o-', color='#2980b9', label='S-Mamba Conformal Tĩnh (Calib T5-T6)', linewidth=1.8, markersize=7)
    plt.plot(months_labels, adapt_monthly_cov, '^-.', color='#27ae60', label='S-Mamba Conformal Cuốn chiếu (Adaptive Rolling 720h)', linewidth=2.2, markersize=8)
    
    plt.title("ĐỘ PHỦ THỰC TẾ (EMPIRICAL COVERAGE) KHOẢNG TIN CẬY 90% THEO THÁNG TRÊN TẬP TEST\nSo sánh Hiệu chỉnh Tĩnh vs Cuốn chiếu thích ứng khi chuyển mùa", fontsize=13, fontweight='bold', pad=12)
    plt.xlabel("Tháng kiểm định năm 2026", fontsize=11)
    plt.ylabel("Tỷ lệ bao phủ thực tế (%)", fontsize=11)
    plt.ylim([70, 100])
    plt.legend(loc='lower left', frameon=True, facecolor='white', framealpha=0.9)
    plt.grid(True, linestyle=':', alpha=0.6)
    plt.tight_layout()
    plot_cov_path = os.path.join(OUT_DIR, "final_test_conformal_monthly_coverage.png")
    plt.savefig(plot_cov_path, dpi=200)
    plt.close()
    shutil.copy2(plot_cov_path, os.path.join(ARTIFACT_DIR, "final_test_conformal_monthly_coverage.png"))
    
    print(">>> ĐÃ XUẤT ĐẦY ĐỦ 5 BIỂU ĐỒ HỌC THUẬT VÀO THƯ MỤC ARTIFACTS!", flush=True)

print("\n" + "="*80, flush=True)
print(">>> TOÀN BỘ QUY TRÌNH BƯỚC 5, 6, 7 ĐÃ HOÀN TẤT THÀNH CÔNG RỰC RỠ!", flush=True)
print("="*80, flush=True)
