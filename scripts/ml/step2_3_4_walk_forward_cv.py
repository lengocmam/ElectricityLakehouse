# -*- coding: utf-8 -*-
"""Bước 2, Bước 3, Bước 4: Chia Walk-Forward CV, Chọn Biến Chống Rò Rỉ và Đánh Giá Mô Hình Trên Dev.

Giao thức Walk-Forward (3 Fold trên Dev: 02/01/2024 -> 23/06/2026):
- Fold 1: Học <= 2024-12-31 | Gap 7 ngày (01/01 -> 07/01/2025) | Thi thử: 08/01/2025 -> 30/04/2025 (Mùa khô)
- Fold 2: Học <= 2025-06-30 | Gap 7 ngày (01/07 -> 07/07/2025) | Thi thử: 08/07/2025 -> 16/11/2025 (CÙNG MÙA VỚI TEST!)
- Fold 3: Học <= 2025-12-31 | Gap 7 ngày (01/01 -> 07/01/2026) | Thi thử: 08/01/2026 -> 23/06/2026 (Đầu 2026 đến hết Dev)

Trong từng fold (Chỉ dùng tập Học của fold đó):
1. Tính ma trận tương quan Pearson (r) và Spearman (rho) với target.
2. Lọc đa cộng tuyến: Cặp biến đầu vào có |r| > 0.95 thì loại biến có tương quan thấp hơn với target.
3. So sánh danh sách biến giữa 3 fold để tìm các biến đồng thuận (Consensus Features).
4. Huấn luyện và đánh giá Bộ ba: XGBoost, S-Mamba, S4D trên 3 fold.
"""
import os
import sys
import gc

os.environ['KMP_DUPLICATE_LIB_OK'] = 'TRUE'
import torch
import torch.nn as nn

import json
import numpy as np
import pandas as pd
from scipy import stats

from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import xgboost as xgb

OUT_DIR = "data_exports/ml_results"
os.makedirs(OUT_DIR, exist_ok=True)
os.environ['MPLCONFIGDIR'] = os.path.join(OUT_DIR, '.mpl_cache')

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

sys.path.append(os.path.abspath("s4_3thuat_toan/code"))
from models import SMamba, S4Forecaster

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')


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
    """Lọc các biến đầu vào có đa cộng tuyến cao (|r| > threshold).
    Giữ lại biến có tương quan cao hơn với biến mục tiêu."""
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
                # Giữ biến tương quan mạnh hơn với target
                if corr_target[col_i] >= corr_target[col_j]:
                    dropped.add(col_j)
                else:
                    dropped.add(col_i)
                    break
                    
    selected = [c for c in candidate_cols if c not in dropped]
    return selected, list(dropped)


# 1. Load Dataset và giới hạn trong phạm vi DEV (Khóa Test)
CSV_PATH = "data_exports/hourly_feature_store.csv"
df = pd.read_csv(CSV_PATH)
df['datetime'] = pd.to_datetime(df['datetime'])
df = df.sort_values('datetime').reset_index(drop=True)

# Khóa cứng phạm vi Dev
dev_cutoff = pd.Timestamp('2026-06-23 23:00:00')
df_dev = df[df['datetime'] <= dev_cutoff].copy().reset_index(drop=True)
print(f">>> Đã khóa Test. Nạp Tập Dev: {len(df_dev):,} giờ ({df_dev['datetime'].min()} -> {df_dev['datetime'].max()})", flush=True)

# Định nghĩa 3 Fold Walk-Forward
FOLDS_DEF = [
    {
        "fold_id": 1,
        "name": "Fold 1 (Mùa khô đầu năm)",
        "train_end": "2024-12-31 23:00:00",
        "gap_start": "2025-01-01 00:00:00",
        "gap_end":   "2025-01-07 23:00:00",
        "val_start": "2025-01-08 00:00:00",
        "val_end":   "2025-04-30 23:00:00",
        "season_desc": "Kiểm thử mùa khô Nam Bộ và các đợt rét nàng Bân miền Bắc"
    },
    {
        "fold_id": 2,
        "name": "Fold 2 (CÙNG MÙA VỚI TEST: T7 -> T11)",
        "train_end": "2025-06-30 23:00:00",
        "gap_start": "2025-07-01 00:00:00",
        "gap_end":   "2025-07-07 23:00:00",
        "val_start": "2025-07-08 00:00:00",
        "val_end":   "2025-11-16 23:00:00",
        "season_desc": "CỰC KỲ QUAN TRỌNG: Mùa mưa lũ, đỉnh nắng nóng cực đoan tương đồng 100% với khung thời gian Test"
    },
    {
        "fold_id": 3,
        "name": "Fold 3 (Gần Test nhất)",
        "train_end": "2025-12-31 23:00:00",
        "gap_start": "2026-01-01 00:00:00",
        "gap_end":   "2026-01-07 23:00:00",
        "val_start": "2026-01-08 00:00:00",
        "val_end":   "2026-06-23 23:00:00",
        "season_desc": "Gần Test nhất về mặt quán tính phát triển phụ tải năm 2026"
    }
]

# Danh mục biến ứng viên đầy đủ (Candidate Pool)
CANDIDATE_POOLS = {
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

print("\n" + "="*80, flush=True)
print("BƯỚC 3: TƯƠNG QUAN & CHỌN BIẾN TRONG TỪNG FOLD (CHỐNG RÒ RỈ DỮ LIỆU)", flush=True)
print("="*80, flush=True)

feature_selection_summary = {}

for target_col, cand_cols in CANDIDATE_POOLS.items():
    print(f"\n>>> PHÂN TÍCH BIẾN CHO MỤC TIÊU: [{target_col}] ({len(cand_cols)} biến ứng viên)", flush=True)
    feature_selection_summary[target_col] = {"folds": {}, "consensus": []}
    
    fold_selected_sets = []
    
    for fold in FOLDS_DEF:
        fid = fold["fold_id"]
        fname = fold["name"]
        
        # Tạo mask strictly trên fold đó
        tr_mask = df_dev['datetime'] <= pd.Timestamp(fold["train_end"])
        df_tr = df_dev[tr_mask].copy()
        
        # Lọc đa cộng tuyến (|r| > 0.95)
        selected_cols, dropped_cols = filter_collinear_features(df_tr, cand_cols, target_col, threshold=0.95)
        fold_selected_sets.append(set(selected_cols))
        
        # Tính tương quan Pearson & Spearman của các biến được chọn
        corrs = {}
        for c in selected_cols:
            r_pearson = float(df_tr[target_col].corr(df_tr[c], method='pearson'))
            r_spearman = float(df_tr[target_col].corr(df_tr[c], method='spearman'))
            corrs[c] = {"pearson": r_pearson, "spearman": r_spearman}
            
        feature_selection_summary[target_col]["folds"][f"fold_{fid}"] = {
            "name": fname,
            "train_samples": len(df_tr),
            "selected_features": selected_cols,
            "dropped_collinear": dropped_cols,
            "correlations": corrs
        }
        print(f"  * {fname:<30}: Giữ {len(selected_cols)} biến | Loại bỏ {len(dropped_cols)} biến trùng lặp {dropped_cols}", flush=True)
        
    # Tìm các biến đồng thuận (Consensus Features: xuất hiện ở cả 3 fold)
    consensus_features = list(set.intersection(*fold_selected_sets))
    consensus_features.sort()
    feature_selection_summary[target_col]["consensus"] = consensus_features
    print(f"  => BIẾN ĐỒNG THUẬN CAO (Cả 3 Fold đều giữ - Consensus): {len(consensus_features)} biến: {consensus_features}", flush=True)

# -------------------------------------------------------------------
# BƯỚC 4: CHỌN SIÊU THAM SỐ & ĐÁNH GIÁ MÔ HÌNH QUA CÁC FOLD
# -------------------------------------------------------------------
print("\n" + "="*80, flush=True)
print("BƯỚC 4: ĐÁNH GIÁ HIỆU NĂNG BỘ BA (XGBOOST, S-MAMBA, S4D) QUA 3 FOLD", flush=True)
print("="*80, flush=True)

walk_forward_results = {}
L = 24  # Cửa sổ quá khứ cho SSM

for target_col, cand_cols in CANDIDATE_POOLS.items():
    print(f"\n==================================================================", flush=True)
    print(f">>> HUẤN LUYỆN WALK-FORWARD: TARGET [{target_col}]", flush=True)
    print(f"==================================================================", flush=True)
    
    # Dùng tập biến đồng thuận (Consensus features)
    selected_feats = feature_selection_summary[target_col]["consensus"]
    
    walk_forward_results[target_col] = {
        "consensus_features": selected_feats,
        "folds": {}
    }
    
    xgb_fold_maes, s4_fold_maes, sm_fold_maes = [], [], []
    
    for fold in FOLDS_DEF:
        fid = fold["fold_id"]
        fname = fold["name"]
        print(f"\n--- {fname} ---", flush=True)
        
        tr_mask = df_dev['datetime'] <= pd.Timestamp(fold["train_end"])
        val_mask = (df_dev['datetime'] >= pd.Timestamp(fold["val_start"])) & (df_dev['datetime'] <= pd.Timestamp(fold["val_end"]))
        
        df_tr = df_dev[tr_mask].copy().reset_index(drop=True)
        df_val = df_dev[val_mask].copy().reset_index(drop=True)
        
        print(f"  Số mẫu: Train = {len(df_tr):,} giờ | Val (Thi thử) = {len(df_val):,} giờ", flush=True)
        
        # 1. Huấn luyện XGBoost
        X_tr_xgb = df_tr[selected_feats].values
        y_tr_xgb = df_tr[target_col].values
        X_val_xgb = df_val[selected_feats].values
        y_val_xgb = df_val[target_col].values
        
        xgb_model = xgb.XGBRegressor(
            n_estimators=1000,
            learning_rate=0.03,
            max_depth=6,
            subsample=0.8,
            colsample_bytree=0.8,
            early_stopping_rounds=30,
            random_state=42,
            n_jobs=-1
        )
        xgb_model.fit(X_tr_xgb, y_tr_xgb, eval_set=[(X_val_xgb, y_val_xgb)], verbose=False)
        p_xgb = xgb_model.predict(X_val_xgb)
        
        mae_xgb = float(mean_absolute_error(y_val_xgb, p_xgb))
        rmse_xgb = float(np.sqrt(mean_squared_error(y_val_xgb, p_xgb)))
        wape_xgb = calc_wape(y_val_xgb, p_xgb)
        r2_xgb = float(r2_score(y_val_xgb, p_xgb))
        xgb_fold_maes.append(mae_xgb)
        print(f"  [XGBoost] : MAE = {mae_xgb:,.2f} MW | RMSE = {rmse_xgb:,.2f} MW | WAPE = {wape_xgb:.2f}% | R2 = {r2_xgb:.4f}", flush=True)

        # 2. Chuẩn bị dữ liệu chuỗi (Sequences) cho S4D & S-Mamba
        exog_cols = [c for c in selected_feats if not ('_lag1' in c or '_lag2' in c or '_lag24' in c)]
        seq_cols = [target_col] + exog_cols
        F_num = len(seq_cols)
        
        scaler = StandardScaler()
        scaler.fit(df_tr[exog_cols].values)
        
        # Normalize dev subset up to val_end
        sub_df = df_dev[df_dev['datetime'] <= pd.Timestamp(fold["val_end"])].copy().reset_index(drop=True)
        sub_norm = sub_df[seq_cols].copy()
        sub_norm[exog_cols] = scaler.transform(sub_df[exog_cols].values)
        raw_mat = sub_norm.values
        
        N_sub = len(sub_df)
        X_seq_sub = np.zeros((N_sub - L, L, F_num), dtype=np.float32)
        y_seq_sub = np.zeros((N_sub - L, 1), dtype=np.float32)
        sub_ts = sub_df['datetime'].iloc[L:].reset_index(drop=True)
        
        for i in range(L, N_sub):
            X_seq_sub[i - L] = raw_mat[i - L : i]
            y_seq_sub[i - L, 0] = raw_mat[i, 0]
            
        tr_seq_idx = np.where(sub_ts <= pd.Timestamp(fold["train_end"]))[0]
        val_seq_idx = np.where((sub_ts >= pd.Timestamp(fold["val_start"])) & (sub_ts <= pd.Timestamp(fold["val_end"])))[0]
        
        X_tr_t = torch.tensor(X_seq_sub[tr_seq_idx], dtype=torch.float32)
        y_tr_t = torch.tensor(y_seq_sub[tr_seq_idx], dtype=torch.float32)
        X_val_t = torch.tensor(X_seq_sub[val_seq_idx], dtype=torch.float32)
        y_val_t = torch.tensor(y_seq_sub[val_seq_idx], dtype=torch.float32)
        y_true_seq = y_seq_sub[val_seq_idx, 0]
        
        loss_fn = nn.SmoothL1Loss(beta=0.5)

        # 3. Huấn luyện S4D
        s4 = S4Forecaster(n_feat=F_num, horizon=1, d_model=32, n_layers=2, d_state=32, dropout=0.1)
        opt_s4 = torch.optim.AdamW(s4.parameters(), lr=3e-3, weight_decay=1e-4)
        sched_s4 = torch.optim.lr_scheduler.CosineAnnealingLR(opt_s4, T_max=8)
        
        best_s4_loss = float('inf')
        best_s4_state = None
        for ep in range(8):
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
            vloss = loss_fn(eval_batched(s4, X_val_t, batch_size=256), y_val_t).item()
            if vloss < best_s4_loss:
                best_s4_loss = vloss
                best_s4_state = {k: v.cpu().clone() for k, v in s4.state_dict().items()}
                
        s4.load_state_dict(best_s4_state)
        p_s4 = eval_batched(s4, X_val_t, batch_size=256).cpu().numpy().flatten()
        mae_s4 = float(mean_absolute_error(y_true_seq, p_s4))
        rmse_s4 = float(np.sqrt(mean_squared_error(y_true_seq, p_s4)))
        wape_s4 = calc_wape(y_true_seq, p_s4)
        r2_s4 = float(r2_score(y_true_seq, p_s4))
        s4_fold_maes.append(mae_s4)
        print(f"  [S4D]     : MAE = {mae_s4:,.2f} MW | RMSE = {rmse_s4:,.2f} MW | WAPE = {wape_s4:.2f}% | R2 = {r2_s4:.4f}", flush=True)

        # 4. Huấn luyện S-Mamba
        sm = SMamba(n_feat=F_num, lookback=L, horizon=1, d_model=32, n_layers=2, d_state=16, dropout=0.1)
        opt_sm = torch.optim.AdamW(sm.parameters(), lr=3e-3, weight_decay=1e-4)
        sched_sm = torch.optim.lr_scheduler.CosineAnnealingLR(opt_sm, T_max=4)
        
        best_sm_loss = float('inf')
        best_sm_state = None
        for ep in range(4):
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
            vloss = loss_fn(eval_batched(sm, X_val_t, batch_size=256), y_val_t).item()
            if vloss < best_sm_loss:
                best_sm_loss = vloss
                best_sm_state = {k: v.cpu().clone() for k, v in sm.state_dict().items()}
                
        sm.load_state_dict(best_sm_state)
        p_sm = eval_batched(sm, X_val_t, batch_size=256).cpu().numpy().flatten()
        mae_sm = float(mean_absolute_error(y_true_seq, p_sm))
        rmse_sm = float(np.sqrt(mean_squared_error(y_true_seq, p_sm)))
        wape_sm = calc_wape(y_true_seq, p_sm)
        r2_sm = float(r2_score(y_true_seq, p_sm))
        sm_fold_maes.append(mae_sm)
        print(f"  [S-Mamba] : MAE = {mae_sm:,.2f} MW | RMSE = {rmse_sm:,.2f} MW | WAPE = {wape_sm:.2f}% | R2 = {r2_sm:.4f}", flush=True)

        walk_forward_results[target_col]["folds"][f"fold_{fid}"] = {
            "name": fname,
            "season_desc": fold["season_desc"],
            "XGBoost": {"MAE": mae_xgb, "RMSE": rmse_xgb, "WAPE%": wape_xgb, "R2": r2_xgb},
            "S4D": {"MAE": mae_s4, "RMSE": rmse_s4, "WAPE%": wape_s4, "R2": r2_s4},
            "S-Mamba": {"MAE": mae_sm, "RMSE": rmse_sm, "WAPE%": wape_sm, "R2": r2_sm}
        }
        
    walk_forward_results[target_col]["summary"] = {
        "XGBoost": {"mean_MAE": float(np.mean(xgb_fold_maes)), "std_MAE": float(np.std(xgb_fold_maes))},
        "S4D": {"mean_MAE": float(np.mean(s4_fold_maes)), "std_MAE": float(np.std(s4_fold_maes))},
        "S-Mamba": {"mean_MAE": float(np.mean(sm_fold_maes)), "std_MAE": float(np.std(sm_fold_maes))}
    }
    print(f"\n>>> TỔNG KẾT WALK-FORWARD [{target_col}]:", flush=True)
    print(f"  * XGBoost : MAE TB = {np.mean(xgb_fold_maes):,.1f} +- {np.std(xgb_fold_maes):.1f} MW", flush=True)
    print(f"  * S4D     : MAE TB = {np.mean(s4_fold_maes):,.1f} +- {np.std(s4_fold_maes):.1f} MW", flush=True)
    print(f"  * S-Mamba : MAE TB = {np.mean(sm_fold_maes):,.1f} +- {np.std(sm_fold_maes):.1f} MW", flush=True)

# Lưu kết quả
with open(os.path.join(OUT_DIR, "walk_forward_cv_and_feature_selection.json"), 'w', encoding='utf-8') as f:
    json.dump({
        "feature_selection": feature_selection_summary,
        "walk_forward_results": walk_forward_results
    }, f, ensure_ascii=False, indent=2)

print("\n>>> ĐÃ HOÀN TẤT BƯỚC 2, 3, VÀ 4!", flush=True)
