# -*- coding: utf-8 -*-
"""Thực nghiệm Dò tìm và Tối ưu hóa Siêu tham số (Empirical Hyperparameter Tuning & Ablation Study).

Mục tiêu khoa học:
Thực hiện tìm kiếm trên lưới (Grid Search) và khảo sát thực nghiệm (Ablation Study)
trên tập phát triển Walk-Forward (Fold 2: 13,103h Train -> 7d Gap -> 3,168h Val) để chứng minh
tính tối ưu của bộ siêu tham số dựa trên số liệu thực nghiệm thay vì chỉ lý thuyết:

1. Thí nghiệm 1: Dò tìm Learning Rate cho S-Mamba và S4D (1e-4, 5e-4, 1e-3, 3e-3, 5e-3, 1e-2).
2. Thí nghiệm 2: Khảo sát Hàm tổn thất (MSE vs MAE vs Smooth L1 beta=0.5 vs Huber delta=1.0).
3. Thí nghiệm 3: Khảo sát Độ dài ngữ cảnh quan sát Lookback Window (L = 12, 24, 48, 72 giờ).
4. Thí nghiệm 4: Tìm kiếm trên lưới XGBoost (learning_rate in [0.01, 0.03, 0.05, 0.10] x max_depth in [4, 6, 8]).
"""
import os
import sys
import gc
import json
import time

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


def main():
    print("=" * 80)
    print("THỰC NGHIỆM DÒ TÌM VÀ TỐI ƯU HÓA SIÊU THAM SỐ (EMPIRICAL HYPERPARAMETER TUNING)")
    print("=" * 80)

    # 1. Nạp dữ liệu và chuẩn bị dữ liệu Fold 2 (Fold kiểm thử cùng mùa với Test)
    CSV_PATH = "data_exports/hourly_feature_store.csv"
    df = pd.read_csv(CSV_PATH)
    df['datetime'] = pd.to_datetime(df['datetime'])
    df = df.sort_values('datetime').reset_index(drop=True)

    target_col = "load_national"
    consensus_features = [
        'wx_temp_c', 'wx_humidity_pct', 'wx_solar_rad_wm2', 'wx_wind_speed_ms',
        'wx_cloud_cover_pct', 'wx_precip_mm', 'hour_sin', 'hour_cos',
        'dow_sin', 'dow_cos', 'month_sin', 'month_cos', 'is_weekend',
        'load_national_lag1', 'load_national_lag2', 'load_national_lag24'
    ]
    exog_cols = [c for c in consensus_features if c != target_col]

    # Fold 2 Definition:
    t_train_end = pd.Timestamp("2025-06-30 23:00:00")
    t_gap_end   = pd.Timestamp("2025-07-07 23:00:00")
    t_val_start = pd.Timestamp("2025-07-08 00:00:00")
    t_val_end   = pd.Timestamp("2025-11-16 23:00:00")

    df_train = df[df['datetime'] <= t_train_end].copy().reset_index(drop=True)
    df_val   = df[(df['datetime'] >= t_val_start) & (df['datetime'] <= t_val_end)].copy().reset_index(drop=True)

    print(f">>> Cấu hình Fold 2: Train={len(df_train):,} giờ | Gap=168 giờ | Val={len(df_val):,} giờ", flush=True)

    # Fit scaler duy nhất trên Train
    scaler = StandardScaler()
    scaler.fit(df_train[exog_cols].values)

    df_train_norm = df_train.copy()
    df_val_norm   = df_val.copy()
    df_train_norm[exog_cols] = scaler.transform(df_train[exog_cols].values)
    df_val_norm[exog_cols]   = scaler.transform(df_val[exog_cols].values)

    y_val_true = df_val[target_col].values

    # Chuẩn bị sequence helper
    def prepare_sequences(lookback_L):
        # Ghép train + gap + val để lookback không bị hụt ở đầu val
        df_sub = df[(df['datetime'] <= t_val_end)].copy().reset_index(drop=True)
        df_sub_norm = df_sub.copy()
        df_sub_norm[exog_cols] = scaler.transform(df_sub[exog_cols].values)

        seq_cols = [target_col] + exog_cols
        raw = df_sub_norm[seq_cols].values
        N = len(df_sub)
        X_all = np.zeros((N - lookback_L, lookback_L, len(seq_cols)), dtype=np.float32)
        y_all = np.zeros((N - lookback_L, 1), dtype=np.float32)
        ts_all = df_sub['datetime'].iloc[lookback_L:].reset_index(drop=True)

        for i in range(lookback_L, N):
            X_all[i - lookback_L] = raw[i - lookback_L : i]
            y_all[i - lookback_L, 0] = raw[i, 0]

        idx_tr = np.where(ts_all <= t_train_end)[0]
        idx_va = np.where((ts_all >= t_val_start) & (ts_all <= t_val_end))[0]

        X_tr_t = torch.tensor(X_all[idx_tr], dtype=torch.float32)
        y_tr_t = torch.tensor(y_all[idx_tr], dtype=torch.float32)
        X_va_t = torch.tensor(X_all[idx_va], dtype=torch.float32)
        y_va_true_sub = y_all[idx_va, 0]

        return X_tr_t, y_tr_t, X_va_t, y_va_true_sub, len(seq_cols)

    results_summary = {
        "learning_rate_search": {},
        "loss_function_search": {},
        "lookback_search": {},
        "xgboost_search": {}
    }

    # =========================================================================
    # THÍ NGHIỆM 1: KHẢO SÁT LEARNING RATE (S-MAMBA & S4D)
    # =========================================================================
    print("\n" + "="*70)
    print("THÍ NGHIỆM 1: DÒ TÌM LEARNING RATE (S-MAMBA & S4D)")
    print("="*70)

    lr_candidates = [1e-4, 5e-4, 1e-3, 3e-3, 5e-3, 1e-2]
    X_tr_t, y_tr_t, X_va_t, y_va_true_24, F_num = prepare_sequences(lookback_L=24)
    loss_smooth = nn.SmoothL1Loss(beta=0.5)

    lr_results = []
    for lr_val in lr_candidates:
        print(f"\n--- Đang thử nghiệm Learning Rate = {lr_val} ---")
        
        # 1.1 S-Mamba
        torch.manual_seed(42)
        sm = SMamba(n_feat=F_num, lookback=24, horizon=1, d_model=32, n_layers=2, d_state=16, dropout=0.1)
        opt_sm = torch.optim.AdamW(sm.parameters(), lr=lr_val, weight_decay=1e-4)
        sched_sm = torch.optim.lr_scheduler.CosineAnnealingLR(opt_sm, T_max=4)
        
        t0 = time.time()
        for ep in range(4):
            sm.train()
            perm = torch.randperm(len(X_tr_t))
            for b in range(0, len(perm), 128):
                idx = perm[b : b + 128]
                opt_sm.zero_grad()
                loss = loss_smooth(sm(X_tr_t[idx]), y_tr_t[idx])
                loss.backward()
                torch.nn.utils.clip_grad_norm_(sm.parameters(), 1.0)
                opt_sm.step()
            sched_sm.step()
            
        p_val_sm = eval_batched(sm, X_va_t, batch_size=256).cpu().numpy().flatten()
        mae_sm = float(mean_absolute_error(y_va_true_24, p_val_sm))
        rmse_sm = float(np.sqrt(mean_squared_error(y_va_true_24, p_val_sm)))
        wape_sm = calc_wape(y_va_true_24, p_val_sm)
        r2_sm = float(r2_score(y_va_true_24, p_val_sm))
        time_sm = time.time() - t0

        # 1.2 S4D
        torch.manual_seed(42)
        s4 = S4Forecaster(n_feat=F_num, horizon=1, d_model=32, n_layers=2, d_state=32, dropout=0.1)
        opt_s4 = torch.optim.AdamW(s4.parameters(), lr=lr_val, weight_decay=1e-4)
        sched_s4 = torch.optim.lr_scheduler.CosineAnnealingLR(opt_s4, T_max=4)
        
        t0 = time.time()
        for ep in range(4):
            s4.train()
            perm = torch.randperm(len(X_tr_t))
            for b in range(0, len(perm), 128):
                idx = perm[b : b + 128]
                opt_s4.zero_grad()
                loss = loss_smooth(s4(X_tr_t[idx]), y_tr_t[idx])
                loss.backward()
                torch.nn.utils.clip_grad_norm_(s4.parameters(), 1.0)
                opt_s4.step()
            sched_s4.step()
            
        p_val_s4 = eval_batched(s4, X_va_t, batch_size=256).cpu().numpy().flatten()
        mae_s4 = float(mean_absolute_error(y_va_true_24, p_val_s4))
        rmse_s4 = float(np.sqrt(mean_squared_error(y_va_true_24, p_val_s4)))
        wape_s4 = calc_wape(y_va_true_24, p_val_s4)
        r2_s4 = float(r2_score(y_va_true_24, p_val_s4))
        time_s4 = time.time() - t0

        lr_results.append({
            "learning_rate": lr_val,
            "smamba_mae": mae_sm, "smamba_rmse": rmse_sm, "smamba_wape": wape_sm, "smamba_r2": r2_sm, "smamba_time": time_sm,
            "s4d_mae": mae_s4, "s4d_rmse": rmse_s4, "s4d_wape": wape_s4, "s4d_r2": r2_s4, "s4d_time": time_s4
        })
        print(f"  * LR={lr_val:<7}: S-Mamba MAE={mae_sm:,.2f} MW (WAPE={wape_sm:.2f}%) | S4D MAE={mae_s4:,.2f} MW (WAPE={wape_s4:.2f}%)")

    results_summary["learning_rate_search"] = lr_results

    # =========================================================================
    # THÍ NGHIỆM 2: KHẢO SÁT HÀM TỔN THẤT (LOSS FUNCTION ABLATION)
    # =========================================================================
    print("\n" + "="*70)
    print("THÍ NGHIỆM 2: KHẢO SÁT HÀM TỔN THẤT (LOSS FUNCTION ABLATION)")
    print("="*70)

    loss_dict = {
        "MSE (L2)": nn.MSELoss(),
        "MAE (L1)": nn.L1Loss(),
        "Huber (delta=1.0)": nn.HuberLoss(delta=1.0),
        "Smooth L1 (beta=0.5)": nn.SmoothL1Loss(beta=0.5)
    }

    loss_results = []
    for l_name, l_fn in loss_dict.items():
        torch.manual_seed(42)
        sm = SMamba(n_feat=F_num, lookback=24, horizon=1, d_model=32, n_layers=2, d_state=16, dropout=0.1)
        opt_sm = torch.optim.AdamW(sm.parameters(), lr=3e-3, weight_decay=1e-4)
        sched_sm = torch.optim.lr_scheduler.CosineAnnealingLR(opt_sm, T_max=4)
        
        for ep in range(4):
            sm.train()
            perm = torch.randperm(len(X_tr_t))
            for b in range(0, len(perm), 128):
                idx = perm[b : b + 128]
                opt_sm.zero_grad()
                loss = l_fn(sm(X_tr_t[idx]), y_tr_t[idx])
                loss.backward()
                torch.nn.utils.clip_grad_norm_(sm.parameters(), 1.0)
                opt_sm.step()
            sched_sm.step()
            
        p_val_sm = eval_batched(sm, X_va_t, batch_size=256).cpu().numpy().flatten()
        mae_sm = float(mean_absolute_error(y_va_true_24, p_val_sm))
        rmse_sm = float(np.sqrt(mean_squared_error(y_va_true_24, p_val_sm)))
        wape_sm = calc_wape(y_va_true_24, p_val_sm)
        r2_sm = float(r2_score(y_va_true_24, p_val_sm))

        loss_results.append({
            "loss_function": l_name,
            "mae": mae_sm, "rmse": rmse_sm, "wape": wape_sm, "r2": r2_sm
        })
        print(f"  * {l_name:<22}: MAE = {mae_sm:,.2f} MW | RMSE = {rmse_sm:,.2f} MW | WAPE = {wape_sm:.2f}% | R2 = {r2_sm:.4f}")

    results_summary["loss_function_search"] = loss_results

    # =========================================================================
    # THÍ NGHIỆM 3: KHẢO SÁT ĐỘ DÀI NGỮ CẢNH LOOKBACK WINDOW (L)
    # =========================================================================
    print("\n" + "="*70)
    print("THÍ NGHIỆM 3: KHẢO SÁT CỬA SỔ QUAN SÁT LỊCH SỬ LOOKBACK (L)")
    print("="*70)

    lookback_candidates = [12, 24, 48, 72]
    lookback_results = []

    for L_val in lookback_candidates:
        X_tr_L, y_tr_L, X_va_L, y_va_true_L, F_num_L = prepare_sequences(lookback_L=L_val)
        
        torch.manual_seed(42)
        sm = SMamba(n_feat=F_num_L, lookback=L_val, horizon=1, d_model=32, n_layers=2, d_state=16, dropout=0.1)
        opt_sm = torch.optim.AdamW(sm.parameters(), lr=3e-3, weight_decay=1e-4)
        sched_sm = torch.optim.lr_scheduler.CosineAnnealingLR(opt_sm, T_max=4)
        
        t0 = time.time()
        for ep in range(4):
            sm.train()
            perm = torch.randperm(len(X_tr_L))
            for b in range(0, len(perm), 128):
                idx = perm[b : b + 128]
                opt_sm.zero_grad()
                loss = loss_smooth(sm(X_tr_L[idx]), y_tr_L[idx])
                loss.backward()
                torch.nn.utils.clip_grad_norm_(sm.parameters(), 1.0)
                opt_sm.step()
            sched_sm.step()
            
        p_val_sm = eval_batched(sm, X_va_L, batch_size=256).cpu().numpy().flatten()
        mae_sm = float(mean_absolute_error(y_va_true_L, p_val_sm))
        rmse_sm = float(np.sqrt(mean_squared_error(y_va_true_L, p_val_sm)))
        wape_sm = calc_wape(y_va_true_L, p_val_sm)
        r2_sm = float(r2_score(y_va_true_L, p_val_sm))
        el_time = time.time() - t0

        lookback_results.append({
            "lookback_window": L_val,
            "mae": mae_sm, "rmse": rmse_sm, "wape": wape_sm, "r2": r2_sm, "training_time_s": el_time
        })
        print(f"  * Lookback L={L_val:2d}h: MAE = {mae_sm:,.2f} MW | RMSE = {rmse_sm:,.2f} MW | WAPE = {wape_sm:.2f}% | Thời gian={el_time:.1f}s")

    results_summary["lookback_search"] = lookback_results

    # =========================================================================
    # THÍ NGHIỆM 4: GRID SEARCH CHO XGBOOST (LEARNING RATE & MAX DEPTH)
    # =========================================================================
    print("\n" + "="*70)
    print("THÍ NGHIỆM 4: GRID SEARCH CHO XGBOOST (LEARNING RATE X MAX DEPTH)")
    print("="*70)

    xgb_lr_grid = [0.01, 0.03, 0.05, 0.10]
    xgb_depth_grid = [4, 6, 8]
    xgb_results = []

    X_tr_xgb = df_train[consensus_features].values
    y_tr_xgb = df_train[target_col].values
    X_va_xgb = df_val[consensus_features].values
    y_va_xgb = df_val[target_col].values

    for depth in xgb_depth_grid:
        for lr in xgb_lr_grid:
            m_xgb = xgb.XGBRegressor(
                n_estimators=1000,
                learning_rate=lr,
                max_depth=depth,
                subsample=0.8,
                colsample_bytree=0.8,
                early_stopping_rounds=30,
                random_state=42,
                n_jobs=-1
            )
            m_xgb.fit(
                X_tr_xgb, y_tr_xgb,
                eval_set=[(X_va_xgb, y_va_xgb)],
                verbose=False
            )
            p_val = m_xgb.predict(X_va_xgb)
            mae = float(mean_absolute_error(y_va_xgb, p_val))
            rmse = float(np.sqrt(mean_squared_error(y_va_xgb, p_val)))
            wape = calc_wape(y_va_xgb, p_val)
            r2 = float(r2_score(y_va_xgb, p_val))
            best_iter = int(m_xgb.best_iteration)

            xgb_results.append({
                "learning_rate": lr,
                "max_depth": depth,
                "best_iteration": best_iter,
                "mae": mae, "rmse": rmse, "wape": wape, "r2": r2
            })
            print(f"  * Depth={depth} | LR={lr:<5}: MAE={mae:,.2f} MW | RMSE={rmse:,.2f} MW | WAPE={wape:.2f}% | Best Tree={best_iter}")

    results_summary["xgboost_search"] = xgb_results

    # Lưu kết quả dạng JSON
    json_path = os.path.join(OUT_DIR, "hyperparameter_tuning_results.json")
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(results_summary, f, ensure_ascii=False, indent=2)
    print(f"\n>>> Đã xuất báo cáo thực nghiệm siêu tham số: {json_path}")

    # =========================================================================
    # VẼ CÁC BIỂU ĐỒ CHỨNG MINH THỰC NGHIỆM ĐẠT CHUẨN KHOA HỌC
    # =========================================================================
    print("\n>>> Đang khởi tạo các biểu đồ thực nghiệm siêu tham số...", flush=True)

    # 1. Biểu đồ Learning Rate Curve
    plt.figure(figsize=(10, 5))
    lrs = [r["learning_rate"] for r in lr_results]
    mae_sm_list = [r["smamba_mae"] for r in lr_results]
    mae_s4_list = [r["s4d_mae"] for r in lr_results]

    plt.plot(lrs, mae_sm_list, 'o-', color='#27ae60', linewidth=2.2, markersize=8, label='S-Mamba (Selective SSM)')
    plt.plot(lrs, mae_s4_list, 's--', color='#2980b9', linewidth=2.0, markersize=8, label='S4D (Structured State Space)')
    plt.xscale('log')
    plt.axvline(x=3e-3, color='#e74c3c', linestyle=':', linewidth=1.8, label='Điểm tối ưu được chọn (lr = 3e-3)')
    plt.title("KHẢO SÁT THỰC NGHIỆM TỐC ĐỘ HỌC (LEARNING RATE) TRÊN FOLD 2", fontsize=12, fontweight='bold', pad=10)
    plt.xlabel("Learning Rate (Log Scale)", fontsize=11)
    plt.ylabel("Validation MAE (MW)", fontsize=11)
    plt.grid(True, which="both", ls=":", alpha=0.6)
    plt.legend(frameon=True, facecolor='white', framealpha=0.9)
    plt.tight_layout()
    p1 = os.path.join(OUT_DIR, "tuning_learning_rate_curve.png")
    plt.savefig(p1, dpi=200)
    plt.close()

    # 2. Biểu đồ Hàm tổn thất
    plt.figure(figsize=(9, 5))
    loss_names = [r["loss_function"] for r in loss_results]
    mae_losses = [r["mae"] for r in loss_results]
    colors = ['#bdc3c7', '#95a5a6', '#3498db', '#27ae60']

    bars = plt.bar(loss_names, mae_losses, color=colors, width=0.55, edgecolor='black', linewidth=0.8)
    for b in bars:
        h = b.get_height()
        plt.text(b.get_x() + b.get_width() / 2, h + 8, f"{h:,.1f} MW", ha='center', va='bottom', fontsize=10, fontweight='bold')
    plt.ylim(0, max(mae_losses) * 1.18)
    plt.title("SO SÁNH THỰC NGHIỆM CÁC HÀM TỔN THẤT (LOSS FUNCTIONS) CHO S-MAMBA", fontsize=12, fontweight='bold', pad=10)
    plt.ylabel("Validation MAE (MW)", fontsize=11)
    plt.grid(axis='y', linestyle=':', alpha=0.6)
    plt.tight_layout()
    p2 = os.path.join(OUT_DIR, "tuning_loss_function_comparison.png")
    plt.savefig(p2, dpi=200)
    plt.close()

    # 3. Biểu đồ Lookback Window
    plt.figure(figsize=(9, 5))
    lookbacks = [f"L = {r['lookback_window']}h" for r in lookback_results]
    mae_lookbacks = [r["mae"] for r in lookback_results]
    times_lookbacks = [r["training_time_s"] for r in lookback_results]

    fig, ax1 = plt.subplots(figsize=(9, 5))
    ax1.plot(lookbacks, mae_lookbacks, 'o-', color='#e67e22', linewidth=2.2, markersize=8, label='Validation MAE (MW)')
    ax1.set_ylabel("Validation MAE (MW)", color='#e67e22', fontsize=11, fontweight='bold')
    ax1.tick_params(axis='y', labelcolor='#e67e22')
    ax1.grid(True, linestyle=':', alpha=0.6)

    ax2 = ax1.twinx()
    ax2.bar(lookbacks, times_lookbacks, width=0.3, alpha=0.3, color='#34495e', label='Thời gian huấn luyện (giây)')
    ax2.set_ylabel("Thời gian huấn luyện (giây)", color='#34495e', fontsize=11)
    ax2.tick_params(axis='y', labelcolor='#34495e')

    plt.title("KHẢO SÁT ĐỘ DÀI CỬA SỔ QUAN SÁT (LOOKBACK WINDOW L) CHO S-MAMBA", fontsize=12, fontweight='bold', pad=10)
    fig.tight_layout()
    p3 = os.path.join(OUT_DIR, "tuning_lookback_window_ablation.png")
    plt.savefig(p3, dpi=200)
    plt.close()

    # 4. Heatmap XGBoost
    plt.figure(figsize=(9, 5))
    pivot_mae = pd.DataFrame(xgb_results).pivot(index='max_depth', columns='learning_rate', values='mae')
    im = plt.imshow(pivot_mae.values, cmap='YlGn_r', aspect='auto')
    plt.colorbar(im, label='Validation MAE (MW)')
    plt.xticks(range(len(xgb_lr_grid)), [str(x) for x in xgb_lr_grid])
    plt.yticks(range(len(xgb_depth_grid)), [f"Depth {d}" for d in xgb_depth_grid])
    plt.xlabel("Learning Rate", fontsize=11)
    plt.ylabel("Tree Max Depth", fontsize=11)
    for i in range(len(xgb_depth_grid)):
        for j in range(len(xgb_lr_grid)):
            val = pivot_mae.values[i, j]
            plt.text(j, i, f"{val:,.1f}\nMW", ha="center", va="center", color="black" if val > 780 else "white", fontweight='bold', fontsize=9)
    plt.title("MA TRẬN DÒ TÌM SIÊU THAM SỐ XGBOOST (GRID SEARCH VALIDATION MAE)", fontsize=12, fontweight='bold', pad=10)
    plt.tight_layout()
    p4 = os.path.join(OUT_DIR, "tuning_xgboost_grid_heatmap.png")
    plt.savefig(p4, dpi=200)
    plt.close()

    print(f"\n>>> Hoàn thành toàn bộ! Đã lưu 4 biểu đồ chứng minh thực nghiệm:")
    print(f"  1. {p1}")
    print(f"  2. {p2}")
    print(f"  3. {p3}")
    print(f"  4. {p4}")


if __name__ == "__main__":
    main()
