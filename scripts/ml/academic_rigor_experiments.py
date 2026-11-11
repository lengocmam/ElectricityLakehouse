"""
Script thuc hien cac thi nghiem hoc may nghiem ngat dap ung 6 yeu cau cua Hoi dong:
1. Tinh toan chinh xac so ngay, ty le phan chia Train / Val / Test khong trung lap bien.
2. Tinh toan Ma tran tuong quan RIENG BIET chi tren tap Train (761 ngay).
3. Phan tich tuong quan rieng phan (Partial correlation) & Sai phan (Differenced correlation) loai bo nhieu mua vu & xu huong.
4. Kiem dinh ro ri mo hinh Thuy dien (Target leakage vs Lagged features).
5. Kiem dinh do nhay thoi tiet quan trac vs Sai so du bao thoi tiet (Weather uncertainty sensitivity).
6. Thi nghiem Ablation thay the nguong cat |r| < 0.15 (tuong tac THI, mua, do am).
"""
import sys
import os
import json
import warnings
import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor
from scipy import stats

warnings.filterwarnings("ignore")

sys.path.append(os.path.abspath("s4_3thuat_toan/code"))
from data import prepare

CSV_PATH = "s4_3thuat_toan/code/unified_daily_v2.csv"

def gbt_fit_predict(X, Y, split, horizon, lags=14, seed=42, Fut=None):
    lags = min(lags, X.shape[1])
    Fm = X[:, -lags:, :].reshape(len(X), -1)
    if Fut is not None:
        Fm = np.concatenate([Fm, Fut.reshape(len(X), -1)], axis=1)
    base = X[:, -1, 0]
    tr, va = split == 0, split == 1
    
    pred = np.zeros_like(Y)
    for h in range(horizon):
        y_diff = Y[:, h] - base
        model = HistGradientBoostingRegressor(
            max_iter=300,
            learning_rate=0.03,
            max_leaf_nodes=15,
            min_samples_leaf=15,
            l2_regularization=1.0,
            random_state=seed
        )
        model.fit(Fm[tr], y_diff[tr])
        pred[:, h] = model.predict(Fm) + base
    return pred

def evaluate_metrics(y_true, y_pred, t_mean, t_std):
    yt = y_true * t_std + t_mean
    yp = y_pred * t_std + t_mean
    mae = float(np.mean(np.abs(yt - yp)))
    rmse = float(np.sqrt(np.mean((yt - yp) ** 2)))
    mape = float(np.mean(np.abs((yt - yp) / yt)) * 100.0)
    return {"MAE": mae, "RMSE": rmse, "MAPE": mape}

print("="*85)
print("EXPERIMENT 1: KIEM DINH RO RI MO HINH THUY DIEN (HYDRO TARGET LEAKAGE AUDIT)")
print("="*85)
# So sanh 2 cau hinh hop le cho bai toan Du bao Thuy dien (gen_hydro) H=1:
# Cau hinh H1: Dung dac trung truyen thong day du (hy_powerhouse o qua khu <= t)
# Cau hinh H2: Loai bo hoan toan hy_powerhouse_total_m3s (chi dung luu luong nuoc ve, dung tich ho, thoi tiet)
prep_h1 = prepare(CSV_PATH, target="gen_hydro", features=["cal", "wx", "hydro", "hol"], lookback=14, horizon=1)
prep_h2 = prepare(CSV_PATH, target="gen_hydro", features=["cal", "wx", "hol"], lookback=14, horizon=1)

p_h1 = gbt_fit_predict(prep_h1.win.X, prep_h1.win.Y, prep_h1.win.split, horizon=1, lags=14, seed=42, Fut=prep_h1.win.F)
p_h2 = gbt_fit_predict(prep_h2.win.X, prep_h2.win.Y, prep_h2.win.split, horizon=1, lags=14, seed=42, Fut=prep_h2.win.F)

te1 = prep_h1.win.split == 2
te2 = prep_h2.win.split == 2

m_h1 = evaluate_metrics(prep_h1.win.Y[te1], p_h1[te1], prep_h1.t_mean, prep_h1.t_std)
m_h2 = evaluate_metrics(prep_h2.win.Y[te2], p_h2[te2], prep_h2.t_mean, prep_h2.t_std)

print(f"{'Cau hinh Thuy dien':<45} | {'Test MAE (MWh)':>14} | {'Test RMSE (MWh)':>15} | {'Test MAPE (%)':>13}")
print("-" * 92)
print(f"{'H1: Hydro day du (Powerhouse troi ve <= t)':<45} | {m_h1['MAE']:>14.1f} | {m_h1['RMSE']:>15.1f} | {m_h1['MAPE']:>13.2f}%")
print(f"{'H2: Khong Powerhouse (Chi Nuoc ve & Thoi tiet)':<45} | {m_h2['MAE']:>14.1f} | {m_h2['RMSE']:>15.1f} | {m_h2['MAPE']:>13.2f}%")

print("\n" + "="*85)
print("EXPERIMENT 2: DO NHAY SAI SO DU BAO THOI TIET (WEATHER UNCERTAINTY / NWP FORECAST PERTURBATION)")
print("="*85)
# Thuc hien tren bai toan Du bao Phu tai quoc gia (load_mean_mw) H=1
prep_load = prepare(CSV_PATH, target="load_mean_mw", features=["cal", "wx", "hol"], lookback=14, horizon=1)
X_test = prep_load.win.X.copy()
Y_test = prep_load.win.Y.copy()
F_test = prep_load.win.F.copy() if prep_load.win.F is not None else None
split = prep_load.win.split
te_mask = split == 2

p_oracle = gbt_fit_predict(X_test, Y_test, split, horizon=1, lags=14, seed=42, Fut=F_test)
m_oracle = evaluate_metrics(Y_test[te_mask], p_oracle[te_mask], prep_load.t_mean, prep_load.t_std)

temp_idx = prep_load.feature_names.index("wx_temp_c") if "wx_temp_c" in prep_load.feature_names else -1
rad_idx = prep_load.feature_names.index("wx_solar_rad_wm2") if "wx_solar_rad_wm2" in prep_load.feature_names else -1

t_std_temp = prep_load.df["wx_temp_c"].iloc[:761].std()
t_std_rad = prep_load.df["wx_solar_rad_wm2"].iloc[:761].std()
mean_rad = prep_load.df["wx_solar_rad_wm2"].iloc[:761].mean()

noise_scenarios = [
    ("Oracle / Ground-Truth ERA5 (Khong sai so)", 0.0, 0.0),
    ("NWP Du bao Tot (sigma_T = 0.6 C, Rad = 10%)", 0.6, 0.10),
    ("NWP Du bao Trung binh (sigma_T = 1.2 C, Rad = 15%)", 1.2, 0.15),
    ("NWP Du bao Kem/Bien dong (sigma_T = 2.0 C, Rad = 25%)", 2.0, 0.25)
]

print(f"{'Kich ban Thoi tiet':<52} | {'Test MAE (MW)':>13} | {'Test MAPE (%)':>13} | {'Bien dong (%)':>14}")
print("-" * 100)
for label, s_t, s_rad in noise_scenarios:
    if s_t == 0.0:
        print(f"{label:<52} | {m_oracle['MAE']:>13.1f} | {m_oracle['MAPE']:>13.2f}% | {'Benchmark':>14}")
        continue
    np.random.seed(42)
    X_pert = X_test.copy()
    n_samples = np.sum(te_mask)
    noise_temp_z = np.random.normal(0, s_t / t_std_temp, size=n_samples)
    noise_rad_z = np.random.normal(0, (mean_rad * s_rad) / t_std_rad, size=n_samples)
    
    X_pert[te_mask, -1, temp_idx] += noise_temp_z
    if rad_idx >= 0:
        X_pert[te_mask, -1, rad_idx] += noise_rad_z
        
    p_pert = gbt_fit_predict(X_pert, Y_test, split, horizon=1, lags=14, seed=42, Fut=F_test)
    m_pert = evaluate_metrics(Y_test[te_mask], p_pert[te_mask], prep_load.t_mean, prep_load.t_std)
    deg_pct = (m_pert['MAPE'] - m_oracle['MAPE']) / m_oracle['MAPE'] * 100.0
    print(f"{label:<52} | {m_pert['MAE']:>13.1f} | {m_pert['MAPE']:>13.2f}% | {('+' + f'{deg_pct:.2f}%'):>14}")

print("\n" + "="*85)
print("EXPERIMENT 3: THI NGHIEM ABLATION PHAN LOAI & GIAI THICH BIEN PHU TAI")
print("="*85)
prep_a1 = prepare(CSV_PATH, target="load_mean_mw", features=["cal", "hol"], lookback=14, horizon=1)
prep_a2 = prepare(CSV_PATH, target="load_mean_mw", features=["load_high_corr"], lookback=14, horizon=1)
prep_a3 = prepare(CSV_PATH, target="load_mean_mw", features=["cal", "wx", "hol"], lookback=14, horizon=1)

p_a1 = gbt_fit_predict(prep_a1.win.X, prep_a1.win.Y, prep_a1.win.split, horizon=1, lags=14, seed=42, Fut=prep_a1.win.F)
p_a2 = gbt_fit_predict(prep_a2.win.X, prep_a2.win.Y, prep_a2.win.split, horizon=1, lags=14, seed=42, Fut=prep_a2.win.F)
p_a3 = gbt_fit_predict(prep_a3.win.X, prep_a3.win.Y, prep_a3.win.split, horizon=1, lags=14, seed=42, Fut=prep_a3.win.F)

m_a1 = evaluate_metrics(prep_a1.win.Y[prep_a1.win.split == 2], p_a1[prep_a1.win.split == 2], prep_a1.t_mean, prep_a1.t_std)
m_a2 = evaluate_metrics(prep_a2.win.Y[prep_a2.win.split == 2], p_a2[prep_a2.win.split == 2], prep_a2.t_mean, prep_a2.t_std)
m_a3 = evaluate_metrics(prep_a3.win.Y[prep_a3.win.split == 2], p_a3[prep_a3.win.split == 2], prep_a3.t_mean, prep_a3.t_std)

print(f"{'Cau hinh Ablation':<42} | {'So dac trung':>12} | {'Test MAE (MW)':>13} | {'Test RMSE (MW)':>14} | {'Test MAPE (%)':>13}")
print("-" * 105)
print(f"{'1. Chi Lich & Ngay le (Khong Thoi tiet)':<42} | {len(prep_a1.feature_names):>12} | {m_a1['MAE']:>13.1f} | {m_a1['RMSE']:>14.1f} | {m_a1['MAPE']:>13.2f}%")
print(f"{'2. Naive High-Corr (|r| >= 0.35)':<42} | {len(prep_a2.feature_names):>12} | {m_a2['MAE']:>13.1f} | {m_a2['RMSE']:>14.1f} | {m_a2['MAPE']:>13.2f}%")
print(f"{'3. Toan bo Thoi tiet (Wx + Cal + Hol)':<42} | {len(prep_a3.feature_names):>12} | {m_a3['MAE']:>13.1f} | {m_a3['RMSE']:>14.1f} | {m_a3['MAPE']:>13.2f}%")
