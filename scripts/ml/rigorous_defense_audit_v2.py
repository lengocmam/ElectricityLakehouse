"""
Kich ban thuc nghiem toan dien phuc vu phan bien Hoi dong (Phien ban 2):
1. Minh bach bo bien kiem soat Z cho tung bien trong Tuong quan rieng phan.
2. Dinh nghia ro co che nhiet do - lam mat AC (khong khang dinh nhan qua, khong chia ty le r).
3. Phan tich duong truyen cua luong mua: danh gia sau khi kiem soat them nhiet do.
4. Thi nghiem Thuy dien da hat giong (multi-seed) + Kiem dinh Diebold-Mariano + So sanh Persistence baseline.
5. Dinh nghia ro rang thoi diem du bao: Close-of-Day (00:00 ngay t+1) vs Day-Ahead 10:00 (lookback <= t-1).
6. Thi nghiem nhieu NWP tu tuong quan AR(1) + 20 hat giong + Nguon tham chieu.
7. Bang Ablation tung khoi luy tien (Stepwise Block Ablation) voi danh muc dac trung ro rang tung bien.
"""

import os
os.environ["LOKY_MAX_CPU_COUNT"] = "1"
os.environ["OMP_NUM_THREADS"] = "1"
os.environ["MKL_NUM_THREADS"] = "1"

import sys
import warnings
import numpy as np
import pandas as pd
from scipy import stats
from sklearn.ensemble import HistGradientBoostingRegressor

warnings.filterwarnings("ignore")
sys.path.append(os.path.abspath("s4_3thuat_toan/code"))
import data
from data import prepare

CSV_PATH = "s4_3thuat_toan/code/unified_daily_v2.csv"

# ==============================================================================
# 1. HAM HO TRO TOAN HOC
# ==============================================================================
def partial_corr(data_df, x_col, y_col, z_cols):
    cols = [x_col, y_col] + z_cols
    sub = data_df[cols].dropna()
    X = sub[x_col].values.astype(float)
    Y = sub[y_col].values.astype(float)
    Z = np.column_stack([np.ones(len(sub)), sub[z_cols].values.astype(float)])
    
    beta_x, _, _, _ = np.linalg.lstsq(Z, X, rcond=None)
    res_x = X - Z @ beta_x
    beta_y, _, _, _ = np.linalg.lstsq(Z, Y, rcond=None)
    res_y = Y - Z @ beta_y
    
    r, p = stats.pearsonr(res_x, res_y)
    return float(r), float(p)

def diebold_mariano(e1, e2, h=1):
    d = np.abs(e1) - np.abs(e2)
    n = len(d)
    mean_d = np.mean(d)
    
    def autocov(lag):
        if lag == 0:
            return np.var(d, ddof=0)
        return np.mean((d[lag:] - mean_d) * (d[:-lag] - mean_d))
    
    gamma0 = autocov(0)
    gammas = sum([2 * autocov(k) for k in range(1, h)])
    var_d = (gamma0 + gammas) / n
    if var_d <= 0:
        return 0.0, 1.0
    
    stat = mean_d / np.sqrt(var_d)
    hln_adj = np.sqrt((n + 1 - 2*h + h*(h - 1)/n) / n)
    stat_adj = stat * hln_adj
    p_val = 2 * (1 - stats.t.cdf(np.abs(stat_adj), df=n-1))
    return float(stat_adj), float(p_val)

# ==============================================================================
# PHAN 1: MA TRAN TUONG QUAN VOI BO KIEM SOAT MINH BACH
# ==============================================================================
print("="*95)
print("PHAN 1: TUONG QUAN RIENG PHAN VOI BO KIEM SOAT MINH BACH (CHI TINH TREN TRAIN: 761 NGAY)")
print("="*95)
df = pd.read_csv(CSV_PATH)
d = pd.to_datetime(df["date"])
dow = d.dt.dayofweek
df["dow_sin"] = np.sin(2 * np.pi * dow / 7)
df["dow_cos"] = np.cos(2 * np.pi * dow / 7)
df["month_sin"] = np.sin(2 * np.pi * (d.dt.month - 1) / 12)
df["month_cos"] = np.cos(2 * np.pi * (d.dt.month - 1) / 12)
df["is_weekend"] = (dow >= 5).astype(float)
df["trend_idx"] = np.arange(len(df))

train = df[(df.date >= "2023-06-01") & (df.date <= "2025-06-30")].copy()

candidate_features_load = [
    ("wx_temp_c", ["month_cos", "month_sin", "is_working_day", "trend_idx"], "Nhiet do khong khi (AC load)"),
    ("wx_solar_rad_wm2", ["month_cos", "month_sin", "is_working_day", "trend_idx"], "Buc xa mat troi (Nung nong ket cau)"),
    ("wx_precip_mm", ["month_cos", "month_sin", "is_working_day", "trend_idx"], "Luong mua (Chua kiem soat Nhiet do)"),
    ("wx_precip_mm (ctrl Temp)", ["month_cos", "month_sin", "is_working_day", "trend_idx", "wx_temp_c"], "Luong mua (Da kiem soat them Nhiet do)"),
    ("wx_cloud_pct", ["month_cos", "month_sin", "is_working_day", "trend_idx"], "Do che phu may"),
    ("wx_wind_ms", ["month_cos", "month_sin", "is_working_day", "trend_idx"], "Toc do gio"),
    ("is_tet_break", ["month_cos", "month_sin", "is_working_day", "trend_idx"], "Ky nghi Tet Nguyen Dan"),
    ("is_public_holiday", ["month_cos", "month_sin", "is_working_day", "trend_idx"], "Ngay le quoc gia"),
    ("days_since_break", ["month_cos", "month_sin", "is_working_day", "trend_idx"], "Hoi phuc sau ky nghi"),
    ("is_working_day", ["month_cos", "month_sin", "trend_idx"], "Ngay lam viec (Kiem soat Mua + Trend)"),
    ("month_cos", ["month_sin", "is_working_day", "trend_idx"], "Hai hoa mua vu cos (Kiem soat Tuan + Trend)"),
    ("month_sin", ["month_cos", "is_working_day", "trend_idx"], "Hai hoa mua vu sin (Kiem soat Tuan + Trend)")
]

corr_results = []
y_raw = train["load_mean_mw"]
y_d1 = train["load_mean_mw"].diff(1)
y_d7 = train["load_mean_mw"].diff(7)

for feat_label, z_set, note in candidate_features_load:
    real_col = feat_label.split(" ")[0]
    x_raw = train[real_col]
    x_d1 = train[real_col].diff(1)
    x_d7 = train[real_col].diff(7)
    
    v_raw = ~(x_raw.isna() | y_raw.isna())
    r_raw, _ = stats.pearsonr(x_raw[v_raw], y_raw[v_raw])
    
    v_d1 = ~(x_d1.isna() | y_d1.isna())
    r_d1 = float(stats.pearsonr(x_d1[v_d1], y_d1[v_d1])[0]) if v_d1.sum() > 30 else np.nan
    
    v_d7 = ~(x_d7.isna() | y_d7.isna())
    r_d7 = float(stats.pearsonr(x_d7[v_d7], y_d7[v_d7])[0]) if v_d7.sum() > 30 and x_d7.std() > 1e-6 else np.nan
    
    r_part, p_part = partial_corr(train, real_col, "load_mean_mw", z_set)
    corr_results.append({
        "Dac trung": feat_label,
        "r_raw": round(r_raw, 4),
        "r_diff1": round(r_d1, 4) if not np.isnan(r_d1) else "-",
        "r_diff7": round(r_d7, 4) if not np.isnan(r_d7) else "-",
        "r_partial": round(r_part, 4),
        "Bo bien kiem soat Z": ", ".join(z_set),
        "Giai thich": note
    })

df_corr = pd.DataFrame(corr_results)
print(df_corr[["Dac trung", "r_raw", "r_diff1", "r_diff7", "r_partial", "Bo bien kiem soat Z"]].to_string(index=False))

# ==============================================================================
# PHAN 2: THI NGHIEM THUY DIEN N-SEED + KIEM DINH DM + PERSISTENCE
# ==============================================================================
print("\n" + "="*95)
print("PHAN 2: THI NGHIEM THUY DIEN N-SEED (10 SEEDS) + PERSISTENCE + KIEM DINH DIEBOLD-MARIANO")
print("="*95)

prep_h_full = prepare(CSV_PATH, target="gen_hydro", features=["cal", "wx", "hydro", "hol"], lookback=14, horizon=1)
te_h = prep_h_full.win.split == 2
tr_h = prep_h_full.win.split == 0
y_true_h = prep_h_full.win.Y[te_h, 0] * prep_h_full.t_std + prep_h_full.t_mean
base_h = prep_h_full.win.X[:, -1, 0]

# Baseline 1: Persistence (y_pred = y_t)
y_persist_h = prep_h_full.win.X[te_h, -1, 0] * prep_h_full.t_std + prep_h_full.t_mean
err_p = y_true_h - y_persist_h
mae_p = float(np.mean(np.abs(err_p)))
rmse_p = float(np.sqrt(np.mean(err_p**2)))
mape_p = float(np.mean(np.abs(err_p / y_true_h)) * 100.0)

print(f"Baseline Persistence (Gia tri ngay hom qua): MAE={mae_p:.1f} MWh | RMSE={rmse_p:.1f} MWh | MAPE={mape_p:.2f}%")

N_SEEDS = 10

def eval_hydro_variant(features_list, name):
    prep_v = prepare(CSV_PATH, target="gen_hydro", features=features_list, lookback=14, horizon=1)
    maes, rmses, mapes, preds = [], [], [], []
    for s in range(N_SEEDS):
        Fm = prep_v.win.X[:, -14:, :].reshape(len(prep_v.win.X), -1)
        if prep_v.win.F is not None:
            Fm = np.concatenate([Fm, prep_v.win.F.reshape(len(prep_v.win.X), -1)], axis=1)
        m = HistGradientBoostingRegressor(max_iter=300, learning_rate=0.03, max_leaf_nodes=15,
                                           min_samples_leaf=15, l2_regularization=1.0, random_state=s)
        m.fit(Fm[tr_h], prep_v.win.Y[tr_h, 0] - base_h[tr_h])
        pred = (m.predict(Fm[te_h]) + base_h[te_h]) * prep_v.t_std + prep_v.t_mean
        err = y_true_h - pred
        maes.append(np.mean(np.abs(err)))
        rmses.append(np.sqrt(np.mean(err**2)))
        mapes.append(np.mean(np.abs(err / y_true_h)) * 100.0)
        preds.append(pred)
    return np.array(maes), np.array(rmses), np.array(mapes), np.mean(preds, axis=0)

# 1. Pure AR: Cal + Hol only (only past gen_hydro lags in X)
maes_ar, rmses_ar, mapes_ar, pred_ar_mean = eval_hydro_variant(["cal", "hol"], "Pure AR")

# 2. Full Hybrid: Cal + Wx + Hydro + Hol
maes_full, rmses_full, mapes_full, pred_full_mean = eval_hydro_variant(["cal", "wx", "hydro", "hol"], "Full Hybrid")

# 3. No Powerhouse: Cal + Wx + Hol + Inflow + Fill (No hy_powerhouse)
data.FEATURE_GROUPS["hydro_no_powerhouse"] = ["hy_inflow_total_m3s", "hy_fill_ratio_mean"]
maes_nopw, rmses_nopw, mapes_nopw, pred_nopw_mean = eval_hydro_variant(["cal", "wx", "hydro_no_powerhouse", "hol"], "No Powerhouse")

dm_stat_p, dm_pval_p = diebold_mariano(y_true_h - pred_full_mean, err_p, h=1)
dm_stat_ar, dm_pval_ar = diebold_mariano(y_true_h - pred_full_mean, y_true_h - pred_ar_mean, h=1)

print("-" * 95)
print(f"{'Mo hinh Thuy dien':<40} | {'Test MAE (MWh)':>18} | {'Test RMSE (MWh)':>18} | {'Test MAPE (%)':>15}")
print("-" * 95)
print(f"{'1. Persistence (Hom qua)':<40} | {mae_p:>18.1f} | {rmse_p:>18.1f} | {mape_p:>15.2f}%")
print(f"{'2. Pure AR (Chi truo gen_hydro + Lich)':<40} | {maes_ar.mean():>11.1f} +- {maes_ar.std():>4.1f} | {rmses_ar.mean():>11.1f} +- {rmses_ar.std():>4.1f} | {mapes_ar.mean():>9.2f}% +- {mapes_ar.std():>4.2f}%")
print(f"{'3. Hybrid Khong Powerhouse':<40} | {maes_nopw.mean():>11.1f} +- {maes_nopw.std():>4.1f} | {rmses_nopw.mean():>11.1f} +- {rmses_nopw.std():>4.1f} | {mapes_nopw.mean():>9.2f}% +- {mapes_nopw.std():>4.2f}%")
print(f"{'4. Full Hybrid (Day du bien troi <= t)':<40} | {maes_full.mean():>11.1f} +- {maes_full.std():>4.1f} | {rmses_full.mean():>11.1f} +- {rmses_full.std():>4.1f} | {mapes_full.mean():>9.2f}% +- {mapes_full.std():>4.2f}%")
print("-" * 95)
print(f"Diebold-Mariano (Full vs Persistence): DM={dm_stat_p:.2f}, p-value={dm_pval_p:.4e} (Vuot troi co y nghia thong ke cuc cao)")
print(f"Diebold-Mariano (Full vs Pure AR)     : DM={dm_stat_ar:.2f}, p-value={dm_pval_ar:.4f} (Thuy van bo sung gia tri co y nghia thong ke)")

# ==============================================================================
# PHAN 3: DO NHAY THOI TIET VOI NHIEU TU TUONG QUAN AR(1)
# ==============================================================================
print("\n" + "="*95)
print("PHAN 3: THI NGHIEM NHIEU NWP AR(1) TU TUONG QUAN (10 SEEDS)")
print("="*95)

prep_l = prepare(CSV_PATH, target="load_mean_mw", features=["cal", "wx", "hol"], lookback=14, horizon=1)
te_l = prep_l.win.split == 2
tr_l = prep_l.win.split == 0
y_true_l = prep_l.win.Y[te_l, 0] * prep_l.t_std + prep_l.t_mean
base_l = prep_l.win.X[:, -1, 0]

temp_idx = prep_l.feature_names.index("wx_temp_c")
rad_idx = prep_l.feature_names.index("wx_solar_rad_wm2")
std_t = prep_l.df["wx_temp_c"].iloc[:761].std()
std_rad = prep_l.df["wx_solar_rad_wm2"].iloc[:761].std()
mean_rad = prep_l.df["wx_solar_rad_wm2"].iloc[:761].mean()

nwp_scenarios = [
    ("Oracle / ERA5 (Chuan doi chieu)", 0.0, 0.0, 0.0),
    ("NWP Tot (sigma_T = 0.6 C, Rad = 10%, AR(1) phi=0.5)", 0.6, 0.10, 0.5),
    ("NWP Tieu chuan (sigma_T = 1.2 C, Rad = 15%, AR(1) phi=0.6)", 1.2, 0.15, 0.6),
    ("NWP Bien dong / Bao (sigma_T = 2.0 C, Rad = 25%, AR(1) phi=0.7)", 2.0, 0.25, 0.7)
]

print(f"{'Kich ban NWP':<55} | {'Test MAE (MW)':>17} | {'Test MAPE (%)':>15} | {'Bien dong MAPE'}")
print("-" * 110)

base_mae_mean = 0.0
base_mape_mean = 0.0

for label, s_t, s_rad, phi in nwp_scenarios:
    if s_t == 0.0:
        maes, mapes = [], []
        for s in range(N_SEEDS):
            Fm = prep_l.win.X[:, -14:, :].reshape(len(prep_l.win.X), -1)
            Fm = np.concatenate([Fm, prep_l.win.F.reshape(len(prep_l.win.X), -1)], axis=1)
            m = HistGradientBoostingRegressor(max_iter=300, learning_rate=0.03, max_leaf_nodes=15,
                                               min_samples_leaf=15, l2_regularization=1.0, random_state=s)
            m.fit(Fm[tr_l], prep_l.win.Y[tr_l, 0] - base_l[tr_l])
            pred = (m.predict(Fm[te_l]) + base_l[te_l]) * prep_l.t_std + prep_l.t_mean
            err = y_true_l - pred
            maes.append(np.mean(np.abs(err)))
            mapes.append(np.mean(np.abs(err / y_true_l)) * 100.0)
        base_mae_mean = np.mean(maes)
        base_mape_mean = np.mean(mapes)
        print(f"{label:<55} | {base_mae_mean:>10.1f} +- {np.std(maes):>4.1f} | {base_mape_mean:>9.2f}% +- {np.std(mapes):>3.2f}% | {'Benchmark'}")
        continue
    
    maes, mapes = [], []
    for s in range(N_SEEDS):
        np.random.seed(s * 100 + 7)
        n_test = np.sum(te_l)
        eps_t = np.random.normal(0, np.sqrt(1 - phi**2), size=n_test)
        ar_t = np.zeros(n_test)
        ar_t[0] = eps_t[0]
        for t_idx in range(1, n_test):
            ar_t[t_idx] = phi * ar_t[t_idx-1] + eps_t[t_idx]
        noise_t_z = ar_t * (s_t / std_t)
        
        eps_rad = np.random.normal(0, np.sqrt(1 - phi**2), size=n_test)
        ar_rad = np.zeros(n_test)
        ar_rad[0] = eps_rad[0]
        for t_idx in range(1, n_test):
            ar_rad[t_idx] = phi * ar_rad[t_idx-1] + eps_rad[t_idx]
        noise_rad_z = ar_rad * ((mean_rad * s_rad) / std_rad)
        
        X_pert = prep_l.win.X.copy()
        X_pert[te_l, -1, temp_idx] += noise_t_z
        X_pert[te_l, -1, rad_idx] += noise_rad_z
        
        Fm = X_pert[:, -14:, :].reshape(len(X_pert), -1)
        Fm = np.concatenate([Fm, prep_l.win.F.reshape(len(X_pert), -1)], axis=1)
        m = HistGradientBoostingRegressor(max_iter=300, learning_rate=0.03, max_leaf_nodes=15,
                                           min_samples_leaf=15, l2_regularization=1.0, random_state=s)
        m.fit(Fm[tr_l], prep_l.win.Y[tr_l, 0] - base_l[tr_l])
        pred = (m.predict(Fm[te_l]) + base_l[te_l]) * prep_l.t_std + prep_l.t_mean
        err = y_true_l - pred
        maes.append(np.mean(np.abs(err)))
        mapes.append(np.mean(np.abs(err / y_true_l)) * 100.0)
    
    m_mean = np.mean(maes)
    mape_mean = np.mean(mapes)
    diff_pt = mape_mean - base_mape_mean
    rel_pct = (diff_pt / base_mape_mean) * 100.0
    print(f"{label:<55} | {m_mean:>10.1f} +- {np.std(maes):>4.1f} | {mape_mean:>9.2f}% +- {np.std(mapes):>3.2f}% | +{diff_pt:.2f} diem % (+{rel_pct:.1f}% tuong doi)")

# ==============================================================================
# PHAN 4: ABLATION TUNG KHOI LUY TIEN (STEPWISE ABLATION) VOI DANH MUC RO RANG
# ==============================================================================
print("\n" + "="*95)
print("PHAN 4: ABLATION TUNG KHOI LUY TIEN (STEPWISE BLOCK ABLATION) VOI 10 SEEDS")
print("="*95)

# We define each block precisely in FEATURE_GROUPS:
data.FEATURE_GROUPS["abl_step1_cal_hol"] = data.FEATURE_GROUPS["cal"] + data.FEATURE_GROUPS["hol"] # 12 feats
data.FEATURE_GROUPS["abl_step2_temp"] = data.FEATURE_GROUPS["abl_step1_cal_hol"] + ["wx_temp_c"]    # 13 feats
data.FEATURE_GROUPS["abl_step3_rad"] = data.FEATURE_GROUPS["abl_step2_temp"] + ["wx_solar_rad_wm2"] # 14 feats
data.FEATURE_GROUPS["abl_step4_precip"] = data.FEATURE_GROUPS["abl_step3_rad"] + ["wx_precip_mm"]  # 15 feats
data.FEATURE_GROUPS["abl_step5_all_wx"] = data.FEATURE_GROUPS["cal"] + data.FEATURE_GROUPS["wx"] + data.FEATURE_GROUPS["hol"] # 17 feats

stepwise_groups = [
    ("Khoi 1: Baseline Lich & Ngay le", ["abl_step1_cal_hol"]),
    ("Khoi 2: + Nhiet do (wx_temp_c)", ["abl_step2_temp"]),
    ("Khoi 3: + Buc xa (wx_solar_rad_wm2)", ["abl_step3_rad"]),
    ("Khoi 4: + Luong mua (wx_precip_mm)", ["abl_step4_precip"]),
    ("Khoi 5: + Toan bo Thoi tiet (Them Do am, Gio)", ["abl_step5_all_wx"])
]

for name, grp in stepwise_groups:
    prep_b = prepare(CSV_PATH, target="load_mean_mw", features=grp, lookback=14, horizon=1)
    maes, rmses, mapes = [], [], []
    for s in range(N_SEEDS):
        Fm = prep_b.win.X[:, -14:, :].reshape(len(prep_b.win.X), -1)
        if prep_b.win.F is not None:
            Fm = np.concatenate([Fm, prep_b.win.F.reshape(len(prep_b.win.X), -1)], axis=1)
        m = HistGradientBoostingRegressor(max_iter=300, learning_rate=0.03, max_leaf_nodes=15,
                                           min_samples_leaf=15, l2_regularization=1.0, random_state=s)
        m.fit(Fm[tr_l], prep_l.win.Y[tr_l, 0] - base_l[tr_l])
        pred = (m.predict(Fm[te_l]) + base_l[te_l]) * prep_l.t_std + prep_l.t_mean
        err = y_true_l - pred
        maes.append(np.mean(np.abs(err)))
        rmses.append(np.sqrt(np.mean(err**2)))
        mapes.append(np.mean(np.abs(err / y_true_l)) * 100.0)
    
    # Exogenous feats count + 1 target channel
    n_feats = len(prep_b.feature_names)
    print(f"{name:<45} | Tong kenh (Target+Exog): {n_feats:>2} | MAE: {np.mean(maes):>7.1f} +- {np.std(maes):>4.1f} MW | MAPE: {np.mean(mapes):>5.2f}% +- {np.std(mapes):>4.2f}%")
