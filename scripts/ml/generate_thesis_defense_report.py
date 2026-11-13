import os
os.environ["LOKY_MAX_CPU_COUNT"] = "1"
os.environ["OMP_NUM_THREADS"] = "1"
os.environ["MKL_NUM_THREADS"] = "1"

import sys
import json
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
N_SEEDS = 5

def diebold_mariano(e1, e2, h=1):
    d = np.abs(e1) - np.abs(e2)
    n = len(d)
    mean_d = np.mean(d)
    def autocov(lag):
        if lag == 0: return np.var(d, ddof=0)
        return np.mean((d[lag:] - mean_d) * (d[:-lag] - mean_d))
    gamma0 = autocov(0)
    gammas = sum([2 * autocov(k) for k in range(1, h)])
    var_d = (gamma0 + gammas) / n
    if var_d <= 0: return 0.0, 1.0
    stat = mean_d / np.sqrt(var_d)
    hln_adj = np.sqrt((n + 1 - 2*h + h*(h - 1)/n) / n)
    stat_adj = stat * hln_adj
    p_val = 2 * (1 - stats.t.cdf(np.abs(stat_adj), df=n-1))
    return float(stat_adj), float(p_val)

# ==============================================================================
# 1. HYDROPOWER BENCHMARKS
# ==============================================================================
prep_h = prepare(CSV_PATH, target="gen_hydro", features=["cal", "wx", "hydro", "hol"], lookback=14, horizon=1)
te_h = prep_h.win.split == 2
tr_h = prep_h.win.split == 0
y_true_h = prep_h.win.Y[te_h, 0] * prep_h.t_std + prep_h.t_mean
base_h = prep_h.win.X[:, -1, 0]

# Baseline Persistence
y_persist_h = prep_h.win.X[te_h, -1, 0] * prep_h.t_std + prep_h.t_mean
err_p = y_true_h - y_persist_h
mae_p = float(np.mean(np.abs(err_p)))
rmse_p = float(np.sqrt(np.mean(err_p**2)))
mape_p = float(np.mean(np.abs(err_p / y_true_h)) * 100.0)

def eval_h(feats):
    p_v = prepare(CSV_PATH, target="gen_hydro", features=feats, lookback=14, horizon=1)
    maes, rmses, mapes, preds = [], [], [], []
    for s in range(N_SEEDS):
        Fm = p_v.win.X[:, -14:, :].reshape(len(p_v.win.X), -1)
        if p_v.win.F is not None:
            Fm = np.concatenate([Fm, p_v.win.F.reshape(len(p_v.win.X), -1)], axis=1)
        m = HistGradientBoostingRegressor(max_iter=250, learning_rate=0.03, max_leaf_nodes=15,
                                           min_samples_leaf=15, l2_regularization=1.0, random_state=s)
        m.fit(Fm[tr_h], p_v.win.Y[tr_h, 0] - base_h[tr_h])
        pred = (m.predict(Fm[te_h]) + base_h[te_h]) * p_v.t_std + p_v.t_mean
        err = y_true_h - pred
        maes.append(np.mean(np.abs(err)))
        rmses.append(np.sqrt(np.mean(err**2)))
        mapes.append(np.mean(np.abs(err / y_true_h)) * 100.0)
        preds.append(pred)
    return np.mean(maes), np.std(maes), np.mean(rmses), np.std(rmses), np.mean(mapes), np.std(mapes), np.mean(preds, axis=0)

mae_ar, s_mae_ar, rmse_ar, s_rmse_ar, mape_ar, s_mape_ar, pred_ar = eval_h(["cal", "hol"])
mae_full, s_mae_full, rmse_full, s_rmse_full, mape_full, s_mape_full, pred_full = eval_h(["cal", "wx", "hydro", "hol"])
data.FEATURE_GROUPS["hydro_no_pw"] = ["hy_inflow_total_m3s", "hy_fill_ratio_mean"]
mae_nopw, s_mae_nopw, rmse_nopw, s_rmse_nopw, mape_nopw, s_mape_nopw, pred_nopw = eval_h(["cal", "wx", "hydro_no_pw", "hol"])

dm_stat_p, dm_pval_p = diebold_mariano(y_true_h - pred_full, err_p, h=1)
dm_stat_ar, dm_pval_ar = diebold_mariano(y_true_h - pred_full, y_true_h - pred_ar, h=1)

hydro_summary = {
    "persistence": {"mae": mae_p, "rmse": rmse_p, "mape": mape_p},
    "pure_ar": {"mae": mae_ar, "mae_std": s_mae_ar, "rmse": rmse_ar, "rmse_std": s_rmse_ar, "mape": mape_ar, "mape_std": s_mape_ar},
    "no_powerhouse": {"mae": mae_nopw, "mae_std": s_mae_nopw, "rmse": rmse_nopw, "rmse_std": s_rmse_nopw, "mape": mape_nopw, "mape_std": s_mape_nopw},
    "full_hybrid": {"mae": mae_full, "mae_std": s_mae_full, "rmse": rmse_full, "rmse_std": s_rmse_full, "mape": mape_full, "mape_std": s_mape_full},
    "dm_vs_persistence": {"stat": dm_stat_p, "p_val": dm_pval_p},
    "dm_vs_ar": {"stat": dm_stat_ar, "p_val": dm_pval_ar}
}

# ==============================================================================
# 2. LOAD STEPWISE ABLATION
# ==============================================================================
prep_l = prepare(CSV_PATH, target="load_mean_mw", features=["cal", "wx", "hol"], lookback=14, horizon=1)
te_l = prep_l.win.split == 2
tr_l = prep_l.win.split == 0
y_true_l = prep_l.win.Y[te_l, 0] * prep_l.t_std + prep_l.t_mean
base_l = prep_l.win.X[:, -1, 0]

data.FEATURE_GROUPS["abl_s1_cal_hol"] = data.FEATURE_GROUPS["cal"] + data.FEATURE_GROUPS["hol"] # 12
data.FEATURE_GROUPS["abl_s2_temp"] = data.FEATURE_GROUPS["abl_s1_cal_hol"] + ["wx_temp_c"]    # 13
data.FEATURE_GROUPS["abl_s3_rad"] = data.FEATURE_GROUPS["abl_s2_temp"] + ["wx_solar_rad_wm2"] # 14
data.FEATURE_GROUPS["abl_s4_precip"] = data.FEATURE_GROUPS["abl_s3_rad"] + ["wx_precip_mm"]  # 15
data.FEATURE_GROUPS["abl_s5_all"] = data.FEATURE_GROUPS["cal"] + data.FEATURE_GROUPS["wx"] + data.FEATURE_GROUPS["hol"] # 17

def eval_load_step(grp):
    p_b = prepare(CSV_PATH, target="load_mean_mw", features=grp, lookback=14, horizon=1)
    maes, rmses, mapes = [], [], []
    for s in range(N_SEEDS):
        Fm = p_b.win.X[:, -14:, :].reshape(len(p_b.win.X), -1)
        if p_b.win.F is not None:
            Fm = np.concatenate([Fm, p_b.win.F.reshape(len(p_b.win.X), -1)], axis=1)
        m = HistGradientBoostingRegressor(max_iter=250, learning_rate=0.03, max_leaf_nodes=15,
                                           min_samples_leaf=15, l2_regularization=1.0, random_state=s)
        m.fit(Fm[tr_l], prep_l.win.Y[tr_l, 0] - base_l[tr_l])
        pred = (m.predict(Fm[te_l]) + base_l[te_l]) * prep_l.t_std + prep_l.t_mean
        err = y_true_l - pred
        maes.append(np.mean(np.abs(err)))
        rmses.append(np.sqrt(np.mean(err**2)))
        mapes.append(np.mean(np.abs(err / y_true_l)) * 100.0)
    return len(p_b.feature_names), np.mean(maes), np.std(maes), np.mean(rmses), np.std(rmses), np.mean(mapes), np.std(mapes)

load_steps = [
    ("Khoi 1: Baseline Lich & Ngay le", ["abl_s1_cal_hol"]),
    ("Khoi 2: + Nhiet do (wx_temp_c)", ["abl_s2_temp"]),
    ("Khoi 3: + Buc xa mat troi (wx_solar_rad_wm2)", ["abl_s3_rad"]),
    ("Khoi 4: + Luong mua lam mat (wx_precip_mm)", ["abl_s4_precip"]),
    ("Khoi 5: + Toan bo Thoi tiet (Them Do am, Gio)", ["abl_s5_all"])
]

load_ablation_summary = []
for name, grp in load_steps:
    n_ch, mae, s_mae, rmse, s_rmse, mape, s_mape = eval_load_step(grp)
    load_ablation_summary.append({
        "name": name, "channels": n_ch, "mae": mae, "mae_std": s_mae,
        "rmse": rmse, "rmse_std": s_rmse, "mape": mape, "mape_std": s_mape
    })

# ==============================================================================
# 3. NWP SENSITIVITY
# ==============================================================================
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

nwp_summary = []
base_mape = load_ablation_summary[-1]["mape"]
base_mae = load_ablation_summary[-1]["mae"]

for label, s_t, s_rad, phi in nwp_scenarios:
    if s_t == 0.0:
        nwp_summary.append({"label": label, "mae": base_mae, "mape": base_mape, "diff_pt": 0.0, "rel_pct": 0.0})
        continue
    maes, mapes = [], []
    for s in range(N_SEEDS):
        np.random.seed(s * 100 + 7)
        n_test = np.sum(te_l)
        eps_t = np.random.normal(0, np.sqrt(1 - phi**2), size=n_test)
        ar_t = np.zeros(n_test)
        ar_t[0] = eps_t[0]
        for t_idx in range(1, n_test): ar_t[t_idx] = phi * ar_t[t_idx-1] + eps_t[t_idx]
        noise_t_z = ar_t * (s_t / std_t)
        
        eps_rad = np.random.normal(0, np.sqrt(1 - phi**2), size=n_test)
        ar_rad = np.zeros(n_test)
        ar_rad[0] = eps_rad[0]
        for t_idx in range(1, n_test): ar_rad[t_idx] = phi * ar_rad[t_idx-1] + eps_rad[t_idx]
        noise_rad_z = ar_rad * ((mean_rad * s_rad) / std_rad)
        
        X_pert = prep_l.win.X.copy()
        X_pert[te_l, -1, temp_idx] += noise_t_z
        X_pert[te_l, -1, rad_idx] += noise_rad_z
        
        Fm = X_pert[:, -14:, :].reshape(len(X_pert), -1)
        Fm = np.concatenate([Fm, prep_l.win.F.reshape(len(X_pert), -1)], axis=1)
        m = HistGradientBoostingRegressor(max_iter=250, learning_rate=0.03, max_leaf_nodes=15,
                                           min_samples_leaf=15, l2_regularization=1.0, random_state=s)
        m.fit(Fm[tr_l], prep_l.win.Y[tr_l, 0] - base_l[tr_l])
        pred = (m.predict(Fm[te_l]) + base_l[te_l]) * prep_l.t_std + prep_l.t_mean
        err = y_true_l - pred
        maes.append(np.mean(np.abs(err)))
        mapes.append(np.mean(np.abs(err / y_true_l)) * 100.0)
    m_mean = float(np.mean(maes))
    mape_mean = float(np.mean(mapes))
    diff_pt = mape_mean - base_mape
    rel_pct = (diff_pt / base_mape) * 100.0
    nwp_summary.append({"label": label, "mae": m_mean, "mape": mape_mean, "diff_pt": diff_pt, "rel_pct": rel_pct})

output_all = {
    "hydro": hydro_summary,
    "load_ablation": load_ablation_summary,
    "nwp_sensitivity": nwp_summary
}

with open("s4_3thuat_toan/code/thesis_defense_rigorous_results.json", "w", encoding="utf-8") as f:
    json.dump(output_all, f, indent=2, ensure_ascii=False)

print("SUCCESSFULLY GENERATED THESIS DEFENSE METRICS!")
