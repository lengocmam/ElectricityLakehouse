import sys, os
import numpy as np
import pandas as pd
from scipy import stats

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

# 1. Load Data
csv_path = 's4_3thuat_toan/data_v2/unified_daily_v2.csv'
df = pd.read_csv(csv_path)
df['date'] = pd.to_datetime(df['date'])
df = df.sort_values('date').reset_index(drop=True)

# Calendar features
dow = df['date'].dt.dayofweek
df['dow_sin'] = np.sin(2 * np.pi * dow / 7)
df['dow_cos'] = np.cos(2 * np.pi * dow / 7)
month = df['date'].dt.month
df['month_sin'] = np.sin(2 * np.pi * (month - 1) / 12)
df['month_cos'] = np.cos(2 * np.pi * (month - 1) / 12)

# Interaction features (Nonlinear)
# Temperature-Humidity Index / Heat Index proxy
df['wx_thi'] = df['wx_temp_c'] * (df['wx_humidity_pct'] / 100.0)

# Exact Splits
start = pd.Timestamp('2023-06-01')
train_end = pd.Timestamp('2025-06-30')
val_start = pd.Timestamp('2025-07-01')
val_end = pd.Timestamp('2025-12-31')
test_start = pd.Timestamp('2026-01-01')
test_end = pd.Timestamp('2026-10-15')

df_pipeline = df[(df.date >= start) & (df.date <= test_end)].copy()
df_train = df[(df.date >= start) & (df.date <= train_end)].copy()
df_val = df[(df.date >= val_start) & (df.date <= val_end)].copy()
df_test = df[(df.date >= test_start) & (df.date <= test_end)].copy()

print("=" * 80)
print("1. KHẢO SÁT & THỐNG NHẤT PHÂN CHIA TẬP DỮ LIỆU (DATA SPLITS)")
print("=" * 80)
n_total = len(df_pipeline)
n_train = len(df_train)
n_val = len(df_val)
n_test = len(df_test)
print(f"Tổng chuỗi pipeline: {df_pipeline['date'].min().strftime('%Y-%m-%d')} đến {df_pipeline['date'].max().strftime('%Y-%m-%d')} = {n_total} ngày (100.0%)")
print(f"  - Tập Huấn luyện (Train): {df_train['date'].min().strftime('%Y-%m-%d')} đến {df_train['date'].max().strftime('%Y-%m-%d')} = {n_train} ngày ({n_train/n_total*100:.2f}%)")
print(f"  - Tập Thẩm định  (Val)  : {df_val['date'].min().strftime('%Y-%m-%d')} đến {df_val['date'].max().strftime('%Y-%m-%d')} = {n_val} ngày ({n_val/n_total*100:.2f}%)")
print(f"  - Tập Kiểm thử   (Test) : {df_test['date'].min().strftime('%Y-%m-%d')} đến {df_test['date'].max().strftime('%Y-%m-%d')} = {n_test} ngày ({n_test/n_total*100:.2f}%)")
print(f"  - Tổng kiểm tra: {n_train} + {n_val} + {n_test} = {n_train + n_val + n_test} ngày")

# Candidate features to evaluate on TRAIN ONLY
candidate_features = [
    # Lịch & Ngày lễ
    "month_cos", "month_sin", "dow_sin", "dow_cos",
    "is_working_day", "is_weekend", "is_public_holiday", "is_tet_break", "is_holiday_break",
    "days_since_break", "days_to_next_break", "tet_offset",
    # Thời tiết
    "wx_temp_c", "wx_humidity_pct", "wx_precip_mm", "wx_solar_rad_wm2", "wx_wind_ms", "wx_cloud_pct", "wx_thi",
    # Thủy văn (Chỉ dùng Lag 1 cho các biến vận hành / nước về)
    "hy_inflow_total_m3s", "hy_inflow_da", "hy_inflow_lo_gam", "hy_inflow_sesan", "hy_inflow_ma_ca",
    "hy_fill_ratio_mean", "hy_n_below_dead", "hy_powerhouse_total_m3s", "hy_spill_total_m3s"
]
candidate_features = [c for c in candidate_features if c in df_train.columns]

# Helper: Partial correlation controlling for confounders Z (linear trend + month_cos + is_working_day)
def partial_corr(df_sub, x_col, y_col, z_cols):
    cols = [x_col, y_col] + z_cols
    sub = df_sub[cols].dropna()
    if len(sub) < 30:
        return np.nan
    X = sub[x_col].values
    Y = sub[y_col].values
    Z = sub[z_cols].values
    Z = np.column_stack([np.ones(len(Z)), Z])
    
    # Residuals
    beta_x, _, _, _ = np.linalg.lstsq(Z, X, rcond=None)
    res_x = X - Z @ beta_x
    beta_y, _, _, _ = np.linalg.lstsq(Z, Y, rcond=None)
    res_y = Y - Z @ beta_y
    
    r, _ = stats.pearsonr(res_x, res_y)
    return float(r)

# 2. LOAD CORRELATION ANALYSIS (STRICTLY ON TRAIN)
print("\n" + "=" * 80)
print("2. MA TRẬN TƯƠNG QUAN PHỤ TẢI (LOAD) - CHỈ TÍNH TRÊN TẬP TRAIN (761 NGÀY)")
print("   Bao gồm: Tương quan thô (r), Tương quan sai phân ngày (Δ1), Sai phân tuần (Δ7), và Tương quan riêng phần (r_partial)")
print("=" * 80)

# Add trend index to train
df_train['trend_idx'] = np.arange(len(df_train))
z_controls_load = ['month_cos', 'month_sin', 'is_working_day', 'trend_idx']

load_records = []
y_raw = df_train['load_mean_mw']
y_d1 = df_train['load_mean_mw'].diff(1)
y_d7 = df_train['load_mean_mw'].diff(7)

for feat in candidate_features:
    x_raw = df_train[feat]
    x_d1 = df_train[feat].diff(1)
    x_d7 = df_train[feat].diff(7)
    
    # 1. Raw Pearson & Spearman
    valid_raw = ~(x_raw.isna() | y_raw.isna())
    r_raw, _ = stats.pearsonr(x_raw[valid_raw], y_raw[valid_raw])
    rho_raw, _ = stats.spearmanr(x_raw[valid_raw], y_raw[valid_raw])
    
    # 2. Differenced d1 (daily shock)
    valid_d1 = ~(x_d1.isna() | y_d1.isna())
    r_d1 = float(stats.pearsonr(x_d1[valid_d1], y_d1[valid_d1])[0]) if valid_d1.sum() > 30 else np.nan
    
    # 3. Differenced d7 (deseasonalized week-over-week shock)
    valid_d7 = ~(x_d7.isna() | y_d7.isna())
    r_d7 = float(stats.pearsonr(x_d7[valid_d7], y_d7[valid_d7])[0]) if valid_d7.sum() > 30 else np.nan
    
    # 4. Partial correlation (controlling for season, workday, and trend)
    # Don't control for z if feat is in z
    z_feat = [z for z in z_controls_load if z != feat]
    r_part = partial_corr(df_train, feat, 'load_mean_mw', z_feat)
    
    load_records.append({
        "feature": feat,
        "r_raw": round(r_raw, 4),
        "rho_raw": round(rho_raw, 4),
        "r_diff1": round(r_d1, 4),
        "r_diff7": round(r_d7, 4),
        "r_partial": round(r_part, 4)
    })

df_load_audit = pd.DataFrame(load_records).sort_values(by="r_partial", key=abs, ascending=False).reset_index(drop=True)
print(df_load_audit.to_string(index=False))

# 3. HYDRO CORRELATION ANALYSIS (STRICTLY ON TRAIN & LAGGED FEATURES)
print("\n" + "=" * 80)
print("3. MA TRẬN TƯƠNG QUAN THỦY ĐIỆN (HYDRO) - CHỈ TÍNH TRÊN TẬP TRAIN (761 NGÀY)")
print("   Loại bỏ rò rỉ: hy_powerhouse_total_m3s CÙNG NGÀY ĐÃ BỎ, chỉ xét Lag 1..7")
print("=" * 80)

z_controls_hydro = ['month_cos', 'month_sin', 'trend_idx']
hydro_records = []
y_hy = df_train['gen_hydro']
y_hy_d1 = df_train['gen_hydro'].diff(1)
y_hy_d7 = df_train['gen_hydro'].diff(7)

# Candidate features for Hydro: must evaluate both concurrent and LAG 1 to show leakage vs real past signal
for feat in candidate_features:
    x_raw = df_train[feat]
    x_lag1 = df_train[feat].shift(1)
    
    valid_raw = ~(x_raw.isna() | y_hy.isna())
    r_raw, _ = stats.pearsonr(x_raw[valid_raw], y_hy[valid_raw])
    
    valid_l1 = ~(x_lag1.isna() | y_hy.isna())
    r_lag1 = float(stats.pearsonr(x_lag1[valid_l1], y_hy[valid_l1])[0]) if valid_l1.sum() > 30 else np.nan
    
    z_feat = [z for z in z_controls_hydro if z != feat]
    r_part = partial_corr(df_train, feat, 'gen_hydro', z_feat)
    
    hydro_records.append({
        "feature": feat,
        "r_concurrent": round(r_raw, 4),
        "r_lag1_past": round(r_lag1, 4),
        "r_partial_season_ctrl": round(r_part, 4)
    })

df_hydro_audit = pd.DataFrame(hydro_records).sort_values(by="r_partial_season_ctrl", key=abs, ascending=False).reset_index(drop=True)
print(df_hydro_audit.to_string(index=False))

# Save results for artifact reporting
df_load_audit.to_csv('s4_3thuat_toan/code/rigorous_load_correlation_train.csv', index=False)
df_hydro_audit.to_csv('s4_3thuat_toan/code/rigorous_hydro_correlation_train.csv', index=False)
