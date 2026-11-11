import os
import shutil
os.environ["LOKY_MAX_CPU_COUNT"] = "1"
os.environ["OMP_NUM_THREADS"] = "1"
os.environ["MKL_NUM_THREADS"] = "1"
os.environ["MPLCONFIGDIR"] = os.path.abspath("scratch/matplotlib")

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import pandas as pd

# Create output folders
OUT_DIR = "s4_3thuat_toan/figures"
os.makedirs(OUT_DIR, exist_ok=True)
ARTIFACT_DIR = r"C:\Users\ASUS\.gemini\antigravity\brain\f345bf93-2a90-4f8b-b023-26cb2c3ce714"
os.makedirs(ARTIFACT_DIR, exist_ok=True)

# Set global aesthetics
plt.rcParams['font.sans-serif'] = 'DejaVu Sans'
plt.rcParams['axes.edgecolor'] = '#333333'
plt.rcParams['axes.linewidth'] = 0.8

# Load correlation tables
df_load = pd.read_csv("s4_3thuat_toan/code/rigorous_load_correlation_train.csv")
df_hydro = pd.read_csv("s4_3thuat_toan/code/rigorous_hydro_correlation_train.csv")

# ==============================================================================
# CHART 1: MA TRẬN TƯƠNG QUAN ĐA CHIỀU PHỤ TẢI (LOAD MULTI-METRIC HEATMAP)
# ==============================================================================
# Select key representative features
top_load_feats = [
    "is_tet_break", "month_cos", "is_working_day", "is_public_holiday",
    "wx_temp_c", "wx_thi", "wx_solar_rad_wm2", "days_since_break",
    "wx_precip_mm", "wx_cloud_pct", "month_sin", "wx_wind_ms", "wx_humidity_pct"
]

sub_load = df_load[df_load['feature'].isin(top_load_feats)].copy()
# Sort by abs(r_partial)
sub_load['abs_part'] = sub_load['r_partial'].abs()
sub_load = sub_load.sort_values(by='abs_part', ascending=False).reset_index(drop=True)

# Prepare heatmap matrix
matrix_load = sub_load[['r_raw', 'rho_raw', 'r_diff7', 'r_partial']].values
row_labels_load = [
    f"{r.feature} ({'Nhiệt độ' if r.feature=='wx_temp_c' else 'Nắng' if r.feature=='wx_solar_rad_wm2' else 'Mưa' if r.feature=='wx_precip_mm' else 'Nhiệt ẩm' if r.feature=='wx_thi' else 'Ngày đi làm' if r.feature=='is_working_day' else 'Tết' if r.feature=='is_tet_break' else 'Mùa vụ' if r.feature=='month_cos' else 'Lễ' if r.feature=='is_public_holiday' else r.feature})"
    for _, r in sub_load.iterrows()
]
col_labels_load = [
    "Pearson thô\n(r_raw)",
    "Spearman thô\n(rho_raw)",
    "Sai phân tuần\n(Δ7_r)",
    "Riêng phần\n(r_partial)"
]

fig, ax = plt.subplots(figsize=(9, 7.5), dpi=300)
sns.heatmap(
    matrix_load,
    annot=True,
    fmt=".3f",
    cmap="vlag",
    center=0,
    vmin=-0.8,
    vmax=0.8,
    xticklabels=col_labels_load,
    yticklabels=row_labels_load,
    cbar_kws={'label': 'Hệ số tương quan (Correlation Coefficient)', 'shrink': 0.8},
    linewidths=1.0,
    linecolor='#f0f0f0',
    ax=ax
)

ax.set_title("MA TRẬN TƯƠNG QUAN ĐA CHIỀU PHỤ TẢI (CHỈ TÍNH TRÊN TẬP TRAIN: 761 NGÀY)\nSo sánh Tương quan Thô vs Sai phân Tuần (Δ7) vs Tương quan Riêng phần (Partial)",
             fontsize=11, fontweight='bold', pad=15)
plt.xticks(fontsize=10, fontweight='bold')
plt.yticks(fontsize=9)
plt.tight_layout()

chart1_path = os.path.join(OUT_DIR, "correlation_heatmap_load.png")
plt.savefig(chart1_path)
plt.close()
shutil.copy(chart1_path, os.path.join(ARTIFACT_DIR, "correlation_heatmap_load.png"))
print("Saved Chart 1:", chart1_path)

# ==============================================================================
# CHART 2: MA TRẬN TƯƠNG QUAN THỦY ĐIỆN & RÀ SOÁT RÒ RỈ (HYDRO HEATMAP)
# ==============================================================================
top_hydro_feats = [
    "hy_powerhouse_total_m3s", "hy_inflow_total_m3s", "hy_inflow_da",
    "hy_inflow_lo_gam", "hy_inflow_sesan", "hy_inflow_ma_ca", "hy_fill_ratio_mean",
    "wx_precip_mm", "wx_humidity_pct", "month_cos", "month_sin", "is_working_day"
]

sub_hydro = df_hydro[df_hydro['feature'].isin(top_hydro_feats)].copy()
sub_hydro['abs_lag1'] = sub_hydro['r_lag1_past'].abs()
sub_hydro = sub_hydro.sort_values(by='abs_lag1', ascending=False).reset_index(drop=True)

matrix_hydro = sub_hydro[['r_concurrent', 'r_lag1_past', 'r_partial_season_ctrl']].values
row_labels_hydro = [
    f"{r.feature} ({'Xả tuabin' if 'powerhouse' in r.feature else 'Nước về tổng' if 'total_m3s' in r.feature else 'Sông Đà' if 'da' in r.feature else 'Sông Lô' if 'lo_gam' in r.feature else 'Sê San' if 'sesan' in r.feature else 'Mã Cả' if 'ma_ca' in r.feature else 'Trữ nước hồ' if 'fill_ratio' in r.feature else 'Mưa' if 'precip' in r.feature else 'Độ ẩm' if 'humidity' in r.feature else 'Mùa cos' if 'month_cos' in r.feature else 'Mùa sin' if 'month_sin' in r.feature else r.feature})"
    for _, r in sub_hydro.iterrows()
]
col_labels_hydro = [
    "Đồng thời cùng ngày\n(r_concurrent - Cảnh báo rò rỉ)",
    "Độ trễ quá khứ 1 ngày\n(r_lag1 - Hợp lệ vận hành)",
    "Riêng phần khử mùa\n(r_partial)"
]

fig, ax = plt.subplots(figsize=(9, 7), dpi=300)
sns.heatmap(
    matrix_hydro,
    annot=True,
    fmt=".3f",
    cmap="Spectral_r",
    center=0,
    vmin=-0.9,
    vmax=0.9,
    xticklabels=col_labels_hydro,
    yticklabels=row_labels_hydro,
    cbar_kws={'label': 'Hệ số tương quan', 'shrink': 0.8},
    linewidths=1.0,
    linecolor='#f0f0f0',
    ax=ax
)

ax.set_title("MA TRẬN TƯƠNG QUAN THỦY ĐIỆN: RÀ SOÁT RÒ RỈ MỤC TIÊU (TRAIN-ONLY 761 NGÀY)\nSo sánh Biến đồng thời (Concurrent) vs Trễ quá khứ hợp lệ (Lag-1)",
             fontsize=11, fontweight='bold', pad=15)
plt.xticks(fontsize=10, fontweight='bold')
plt.yticks(fontsize=9)
plt.tight_layout()

chart2_path = os.path.join(OUT_DIR, "correlation_heatmap_hydro.png")
plt.savefig(chart2_path)
plt.close()
shutil.copy(chart2_path, os.path.join(ARTIFACT_DIR, "correlation_heatmap_hydro.png"))
print("Saved Chart 2:", chart2_path)

# ==============================================================================
# CHART 3: MA TRẬN ĐA CỘNG TUYẾN GIỮA CÁC ĐẶC TRƯNG ĐẦU VÀO (FEATURE-TO-FEATURE CROSS-CORR)
# ==============================================================================
df_raw = pd.read_csv("s4_3thuat_toan/code/unified_daily_v2.csv")
d = pd.to_datetime(df_raw["date"])
dow = d.dt.dayofweek
df_raw["dow_sin"] = np.sin(2 * np.pi * dow / 7)
df_raw["dow_cos"] = np.cos(2 * np.pi * dow / 7)
df_raw["month_sin"] = np.sin(2 * np.pi * (d.dt.month - 1) / 12)
df_raw["month_cos"] = np.cos(2 * np.pi * (d.dt.month - 1) / 12)
df_raw["is_weekend"] = (dow >= 5).astype(float)
df_train = df_raw[(df_raw.date >= "2023-06-01") & (df_raw.date <= "2025-06-30")].copy()

cross_feats = [
    "load_mean_mw", "wx_temp_c", "wx_solar_rad_wm2", "wx_precip_mm", "wx_humidity_pct",
    "month_cos", "month_sin", "is_working_day", "is_tet_break", "is_public_holiday",
    "hy_inflow_total_m3s", "hy_fill_ratio_mean"
]

cross_names = [
    "Phụ tải", "Nhiệt độ", "Bức xạ", "Lượng mưa", "Độ ẩm",
    "Mùa (cos)", "Mùa (sin)", "Ngày đi làm", "Nghỉ Tết", "Ngày lễ",
    "Nước về hồ", "Tích nước hồ"
]

corr_cross = df_train[cross_feats].corr(method='pearson')

# Mask the upper triangle for clean academic view
mask = np.triu(np.ones_like(corr_cross, dtype=bool), k=1)

fig, ax = plt.subplots(figsize=(10.5, 8.5), dpi=300)
sns.heatmap(
    corr_cross,
    mask=mask,
    annot=True,
    fmt=".2f",
    cmap="coolwarm",
    center=0,
    vmin=-1.0,
    vmax=1.0,
    xticklabels=cross_names,
    yticklabels=cross_names,
    cbar_kws={'label': 'Hệ số tương quan chéo Pearson (r)', 'shrink': 0.8},
    linewidths=0.8,
    linecolor='white',
    ax=ax
)

ax.set_title("MA TRẬN TƯƠNG QUAN CHÉO ĐẦY ĐỦ GIỮA CÁC ĐẶC TRƯNG CHÍNH (TRAIN: 761 NGÀY)\nKiểm tra Đa cộng tuyến & Mối tương hỗ giữa Khí tượng, Thủy văn và Lịch vận hành",
             fontsize=11, fontweight='bold', pad=15)
plt.xticks(fontsize=9, rotation=35, ha='right', fontweight='bold')
plt.yticks(fontsize=9, fontweight='bold')
plt.tight_layout()

chart3_path = os.path.join(OUT_DIR, "cross_correlation_features_heatmap.png")
plt.savefig(chart3_path)
plt.close()
shutil.copy(chart3_path, os.path.join(ARTIFACT_DIR, "cross_correlation_features_heatmap.png"))
print("Saved Chart 3:", chart3_path)
print("ALL CORRELATION CHARTS EXPORTED SUCCESSFULLY!")
