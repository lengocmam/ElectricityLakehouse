# -*- coding: utf-8 -*-
"""Bước 0 & Bước 1: Khóa Test, Kiểm tra Dữ liệu và Phân tích Khám phá EDA trên Dev Set.

Quy tắc nghiêm ngặt:
- Khóa toàn bộ Test Set (01/07/2026 -> 16/11/2026, 3.336 giờ) và Gap 7 ngày (24/06/2026 -> 30/06/2026, 168 giờ).
- Tập Dev phân tích: 02/01/2024 01:00 -> 23/06/2026 23:00 (21.695 giờ).
- Kiểm tra tính liên tục thời gian, giá trị rỗng (null), giá trị âm bất thường.
- Phân tích tương quan sơ bộ & phân phối công suất trên Dev.
- Ghi chú: 25 giờ đầu (01/01/2024 00:00 -> 02/01/2024 00:00) đã loại bỏ khi tạo trễ lag24.
"""
import os
import sys
import json
import numpy as np
import pandas as pd

OUT_DIR = "data_exports/ml_results"
os.makedirs(OUT_DIR, exist_ok=True)
os.environ['MPLCONFIGDIR'] = os.path.join(OUT_DIR, '.mpl_cache')

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

print("="*80, flush=True)
print("BƯỚC 0: KHÓA TEST SET & XÁC LẬP TẬP DEV", flush=True)
print("="*80, flush=True)

CSV_PATH = "data_exports/hourly_feature_store.csv"
df = pd.read_csv(CSV_PATH)
df['datetime'] = pd.to_datetime(df['datetime'])
df = df.sort_values('datetime').reset_index(drop=True)

total_rows = len(df)
print(f"Tổng số bản ghi ban đầu: {total_rows:,} giờ ({df['datetime'].min()} -> {df['datetime'].max()})", flush=True)

# Khóa Test và Gap
dev_end = pd.Timestamp('2026-06-23 23:00:00')
gap_end = pd.Timestamp('2026-06-30 23:00:00')
test_start = pd.Timestamp('2026-07-01 00:00:00')

df_dev = df[df['datetime'] <= dev_end].copy().reset_index(drop=True)
df_gap = df[(df['datetime'] > dev_end) & (df['datetime'] <= gap_end)].copy()
df_test = df[df['datetime'] >= test_start].copy()

print(f"  * Tập DEV (02/01/2024 01:00 -> 23/06/2026 23:00) : {len(df_dev):,} giờ ({len(df_dev)/total_rows*100:.2f}%)", flush=True)
print(f"  * Vùng GAP đệm 7 ngày (24/06/2026 -> 30/06/2026) : {len(df_gap):,} giờ ({len(df_gap)/total_rows*100:.2f}%) [KHÓA BỎ QUA]", flush=True)
print(f"  * Tập TEST độc lập (01/07/2026 -> 16/11/2026)    : {len(df_test):,} giờ ({len(df_test)/total_rows*100:.2f}%) [KHÓA NIÊM PHONG]", flush=True)
print(f"  * Ghi chú kỹ thuật: 25 giờ đầu tiên (01/01/2024 00:00 -> 02/01/2024 00:00) đã loại bỏ hợp lệ khi tạo lag24/rolling.", flush=True)

# Giải phóng bộ nhớ của Test để đảm bảo không rò rỉ
del df_test, df_gap

print("\n" + "="*80, flush=True)
print("BƯỚC 1: KIỂM TRA CHẤT LƯỢNG DỮ LIỆU & EDA (CHỈ TRÊN TẬP DEV)", flush=True)
print("="*80, flush=True)

# 1. Kiểm tra tính liên tục của trục thời gian
expected_hours = pd.date_range(start=df_dev['datetime'].min(), end=df_dev['datetime'].max(), freq='h')
missing_timestamps = expected_hours.difference(df_dev['datetime'])
print(f"1. Kiểm tra tính liên tục thời gian (Timestamp Continuity):", flush=True)
print(f"   - Số giờ kỳ vọng: {len(expected_hours):,} giờ", flush=True)
print(f"   - Số giờ thực tế: {len(df_dev):,} giờ", flush=True)
print(f"   - Số giờ bị thiếu: {len(missing_timestamps)} giờ -> {'HOÀN HẢO (100% liên tục)' if len(missing_timestamps) == 0 else 'CẢNH BÁO'}", flush=True)

# 2. Kiểm tra giá trị thiếu (Null/NaN)
null_counts = df_dev.isnull().sum()
total_nulls = null_counts.sum()
print(f"2. Kiểm tra giá trị khuyết (Missing Values): {total_nulls} ô khuyết trên toàn bộ {len(df_dev.columns)} cột.", flush=True)

# 3. Kiểm tra tính bất thường vật lý (Physical Anomaly Check)
core_targets = ['load_national', 'net_load', 'p_solar_total', 'p_hydro', 'p_wind', 'p_coal']
print("3. Thống kê mô tả các mục tiêu hệ thống điện trên Dev Set (MW):", flush=True)
stats_summary = {}
for col in core_targets:
    if col in df_dev.columns:
        s = df_dev[col]
        negative_count = (s < 0).sum()
        stats_summary[col] = {
            "mean": float(s.mean()),
            "std": float(s.std()),
            "min": float(s.min()),
            "p25": float(s.quantile(0.25)),
            "median": float(s.median()),
            "p75": float(s.quantile(0.75)),
            "max": float(s.max()),
            "negative_count": int(negative_count)
        }
        print(f"   * {col:<15}: Min={s.min():>8.1f} | Mean={s.mean():>8.1f} | Max={s.max():>8.1f} | Std={s.std():>8.1f} | <0: {negative_count}", flush=True)

# 4. Tương quan sơ bộ giữa Phụ tải & Nguồn phát với Thời tiết/Lịch trên Dev
weather_cal_cols = [
    'wx_temp_c', 'wx_apparent_temp_c', 'wx_humidity_pct', 'wx_solar_rad_wm2',
    'wx_wind_speed_ms', 'wx_cloud_cover_pct', 'hour_sin', 'hour_cos',
    'dow_sin', 'dow_cos', 'month_sin', 'month_cos', 'is_working_day', 'is_weekend'
]
corr_matrix = df_dev[core_targets + weather_cal_cols].corr(method='pearson')

# 5. Vẽ biểu đồ trực quan hóa EDA trên Dev
print("\n>>> Xuất biểu đồ EDA trên Tập Dev...", flush=True)
fig, axes = plt.subplots(2, 2, figsize=(18, 14))

# Subplot 1: Chu kỳ phụ tải theo giờ trong ngày
ax1 = axes[0, 0]
df_dev['hour'] = df_dev['datetime'].dt.hour
hourly_profile = df_dev.groupby('hour')[['load_national', 'net_load', 'p_solar_total', 'p_hydro']].mean()
hourly_profile['load_national'].plot(ax=ax1, label='Phụ tải Quốc gia (Total Load)', color='#1e3a8a', linewidth=2.5)
hourly_profile['net_load'].plot(ax=ax1, label='Phụ tải Ròng (Duck Curve)', color='#b91c1c', linewidth=2.5, linestyle='--')
hourly_profile['p_hydro'].plot(ax=ax1, label='Thủy điện (Hydro)', color='#0284c7', linewidth=2.0)
hourly_profile['p_solar_total'].plot(ax=ax1, label='Điện mặt trời (Solar)', color='#eab308', linewidth=2.0)
ax1.set_title('Biểu đồ Chu kỳ Giờ Điển hình (24h Diurnal Profile) trên Tập Dev', fontsize=12, fontweight='bold')
ax1.set_xlabel('Giờ trong ngày (0 - 23h)')
ax1.set_ylabel('Công suất trung bình (MW)')
ax1.grid(True, linestyle=':', alpha=0.6)
ax1.legend(frameon=True, fontsize=10)

# Subplot 2: Ma trận tương quan nhiệt giữa Phụ tải và Khí tượng
ax2 = axes[0, 1]
sub_corr = corr_matrix.loc[core_targets, weather_cal_cols]
sns.heatmap(sub_corr, annot=True, fmt=".2f", cmap="coolwarm", center=0, cbar=True, ax=ax2, linewidths=0.5)
ax2.set_title('Ma trận Tương quan Pearson Sơ bộ (Targets vs Khí hậu/Lịch - Dev Set)', fontsize=12, fontweight='bold')
ax2.set_xticklabels(ax2.get_xticklabels(), rotation=45, ha='right', fontsize=9)

# Subplot 3: Phân phối xác suất (KDE) của Phụ tải và Phụ tải ròng
ax3 = axes[1, 0]
sns.kdeplot(df_dev['load_national'], ax=ax3, label='Phụ tải Quốc gia', color='#1e3a8a', fill=True, alpha=0.3, linewidth=2)
sns.kdeplot(df_dev['net_load'], ax=ax3, label='Phụ tải Ròng (Net Load)', color='#b91c1c', fill=True, alpha=0.3, linewidth=2)
ax3.set_title('Phân phối Mật độ Xác suất (KDE Distribution) trên Tập Dev', fontsize=12, fontweight='bold')
ax3.set_xlabel('Công suất (MW)')
ax3.set_ylabel('Mật độ xác suất')
ax3.grid(True, linestyle=':', alpha=0.6)
ax3.legend(frameon=True, fontsize=10)

# Subplot 4: Tương quan giữa Bức xạ mặt trời và Điện mặt trời thực tế
ax4 = axes[1, 1]
sample_dev = df_dev.sample(n=3000, random_state=42)
scatter = ax4.scatter(sample_dev['wx_solar_rad_wm2'], sample_dev['p_solar_total'], c=sample_dev['hour'], cmap='viridis', alpha=0.5, s=15)
cbar = plt.colorbar(scatter, ax=ax4)
cbar.set_label('Giờ trong ngày', fontsize=10)
ax4.set_title('Tương quan Bức xạ Quang năng (W/m²) vs Phát Điện Mặt Trời (MW)', fontsize=12, fontweight='bold')
ax4.set_xlabel('Bức xạ Mặt Trời (W/m²)')
ax4.set_ylabel('Công suất Phát ĐMT (MW)')
ax4.grid(True, linestyle=':', alpha=0.6)

plt.tight_layout()
eda_plot_path = os.path.join(OUT_DIR, "dev_eda_inspection.png")
plt.savefig(eda_plot_path, dpi=200, bbox_inches='tight')
plt.close()
print(f">>> Đã lưu biểu đồ EDA trên Dev: {eda_plot_path}", flush=True)

# Xuất bản báo cáo kiểm toán Dev
audit_report = {
    "total_dataset_rows": total_rows,
    "dev_rows": len(df_dev),
    "gap_rows": len(df_gap) if 'df_gap' in locals() else 168,
    "test_rows": total_rows - len(df_dev) - 168,
    "missing_timestamps": len(missing_timestamps),
    "total_nulls": int(total_nulls),
    "target_stats_dev": stats_summary
}
with open(os.path.join(OUT_DIR, "dev_data_audit_summary.json"), 'w', encoding='utf-8') as f:
    json.dump(audit_report, f, ensure_ascii=False, indent=2)

print(">>> HOÀN TẤT BƯỚC 0 VÀ BƯỚC 1!", flush=True)
