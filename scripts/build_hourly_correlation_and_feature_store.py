"""Build Hourly Feature Store and Correlation Analysis (Strict Zero-Leakage).

Extracts from Trino Iceberg Gold tables:
- fact_generation_power_hourly
- fact_load_hourly
- fact_weather_hourly
- dim_date

Performs Chronological Split (Train 70% / Val 15% / Test 15%).
Computes Correlation Matrix & Partial Correlations ONLY on Train set.
Saves heatmaps and the final Feature Store CSV.
"""
import json
import os
import urllib.request
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats

def query_trino(sql):
    req = urllib.request.Request(
        'http://localhost:8082/v1/statement',
        data=sql.encode('utf-8'),
        headers={'X-Trino-User': 'trino', 'X-Trino-Catalog': 'iceberg', 'X-Trino-Schema': 'gold'}
    )
    with urllib.request.urlopen(req) as resp:
        res = json.loads(resp.read().decode())
    columns = [c['name'] for c in res.get('columns', [])]
    data = res.get('data', [])
    next_uri = res.get('nextUri')
    while next_uri:
        req = urllib.request.Request(next_uri, headers={'X-Trino-User': 'trino'})
        with urllib.request.urlopen(req) as resp:
            res = json.loads(resp.read().decode())
        if not columns and 'columns' in res:
            columns = [c['name'] for c in res['columns']]
        data.extend(res.get('data', []))
        next_uri = res.get('nextUri')
    return pd.DataFrame(data, columns=columns)

print("1. Querying Gold facts from Trino Iceberg...")

# 1. Generation power hourly pivoted
sql_gen = """
SELECT 
    date_key, 
    time_key,
    COALESCE(SUM(CASE WHEN s.source_name = 'Nhiệt điện than' THEN generation_power_mw END), 0) AS p_coal,
    COALESCE(SUM(CASE WHEN s.source_name = 'Thủy điện' THEN generation_power_mw END), 0) AS p_hydro,
    COALESCE(SUM(CASE WHEN s.source_name = 'Tuabin khí (GAS)' THEN generation_power_mw END), 0) AS p_gas,
    COALESCE(SUM(CASE WHEN s.source_name = 'Tuabin khí (DO)' THEN generation_power_mw END), 0) AS p_gas_do,
    COALESCE(SUM(CASE WHEN s.source_name = 'Gió' THEN generation_power_mw END), 0) AS p_wind,
    COALESCE(SUM(CASE WHEN s.source_name = 'Mặt trời farm' THEN generation_power_mw END), 0) AS p_solar_farm,
    COALESCE(SUM(CASE WHEN s.source_name = 'ĐMTMN (ĐC)' THEN generation_power_mw END), 0) AS p_solar_rooftop,
    COALESCE(SUM(CASE WHEN s.source_name = 'Sinh khối' THEN generation_power_mw END), 0) AS p_biomass,
    COALESCE(SUM(CASE WHEN s.source_name = 'Mua TQ' THEN generation_power_mw END), 0) AS p_import,
    COALESCE(SUM(CASE WHEN s.source_name = 'Nhiệt điện dầu FO' THEN generation_power_mw END), 0) AS p_oil_fo,
    COALESCE(SUM(CASE WHEN s.source_name = 'Nhiệt điện dầu DO' THEN generation_power_mw END), 0) AS p_oil_do,
    COALESCE(SUM(CASE WHEN s.source_name = 'Nhiệt điện khác' THEN generation_power_mw END), 0) AS p_thermal_other
FROM fact_generation_power_hourly f
JOIN dim_energy_source s ON f.energy_source_key = s.energy_source_key
GROUP BY date_key, time_key
ORDER BY date_key, time_key
"""
df_gen = query_trino(sql_gen)
for col in df_gen.columns:
    df_gen[col] = pd.to_numeric(df_gen[col])
print(f"   - Generation rows: {len(df_gen)}")

# 2. Load hourly pivoted
sql_load = """
SELECT 
    date_key,
    time_key,
    COALESCE(SUM(CASE WHEN r.region_name = 'national' THEN load_mw END), 0) AS load_national,
    COALESCE(SUM(CASE WHEN r.region_name = 'north' THEN load_mw END), 0) AS load_north,
    COALESCE(SUM(CASE WHEN r.region_name = 'central' THEN load_mw END), 0) AS load_central,
    COALESCE(SUM(CASE WHEN r.region_name = 'south' THEN load_mw END), 0) AS load_south
FROM fact_load_hourly f
JOIN dim_region r ON f.region_key = r.region_key
GROUP BY date_key, time_key
ORDER BY date_key, time_key
"""
df_load = query_trino(sql_load)
for col in df_load.columns:
    df_load[col] = pd.to_numeric(df_load[col])
print(f"   - Load rows: {len(df_load)}")

# 3. Weather hourly aggregated across stations
sql_weather = """
SELECT 
    date_key,
    time_key,
    ROUND(AVG(temperature_2m), 2) AS wx_temp_c,
    ROUND(AVG(apparent_temperature), 2) AS wx_apparent_temp_c,
    ROUND(AVG(relative_humidity_2m), 2) AS wx_humidity_pct,
    ROUND(AVG(shortwave_radiation), 2) AS wx_solar_rad_wm2,
    ROUND(AVG(wind_speed_10m), 2) AS wx_wind_speed_ms,
    ROUND(AVG(cloud_cover), 2) AS wx_cloud_cover_pct,
    ROUND(AVG(precipitation), 2) AS wx_precip_mm
FROM fact_weather_hourly
GROUP BY date_key, time_key
ORDER BY date_key, time_key
"""
df_weather = query_trino(sql_weather)
for col in df_weather.columns:
    df_weather[col] = pd.to_numeric(df_weather[col])
print(f"   - Weather rows: {len(df_weather)}")

# 4. Join datasets
print("2. Joining fact tables and building features...")
df = pd.merge(df_gen, df_load, on=['date_key', 'time_key'], how='inner')
df = pd.merge(df, df_weather, on=['date_key', 'time_key'], how='inner')

# Parse date
df['date_str'] = df['date_key'].astype(str)
df['datetime'] = pd.to_datetime(df['date_str'] + ' ' + df['time_key'].astype(str) + ':00:00')
df = df.sort_values('datetime').reset_index(drop=True)

# Domain Engineering
df['p_solar_total'] = df['p_solar_farm'] + df['p_solar_rooftop']
df['p_gas_total'] = df['p_gas'] + df['p_gas_do']
df['gen_renewables'] = df['p_solar_total'] + df['p_wind']
df['gen_total'] = (df['p_coal'] + df['p_hydro'] + df['p_gas_total'] + 
                   df['p_solar_total'] + df['p_wind'] + df['p_biomass'] + 
                   df['p_import'] + df['p_oil_fo'] + df['p_oil_do'] + df['p_thermal_other'])
df['net_load'] = df['load_national'] - df['gen_renewables']

# Calendar & Periodic
hour = df['time_key']
df['hour_sin'] = np.sin(2 * np.pi * hour / 24)
df['hour_cos'] = np.cos(2 * np.pi * hour / 24)

dow = df['datetime'].dt.dayofweek
df['dow_sin'] = np.sin(2 * np.pi * dow / 7)
df['dow_cos'] = np.cos(2 * np.pi * dow / 7)
df['is_weekend'] = (dow >= 5).astype(float)
df['is_working_day'] = (dow < 5).astype(float)

month = df['datetime'].dt.month
df['month_sin'] = np.sin(2 * np.pi * (month - 1) / 12)
df['month_cos'] = np.cos(2 * np.pi * (month - 1) / 12)

# Autoregressive Lags (strictly past <= t-1)
print("3. Generating autoregressive lag features...")
for target_name in ['load_national', 'net_load', 'p_solar_total', 'p_wind', 'p_hydro', 'p_coal']:
    df[f'{target_name}_lag1'] = df[target_name].shift(1)
    df[f'{target_name}_lag2'] = df[target_name].shift(2)
    df[f'{target_name}_lag24'] = df[target_name].shift(24)

# Weather lags
df['wx_solar_rad_lag1'] = df['wx_solar_rad_wm2'].shift(1)
df['wx_temp_lag1'] = df['wx_temp_c'].shift(1)
df['wx_wind_lag1'] = df['wx_wind_speed_ms'].shift(1)

# Drop initial NaNs from lag24
df = df.dropna().reset_index(drop=True)
print(f"   - Final clean hourly dataset: {len(df)} rows ({df['datetime'].min()} to {df['datetime'].max()})")

# 5. Chronological Split (Anti-Leakage)
print("4. Applying Chronological Split (Train 70% / Val 15% / Test 15%)...")
# Train: 2024-01-01 -> 2025-12-31 (~70%)
# Val:   2026-01-01 -> 2026-06-30 (~15%)
# Test:  2026-07-01 -> 2026-11-16 (~15%)
train_mask = df['datetime'] <= '2025-12-31 23:00:00'
val_mask = (df['datetime'] > '2025-12-31 23:00:00') & (df['datetime'] <= '2026-06-30 23:00:00')
test_mask = df['datetime'] > '2026-06-30 23:00:00'

df_train = df[train_mask].copy()
df_val = df[val_mask].copy()
df_test = df[test_mask].copy()

print(f"   - Train set: {len(df_train)} rows ({len(df_train)/len(df)*100:.1f}%) | {df_train['datetime'].min()} -> {df_train['datetime'].max()}")
print(f"   - Val set:   {len(df_val)} rows ({len(df_val)/len(df)*100:.1f}%) | {df_val['datetime'].min()} -> {df_val['datetime'].max()}")
print(f"   - Test set:  {len(df_test)} rows ({len(df_test)/len(df)*100:.1f}%) | {df_test['datetime'].min()} -> {df_test['datetime'].max()}")

# 6. Correlation Matrix STRICTLY ON TRAIN SET
print("5. Computing Correlation Matrix ONLY on Train Set...")
analysis_cols = [
    # Targets
    'load_national', 'net_load', 'p_solar_total', 'p_wind', 'p_hydro', 'p_coal', 'p_gas_total',
    # Weather
    'wx_temp_c', 'wx_apparent_temp_c', 'wx_humidity_pct', 'wx_solar_rad_wm2', 
    'wx_wind_speed_ms', 'wx_cloud_cover_pct', 'wx_precip_mm',
    # Periodic / Calendar
    'hour_sin', 'hour_cos', 'dow_sin', 'dow_cos', 'month_sin', 'month_cos', 'is_weekend',
    # Autoregressive past
    'load_national_lag1', 'load_national_lag24',
    'net_load_lag1', 'net_load_lag24',
    'p_solar_total_lag1', 'p_solar_total_lag24',
    'p_wind_lag1', 'p_wind_lag24',
    'p_hydro_lag1', 'p_hydro_lag24',
    'p_coal_lag1', 'p_coal_lag24'
]

train_corr = df_train[analysis_cols].corr()

# Partial Correlation function
def partial_corr(data, x, y, covars):
    cols = [x, y] + covars
    sub = data[cols].dropna()
    inv = np.linalg.pinv(sub.cov().values)
    r = -inv[0, 1] / np.sqrt(inv[0, 0] * inv[1, 1])
    return r

# Save CSV
os.makedirs('data_exports', exist_ok=True)
df.to_csv('data_exports/hourly_feature_store.csv', index=False)
print("   - Saved: data_exports/hourly_feature_store.csv")

# 7. Plot Correlation Heatmaps
print("6. Generating Correlation Visualizations...")
os.makedirs('s4_3thuat_toan/figures', exist_ok=True)

# Heatmap 1: Targets vs Weather & Calendar
plt.figure(figsize=(14, 8), dpi=150)
target_cols = ['load_national', 'net_load', 'p_solar_total', 'p_wind', 'p_hydro', 'p_coal', 'p_gas_total']
feature_cols = ['wx_temp_c', 'wx_apparent_temp_c', 'wx_humidity_pct', 'wx_solar_rad_wm2', 
                'wx_wind_speed_ms', 'wx_cloud_cover_pct', 'wx_precip_mm',
                'hour_sin', 'hour_cos', 'dow_sin', 'month_cos', 'month_sin', 'is_weekend']

sub_corr = train_corr.loc[target_cols, feature_cols]
sns.heatmap(sub_corr, annot=True, fmt=".2f", cmap='coolwarm', center=0, vmin=-1, vmax=1,
            cbar_kws={'label': 'Hệ số tương quan Pearson (r)'}, linewidths=0.5)
plt.title('Ma trận Tương quan: Cơ cấu Nguồn & Phụ tải vs Thời tiết & Chu kỳ (Tính trên Tập Train)', fontsize=13, pad=15)
plt.xlabel('Đặc trưng Thời tiết & Chu kỳ thời gian', fontsize=11)
plt.ylabel('Mục tiêu Dự báo (Targets)', fontsize=11)
plt.xticks(rotation=45, ha='right')
plt.tight_layout()
heatmap_path1 = 's4_3thuat_toan/figures/correlation_targets_vs_weather.png'
plt.savefig(heatmap_path1)
plt.close()
print(f"   - Saved: {heatmap_path1}")

# Heatmap 2: Full Correlation Heatmap
plt.figure(figsize=(18, 14), dpi=150)
sns.heatmap(train_corr, annot=False, cmap='vlag', center=0, vmin=-1, vmax=1,
            cbar_kws={'label': 'Pearson r'}, linewidths=0.2)
plt.title('Ma trận Tương quan Toàn diện (Chống rò rỉ - Chỉ tính trên 70% dữ liệu Train)', fontsize=14, pad=15)
plt.tight_layout()
heatmap_path2 = 's4_3thuat_toan/figures/correlation_matrix_hourly_full.png'
plt.savefig(heatmap_path2)
plt.close()
print(f"   - Saved: {heatmap_path2}")

# 8. Output Analysis Summary
print("\n" + "="*80)
print("KẾT QUẢ PHÂN TÍCH TƯƠNG QUAN & LỰA CHỌN ĐẶC TRƯNG (CHỈ TẬP TRAIN)")
print("="*80)

summary_results = {}
for target in ['load_national', 'net_load', 'p_solar_total', 'p_wind', 'p_hydro', 'p_coal']:
    corrs = train_corr[target].drop(target)
    # Top 5 positive and negative
    top_pos = corrs.sort_values(ascending=False).head(4)
    top_neg = corrs.sort_values(ascending=True).head(3)
    
    # Calculate key partial correlations
    p_rad = partial_corr(df_train, target, 'wx_solar_rad_wm2', ['hour_sin', 'hour_cos'])
    p_temp = partial_corr(df_train, target, 'wx_temp_c', ['hour_sin', 'hour_cos', 'month_cos'])
    p_wind = partial_corr(df_train, target, 'wx_wind_speed_ms', ['hour_sin', 'hour_cos'])
    
    summary_results[target] = {
        'top_pos': top_pos.to_dict(),
        'top_neg': top_neg.to_dict(),
        'p_rad': round(p_rad, 4),
        'p_temp': round(p_temp, 4),
        'p_wind': round(p_wind, 4)
    }
    print(f"\n[TARGET: {target.upper()}]")
    print(f"  • Top tương quan dương mạnh nhất: {', '.join([f'{k} (r={v:.3f})' for k,v in top_pos.items()])}")
    print(f"  • Top tương quan âm mạnh nhất:   {', '.join([f'{k} (r={v:.3f})' for k,v in top_neg.items()])}")
    print(f"  • Tương quan riêng phần kiểm soát chu kỳ:")
    print(f"    - r_partial(Thời tiết nhiệt độ): {p_temp:.4f}")
    print(f"    - r_partial(Bức xạ mặt trời):   {p_rad:.4f}")
    print(f"    - r_partial(Tốc độ gió):        {p_wind:.4f}")

# Save json summary
with open('s4_3thuat_toan/figures/correlation_summary.json', 'w', encoding='utf-8') as f:
    json.dump(summary_results, f, indent=2, ensure_ascii=False)
print("\nHoàn tất phân tích tương quan và trích xuất Feature Store!")
