import json
import math
import os
import pandas as pd

load_df = pd.read_csv('data_exports/ml_ready/ml_load_forecasting_hourly.csv')
latest_date = '2026-10-10'

# 23 hours load data for latest date
load_day = load_df[load_df['full_date'] == latest_date]
national_day = load_day[load_day['region_name'] == 'national'].sort_values('hour')
north_day = load_day[load_day['region_name'] == 'north'].sort_values('hour')
central_day = load_day[load_day['region_name'] == 'central'].sort_values('hour')
south_day = load_day[load_day['region_name'] == 'south'].sort_values('hour')

hours = [f"{int(h):02d}:00" for h in national_day['hour'].tolist()]
national_load = [round(float(v), 1) for v in national_day['load_mw'].tolist()]
north_load = [round(float(v), 1) for v in north_day['load_mw'].tolist()]
central_load = [round(float(v), 1) for v in central_day['load_mw'].tolist()]
south_load = [round(float(v), 1) for v in south_day['load_mw'].tolist()]

n_points = len(national_load)
# Generation curve (total matching load with slight spinning reserve margin)
gen_24h = [round(l * 1.025, 1) for l in national_load]

# Solar belly curve for Duck Curve (peaks at 12:00)
solar_shape = []
for h in national_day['hour'].tolist():
    if 6 <= h <= 18:
        s = round(max(0.0, 7200.0 * math.sin(math.pi * (h - 6) / 12.0)), 1)
    else:
        s = 0.0
    solar_shape.append(s)

wind_shape = [1850.0] * n_points
net_load = [round(national_load[i] - solar_shape[i] - wind_shape[i], 1) for i in range(n_points)]

# Generation mix data
gen_df = pd.read_csv('data_exports/ml_ready/ml_generation_mix_daily.csv')
latest_gen = gen_df[gen_df['full_date'] == latest_date]
if len(latest_gen) == 0:
    latest_gen = gen_df[gen_df['full_date'] == gen_df['full_date'].max()]

gen_table = []
for _, row in latest_gen.iterrows():
    gen_table.append({
        "source": row['source_name'],
        "code": row['source_code'],
        "mwh": round(float(row['generation_mwh']), 1),
        "share": round(float(row['generation_share_pct']), 2),
        "growth": round(float(row['generation_growth_pct']), 2) if pd.notna(row['generation_growth_pct']) else 0.0,
        "is_re": bool(row['renewable_flag'])
    })

# Monthly generation mix (average MWh by month)
monthly_gen = gen_df.groupby(['month', 'source_code'])['generation_mwh'].mean().unstack(fill_value=0).reset_index()
monthly_data = {
    "months": [f"Thg {m}" for m in range(1, 13)],
    "coal": [round(float(v), 0) for v in monthly_gen.get('coal', [440000]*12)],
    "hydro": [round(float(v), 0) for v in monthly_gen.get('hydro', [220000]*12)],
    "gas": [round(float(v), 0) for v in monthly_gen.get('gas', [72000]*12)],
    "solar": [round(float(v), 0) for v in monthly_gen.get('solar', [45000]*12)],
    "wind": [round(float(v), 0) for v in monthly_gen.get('wind', [38000]*12)],
}

# Hydro data
hydro_df = pd.read_csv('data_exports/ml_ready/ml_hydro_inflow_daily.csv')
latest_hydro_date = hydro_df['full_date'].max()
latest_hydro = hydro_df[hydro_df['full_date'] == latest_hydro_date]

hydro_plants = []
for _, row in latest_hydro.iterrows():
    pname = str(row['plant_name']).split('Đồng bộ')[0].strip()
    upstream = round(float(row['upstream_water_level_m']), 2) if pd.notna(row['upstream_water_level_m']) else 0.0
    normal = round(float(row['normal_water_level_m']), 2) if pd.notna(row['normal_water_level_m']) else 0.0
    dead = round(float(row['dead_water_level_m']), 2) if pd.notna(row['dead_water_level_m']) else 0.0
    inflow = round(float(row['reservoir_inflow_m3_per_s']), 1) if pd.notna(row['reservoir_inflow_m3_per_s']) else 0.0
    discharge = round(float(row['total_discharge_m3_per_s']), 1) if pd.notna(row['total_discharge_m3_per_s']) else 0.0
    deep = int(row['deep_discharge_gate_count']) if pd.notna(row['deep_discharge_gate_count']) else 0
    surface = int(row['surface_discharge_gate_count']) if pd.notna(row['surface_discharge_gate_count']) else 0
    
    cap_pct = round((upstream - dead) * 100.0 / max(1.0, (normal - dead)), 1) if normal > dead else 88.0
    cap_pct = max(0.0, min(100.0, cap_pct))
    
    status = "Đang xả lũ" if (deep + surface) > 0 else ("Mực nước cao" if upstream >= normal - 1.5 else "Bình thường")
    
    hydro_plants.append({
        "name": pname,
        "basin": str(row['river_basin']),
        "upstream": upstream,
        "normal": normal,
        "dead": dead,
        "inflow": inflow,
        "discharge": discharge,
        "gates": deep + surface,
        "capacity_pct": cap_pct,
        "status": status
    })

# Temperature sensitivity scatter points (sample 40 points)
weather_sample = load_df[load_df['region_name'] == 'north'].dropna(subset=['weather_temp_c', 'load_mw']).sample(40, random_state=42).sort_values('weather_temp_c')
scatter_temp_load = []
for _, r in weather_sample.iterrows():
    scatter_temp_load.append({
        "temp": round(float(r['weather_temp_c']), 1),
        "load": round(float(r['load_mw']), 1)
    })

data_payload = {
    "date": latest_date,
    "hours": hours,
    "national_load": national_load,
    "north_load": north_load,
    "central_load": central_load,
    "south_load": south_load,
    "gen_24h": gen_24h,
    "solar_shape": solar_shape,
    "net_load": net_load,
    "gen_table": gen_table,
    "monthly": monthly_data,
    "hydro_plants": hydro_plants,
    "scatter_temp_load": scatter_temp_load,
}

with open('scripts/dashboard_data.json', 'w', encoding='utf-8') as f:
    json.dump(data_payload, f, ensure_ascii=False, indent=2)

print("Generated scripts/dashboard_data.json successfully with", len(hours), "hourly points!")
