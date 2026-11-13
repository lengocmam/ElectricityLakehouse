-- ==============================================================================
-- VIEW 2: v_dashboard2_climate_hydro_daily
-- Purpose: Serve Dashboard 2 (Climate Resilience, Hydropower & Water Security)
-- Query Engine: Trino (Iceberg Catalog)
-- Performance: Pre-aggregates daily reservoir status, basin totals, and regional weather
-- ==============================================================================

CREATE OR REPLACE VIEW iceberg.gold.v_dashboard2_climate_hydro_daily AS
WITH hydro_daily AS (
    SELECT 
        h.date_key,
        d.full_date,
        d.month,
        d.season,
        p.plant_key,
        split_part(p.plant_name, ' Đồng bộ', 1) AS plant_name,
        p.river_basin,
        h.upstream_water_level_m,
        h.normal_water_level_m,
        h.dead_water_level_m,
        h.reservoir_inflow_m3_per_s,
        h.total_discharge_m3_per_s,
        h.spillway_discharge_m3_per_s,
        h.powerhouse_discharge_m3_per_s,
        h.deep_discharge_gate_count,
        h.surface_discharge_gate_count,
        (h.deep_discharge_gate_count + h.surface_discharge_gate_count) AS total_gates_open,
        ROUND((h.upstream_water_level_m - h.dead_water_level_m) * 100.0 / 
              NULLIF(h.normal_water_level_m - h.dead_water_level_m, 0), 1) AS useful_capacity_pct,
        CASE 
            WHEN (h.deep_discharge_gate_count + h.surface_discharge_gate_count) > 0 THEN 'Đang xả lũ'
            WHEN h.upstream_water_level_m >= (h.normal_water_level_m - 1.5) THEN 'Mực nước cao / Đầy hồ'
            WHEN h.upstream_water_level_m <= (h.dead_water_level_m + 3.0) THEN 'Nguy cơ kiệt nước'
            ELSE 'Vận hành bình thường'
        END AS reservoir_alert_status
    FROM iceberg.gold.fact_hydro_status h
    JOIN iceberg.gold.dim_plant p ON h.plant_key = p.plant_key
    JOIN iceberg.gold.dim_date d ON h.date_key = d.date_key
),
weather_regional_daily AS (
    SELECT 
        w.date_key,
        loc.region_key,
        ROUND(AVG(w.temperature_2m), 1) AS avg_temperature_c,
        ROUND(MAX(w.temperature_2m), 1) AS max_temperature_c,
        ROUND(MIN(w.temperature_2m), 1) AS min_temperature_c,
        ROUND(AVG(w.apparent_temperature), 1) AS avg_apparent_temp_c,
        ROUND(AVG(w.relative_humidity_2m), 1) AS avg_humidity_pct,
        ROUND(SUM(w.precipitation), 1) AS total_precipitation_mm,
        ROUND(AVG(w.shortwave_radiation), 1) AS avg_solar_radiation_w_m2,
        ROUND(AVG(w.wind_speed_10m), 1) AS avg_wind_speed_m_s
    FROM iceberg.gold.fact_weather_hourly w
    JOIN iceberg.gold.dim_location loc ON w.location_key = loc.location_key
    GROUP BY w.date_key, loc.region_key
),
national_weather_daily AS (
    SELECT 
        date_key,
        ROUND(AVG(avg_temperature_c), 1) AS national_avg_temp_c,
        ROUND(MAX(max_temperature_c), 1) AS national_max_temp_c,
        ROUND(AVG(avg_apparent_temp_c), 1) AS national_apparent_temp_c,
        ROUND(AVG(avg_solar_radiation_w_m2), 1) AS national_solar_radiation_w_m2,
        ROUND(SUM(total_precipitation_mm), 1) AS national_precipitation_mm
    FROM weather_regional_daily
    GROUP BY date_key
),
national_load_daily AS (
    SELECT 
        l.date_key,
        MAX(l.daily_peak_load_mw) AS peak_load_mw,
        AVG(l.daily_avg_load_mw) AS avg_load_mw
    FROM iceberg.gold.fact_load_hourly l
    GROUP BY l.date_key
),
solar_generation_daily AS (
    SELECT 
        g.date_key,
        SUM(CASE WHEN LOWER(s.source_code) = 'solar' THEN g.generation_mwh ELSE 0 END) AS solar_generation_mwh
    FROM iceberg.gold.fact_generation_daily g
    JOIN iceberg.gold.dim_energy_source s ON g.energy_source_key = s.energy_source_key
    GROUP BY g.date_key
)
SELECT 
    hd.*,
    nw.national_avg_temp_c,
    nw.national_max_temp_c,
    nw.national_apparent_temp_c,
    nw.national_solar_radiation_w_m2,
    nw.national_precipitation_mm,
    nl.peak_load_mw,
    nl.avg_load_mw,
    sg.solar_generation_mwh
FROM hydro_daily hd
LEFT JOIN national_weather_daily nw ON hd.date_key = nw.date_key
LEFT JOIN national_load_daily nl ON hd.date_key = nl.date_key
LEFT JOIN solar_generation_daily sg ON hd.date_key = sg.date_key;
