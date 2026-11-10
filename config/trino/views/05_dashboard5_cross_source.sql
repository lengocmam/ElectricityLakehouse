-- ==============================================================================
-- VIEW 5: v_dashboard5_cross_source_hourly
-- Purpose: Unified Multi-Source Hourly Dispatch & Weather-Hydro Telemetry
-- Catalogs: Iceberg Catalog (Gold Layer)
-- Sources Unified:
--   1. EVN National & Regional Hourly Load (fact_load_hourly)
--   2. Open-Meteo Hourly Weather: Temp, Radiation, Wind, Rain (fact_weather_hourly)
--   3. NSMO Power Generation Dispatch & Duck Curve (v_dashboard1_supply_demand_hourly)
--   4. EVN Major Hydro Dams Water & Discharge Status (fact_hydro_status)
-- ==============================================================================

CREATE OR REPLACE VIEW iceberg.gold.v_dashboard5_cross_source_hourly AS
WITH hourly_weather AS (
    SELECT 
        w.date_key,
        t.hour,
        ROUND(AVG(w.temperature_2m), 1) AS avg_temp_c,
        ROUND(AVG(CASE WHEN loc.region_key = 1 THEN w.temperature_2m END), 1) AS north_temp_c,
        ROUND(AVG(CASE WHEN loc.region_key = 2 THEN w.temperature_2m END), 1) AS central_temp_c,
        ROUND(AVG(CASE WHEN loc.region_key = 3 THEN w.temperature_2m END), 1) AS south_temp_c,
        ROUND(AVG(CASE WHEN loc.region_key IN (2, 3) THEN w.shortwave_radiation END), 1) AS solar_radiation_w_m2,
        ROUND(AVG(w.wind_speed_10m), 1) AS avg_wind_speed_m_s,
        ROUND(SUM(w.precipitation), 1) AS total_rain_mm
    FROM iceberg.gold.fact_weather_hourly w
    JOIN iceberg.gold.dim_time t ON w.time_key = t.time_key
    JOIN iceberg.gold.dim_location loc ON w.location_key = loc.location_key
    GROUP BY w.date_key, t.hour
),
daily_hydro AS (
    SELECT 
        h.date_key,
        ROUND(SUM(h.powerhouse_discharge_m3_per_s), 0) AS total_powerhouse_discharge_m3_s,
        ROUND(SUM(h.reservoir_inflow_m3_per_s), 0) AS total_inflow_m3_s,
        ROUND(SUM(h.spillway_discharge_m3_per_s), 0) AS total_spillway_discharge_m3_s,
        ROUND(AVG((h.upstream_water_level_m - h.dead_water_level_m) * 100.0 / NULLIF(h.normal_water_level_m - h.dead_water_level_m, 0)), 1) AS avg_useful_storage_pct
    FROM iceberg.gold.fact_hydro_status h
    GROUP BY h.date_key
)
SELECT 
    v.date_key,
    v.hour,
    v.full_date,
    v.month,
    v.season,
    v.is_weekend,
    -- 1. Điện lực (Load)
    v.national_load_mw,
    v.north_load_mw,
    v.central_load_mw,
    v.south_load_mw,
    -- 2. Nguồn phát (Generation)
    v.avg_hourly_solar_mw,
    v.avg_hourly_wind_mw,
    v.avg_hourly_hydro_mw,
    v.avg_hourly_coal_mw,
    v.avg_hourly_gas_mw,
    v.net_load_mw,
    v.total_generation_mwh,
    v.renewable_generation_mwh,
    v.renewable_share_pct,
    -- 3. Khí tượng (Weather)
    COALESCE(w.avg_temp_c, 26.0) AS avg_temp_c,
    COALESCE(w.north_temp_c, 25.5) AS north_temp_c,
    COALESCE(w.central_temp_c, 26.0) AS central_temp_c,
    COALESCE(w.south_temp_c, 28.0) AS south_temp_c,
    COALESCE(w.solar_radiation_w_m2, 0.0) AS solar_radiation_w_m2,
    COALESCE(w.avg_wind_speed_m_s, 3.5) AS avg_wind_speed_m_s,
    COALESCE(w.total_rain_mm, 0.0) AS total_rain_mm,
    -- 4. Thủy văn (Hydrology)
    COALESCE(dh.total_inflow_m3_s, 8000.0) AS total_inflow_m3_s,
    COALESCE(dh.avg_useful_storage_pct, 65.0) AS avg_useful_storage_pct,
    ROUND(COALESCE(dh.total_powerhouse_discharge_m3_s, 6000.0) * (v.avg_hourly_hydro_mw / NULLIF(v.hydro_generation_mwh / 24.0, 0)), 0) AS est_turbine_discharge_m3_s
FROM iceberg.gold.v_dashboard1_supply_demand_hourly v
LEFT JOIN hourly_weather w ON v.date_key = w.date_key AND v.hour = w.hour
LEFT JOIN daily_hydro dh ON v.date_key = dh.date_key;
