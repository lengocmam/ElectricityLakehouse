-- ==============================================================================
-- SEMANTIC VIEWS FOR DASHBOARD 1 & DASHBOARD 2
-- Aligned 100% with User Specification & Storytelling Architecture
-- ==============================================================================

-- 1. v_load_national_hourly: National hourly load aggregated across 3 regions
CREATE OR REPLACE VIEW iceberg.gold.v_load_national_hourly AS
SELECT 
    d.full_date AS date_value,
    t.hour AS hour_of_day,
    SUM(l.load_mw) AS load_mw,
    COUNT(DISTINCT l.region_key) AS n_regions
FROM iceberg.gold.fact_load_hourly l
JOIN iceberg.gold.dim_date d ON l.date_key = d.date_key
JOIN iceberg.gold.dim_time t ON l.time_key = t.time_key
WHERE l.region_key IN (1, 2, 3)
GROUP BY d.full_date, t.hour;

-- 2. v_load_region_hourly: Regional hourly load (Bắc, Trung, Nam)
CREATE OR REPLACE VIEW iceberg.gold.v_load_region_hourly AS
SELECT 
    d.full_date AS date_value,
    t.hour AS hour_of_day,
    CASE 
        WHEN l.region_key = 1 THEN 'Bắc'
        WHEN l.region_key = 2 THEN 'Trung'
        WHEN l.region_key = 3 THEN 'Nam'
        ELSE 'Khác'
    END AS region_name,
    l.load_mw
FROM iceberg.gold.fact_load_hourly l
JOIN iceberg.gold.dim_date d ON l.date_key = d.date_key
JOIN iceberg.gold.dim_time t ON l.time_key = t.time_key
WHERE l.region_key IN (1, 2, 3);

-- 3. v_capacity_daily: Daily capacity and peak available dispatchable capacity
CREATE OR REPLACE VIEW iceberg.gold.v_capacity_daily AS
WITH period_totals AS (
    SELECT 
        data_date AS date_value,
        period,
        CASE 
            WHEN LOWER(generation_source) LIKE '%than%' THEN 'Than'
            WHEN LOWER(generation_source) LIKE '%khí%' OR LOWER(generation_source) LIKE '%gas%' THEN 'Khí'
            WHEN LOWER(generation_source) LIKE '%thủy điện%' THEN 'Thủy điện'
            WHEN LOWER(generation_source) LIKE '%mặt trời%' OR LOWER(generation_source) LIKE '%đmt%' THEN 'Mặt trời'
            WHEN LOWER(generation_source) LIKE '%gió%' THEN 'Gió'
            ELSE 'Khác'
        END AS source_group,
        SUM(dispatched_power_mw) AS period_mw
    FROM iceberg.silver.power
    WHERE generation_source NOT LIKE '%thương phẩm%' 
      AND generation_source NOT LIKE '%Quốc gia%'
    GROUP BY 
        data_date,
        period,
        CASE 
            WHEN LOWER(generation_source) LIKE '%than%' THEN 'Than'
            WHEN LOWER(generation_source) LIKE '%khí%' OR LOWER(generation_source) LIKE '%gas%' THEN 'Khí'
            WHEN LOWER(generation_source) LIKE '%thủy điện%' THEN 'Thủy điện'
            WHEN LOWER(generation_source) LIKE '%mặt trời%' OR LOWER(generation_source) LIKE '%đmt%' THEN 'Mặt trời'
            WHEN LOWER(generation_source) LIKE '%gió%' THEN 'Gió'
            ELSE 'Khác'
        END
)
SELECT 
    date_value,
    source_group,
    -- available_mw is the evening peak available capacity (or midday if evening is 0, like solar)
    COALESCE(
        NULLIF(MAX(CASE WHEN period = 'evening_peak' THEN period_mw ELSE 0 END), 0),
        MAX(period_mw)
    ) AS available_mw,
    MAX(period_mw) AS capacity_mw
FROM period_totals
GROUP BY date_value, source_group;

-- 4. v_gen_hourly: Realistic hourly dispatch generation shaped from daily fact totals
CREATE OR REPLACE VIEW iceberg.gold.v_gen_hourly AS
WITH hourly_dispatch_profile (hour_of_day, coal_factor, gas_factor, hydro_factor, solar_factor, wind_factor, other_factor) AS (
    VALUES
        ( 0, 1.0025, 0.6016, 0.9243, 0.0000, 1.0615, 1.0000),
        ( 1, 0.9825, 0.5469, 0.9028, 0.0000, 1.0918, 1.0000),
        ( 2, 0.9724, 0.5251, 0.8813, 0.0000, 1.1120, 1.0000),
        ( 3, 0.9724, 0.5251, 0.8706, 0.0000, 1.1120, 1.0000),
        ( 4, 0.9825, 0.5469, 0.8813, 0.0000, 1.0918, 1.0000),
        ( 5, 1.0025, 0.6563, 0.9136, 0.0000, 1.0312, 1.0000),
        ( 6, 1.0125, 0.8751, 0.9673, 0.1193, 0.9604, 1.0000),
        ( 7, 1.0226, 1.1486, 1.0210, 0.5963, 0.9099, 1.0000),
        ( 8, 1.0326, 1.2580, 1.0533, 1.3416, 0.8593, 1.0000),
        ( 9, 1.0025, 1.1486, 0.9673, 2.1863, 0.8290, 1.0000),
        (10, 0.9323, 0.9845, 0.8598, 2.9317, 0.8088, 1.0000),
        (11, 0.8622, 0.8204, 0.7846, 3.2298, 0.7885, 1.0000),
        (12, 0.8220, 0.7439, 0.7525, 3.3292, 0.7887, 1.0000),
        (13, 0.8521, 0.7876, 0.7631, 3.1304, 0.8088, 1.0000),
        (14, 0.9223, 0.9298, 0.8276, 2.7826, 0.8492, 1.0000),
        (15, 1.0025, 1.1486, 0.9673, 2.1366, 0.9099, 1.0000),
        (16, 1.0526, 1.3674, 1.1178, 1.4410, 0.9705, 1.0000),
        (17, 1.1028, 1.5861, 1.2682, 0.6758, 1.0716, 1.0000),
        (18, 1.1429, 1.6955, 1.3757, 0.0994, 1.1929, 1.0000),
        (19, 1.1228, 1.6190, 1.3435, 0.0000, 1.2131, 1.0000),
        (20, 1.0927, 1.4768, 1.2897, 0.0000, 1.1929, 1.0000),
        (21, 1.0627, 1.2580, 1.2038, 0.0000, 1.1626, 1.0000),
        (22, 1.0326, 0.9845, 1.0748, 0.0000, 1.1120, 1.0000),
        (23, 1.0125, 0.7657, 0.9888, 0.0000, 1.0716, 1.0000)
),
daily_gen AS (
    SELECT 
        data_date AS date_value,
        CASE 
            WHEN LOWER(energy_source) LIKE '%than%' THEN 'Than'
            WHEN LOWER(energy_source) LIKE '%khí%' OR LOWER(energy_source) LIKE '%gas%' THEN 'Khí'
            WHEN LOWER(energy_source) LIKE '%thủy điện%' THEN 'Thủy điện'
            WHEN LOWER(energy_source) LIKE '%mặt trời%' OR LOWER(energy_source) LIKE '%đmt%' THEN 'Mặt trời'
            WHEN LOWER(energy_source) LIKE '%gió%' THEN 'Gió'
            ELSE 'Khác'
        END AS source_group,
        SUM(generation_million_kwh * 1000.0) AS generation_mwh
    FROM iceberg.silver.generation
    WHERE energy_source NOT LIKE '%thương phẩm%'
    GROUP BY 
        data_date,
        CASE 
            WHEN LOWER(energy_source) LIKE '%than%' THEN 'Than'
            WHEN LOWER(energy_source) LIKE '%khí%' OR LOWER(energy_source) LIKE '%gas%' THEN 'Khí'
            WHEN LOWER(energy_source) LIKE '%thủy điện%' THEN 'Thủy điện'
            WHEN LOWER(energy_source) LIKE '%mặt trời%' OR LOWER(energy_source) LIKE '%đmt%' THEN 'Mặt trời'
            WHEN LOWER(energy_source) LIKE '%gió%' THEN 'Gió'
            ELSE 'Khác'
        END
)
SELECT 
    dg.date_value,
    dp.hour_of_day,
    dg.source_group,
    ROUND(
        (dg.generation_mwh / 24.0) * 
        CASE dg.source_group
            WHEN 'Than' THEN dp.coal_factor
            WHEN 'Khí' THEN dp.gas_factor
            WHEN 'Thủy điện' THEN dp.hydro_factor
            WHEN 'Mặt trời' THEN dp.solar_factor
            WHEN 'Gió' THEN dp.wind_factor
            ELSE dp.other_factor
        END, 1
    ) AS generation_mw
FROM daily_gen dg
CROSS JOIN hourly_dispatch_profile dp;

-- 5. v_weather_hourly: Regional hourly weather (Nhiệt độ, Mưa, Bức xạ)
CREATE OR REPLACE VIEW iceberg.gold.v_weather_hourly AS
SELECT 
    d.full_date AS date_value,
    t.hour AS hour_of_day,
    CASE 
        WHEN loc.region_key = 1 THEN 'Bắc'
        WHEN loc.region_key = 2 THEN 'Trung'
        WHEN loc.region_key = 3 THEN 'Nam'
        ELSE 'Khác'
    END AS region_name,
    ROUND(AVG(w.temperature_2m), 1) AS temperature_c,
    ROUND(SUM(w.precipitation), 1) AS precipitation_mm,
    ROUND(AVG(w.shortwave_radiation), 1) AS radiation_wm2
FROM iceberg.gold.fact_weather_hourly w
JOIN iceberg.gold.dim_date d ON w.date_key = d.date_key
JOIN iceberg.gold.dim_time t ON w.time_key = t.time_key
JOIN iceberg.gold.dim_location loc ON w.location_key = loc.location_key
WHERE loc.region_key IN (1, 2, 3)
GROUP BY 
    d.full_date,
    t.hour,
    CASE 
        WHEN loc.region_key = 1 THEN 'Bắc'
        WHEN loc.region_key = 2 THEN 'Trung'
        WHEN loc.region_key = 3 THEN 'Nam'
        ELSE 'Khác'
    END;

-- 6. v_hydro_daily: Daily reservoir status, hydrological flows, and multi-tier alert levels
CREATE OR REPLACE VIEW iceberg.gold.v_hydro_daily AS
WITH base AS (
    SELECT 
        d.full_date AS date_value,
        p.river_basin AS basin_name,
        split_part(p.plant_name, ' Đồng bộ', 1) AS reservoir_name,
        AVG(h.upstream_water_level_m) AS water_level_m,
        AVG(h.dead_water_level_m) AS dead_level_m,
        AVG(h.normal_water_level_m) AS normal_level_m,
        AVG(h.reservoir_inflow_m3_per_s) AS inflow_m3s,
        AVG(h.powerhouse_discharge_m3_per_s) AS turbine_flow_m3s,
        AVG(h.spillway_discharge_m3_per_s) AS spill_flow_m3s,
        MAX(h.deep_discharge_gate_count + h.surface_discharge_gate_count) AS spill_gates_open
    FROM iceberg.gold.fact_hydro_status h
    JOIN iceberg.gold.dim_date d ON h.date_key = d.date_key
    JOIN iceberg.gold.dim_plant p ON h.plant_key = p.plant_key
    GROUP BY d.full_date, p.river_basin, split_part(p.plant_name, ' Đồng bộ', 1)
),
rolling AS (
    SELECT 
        *,
        ROUND(
            (water_level_m - dead_level_m) * 100.0 / 
            NULLIF(normal_level_m - dead_level_m, 0), 1
        ) AS storage_pct,
        AVG(inflow_m3s) OVER (
            PARTITION BY reservoir_name 
            ORDER BY date_value 
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        ) AS inflow_avg_7d
    FROM base
)
SELECT 
    date_value,
    basin_name,
    reservoir_name,
    ROUND(water_level_m, 2) AS water_level_m,
    ROUND(dead_level_m, 2) AS dead_level_m,
    ROUND(normal_level_m, 2) AS normal_level_m,
    ROUND(inflow_m3s, 1) AS inflow_m3s,
    ROUND(turbine_flow_m3s, 1) AS turbine_flow_m3s,
    ROUND(spill_flow_m3s, 1) AS spill_flow_m3s,
    spill_gates_open,
    storage_pct,
    CASE 
        WHEN water_level_m >= normal_level_m AND inflow_m3s > (1.5 * COALESCE(inflow_avg_7d, inflow_m3s)) THEN 3
        WHEN spill_gates_open > 0 OR storage_pct >= 95.0 THEN 2
        ELSE 1
    END AS alert_level,
    CASE 
        WHEN water_level_m >= normal_level_m AND inflow_m3s > (1.5 * COALESCE(inflow_avg_7d, inflow_m3s)) THEN '🔴 Nguy hiểm'
        WHEN spill_gates_open > 0 OR storage_pct >= 95.0 THEN '🟡 Cảnh báo'
        ELSE '🟢 Bình thường'
    END AS alert_label
FROM rolling;
