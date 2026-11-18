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
    FROM iceberg.silver.power_daily
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

-- 4. v_gen_hourly: Measured hourly dispatch generation directly from fact_generation_power_hourly
CREATE OR REPLACE VIEW iceberg.gold.v_gen_hourly AS
SELECT 
    d.full_date AS date_value,
    t.hour AS hour_of_day,
    CASE 
        WHEN LOWER(s.source_name) LIKE '%than%' THEN 'Than'
        WHEN LOWER(s.source_name) LIKE '%khí%' THEN 'Khí'
        WHEN LOWER(s.source_name) LIKE '%thủy điện%' THEN 'Thủy điện'
        WHEN LOWER(s.source_name) LIKE '%mặt trời%' OR LOWER(s.source_name) LIKE '%đmt%' THEN 'Mặt trời'
        WHEN LOWER(s.source_name) LIKE '%gió%' THEN 'Gió'
        ELSE 'Khác'
    END AS source_group,
    ROUND(SUM(f.generation_power_mw), 1) AS generation_mw
FROM iceberg.gold.fact_generation_power_hourly f
JOIN iceberg.gold.dim_date d ON f.date_key = d.date_key
JOIN iceberg.gold.dim_time t ON f.time_key = t.time_key
JOIN iceberg.gold.dim_energy_source s ON f.energy_source_key = s.energy_source_key
GROUP BY 
    d.full_date,
    t.hour,
    CASE 
        WHEN LOWER(s.source_name) LIKE '%than%' THEN 'Than'
        WHEN LOWER(s.source_name) LIKE '%khí%' THEN 'Khí'
        WHEN LOWER(s.source_name) LIKE '%thủy điện%' THEN 'Thủy điện'
        WHEN LOWER(s.source_name) LIKE '%mặt trời%' OR LOWER(s.source_name) LIKE '%đmt%' THEN 'Mặt trời'
        WHEN LOWER(s.source_name) LIKE '%gió%' THEN 'Gió'
        ELSE 'Khác'
    END;

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
