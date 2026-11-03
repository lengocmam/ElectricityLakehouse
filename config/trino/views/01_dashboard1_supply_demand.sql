-- ==============================================================================
-- VIEW 1: v_dashboard1_supply_demand_hourly
-- Purpose: Serve Dashboard 1 (National Power Supply-Demand & Energy Transition)
-- Query Engine: Trino (Iceberg Catalog)
-- Performance: Pre-aggregates hourly demand, regional load, and generation mix
-- ==============================================================================

CREATE OR REPLACE VIEW iceberg.gold.v_dashboard1_supply_demand_hourly AS
WITH hourly_load_by_region AS (
    SELECT 
        l.date_key,
        l.time_key,
        t.hour,
        d.full_date,
        d.day_name,
        d.day_of_week,
        d.month,
        d.season,
        d.is_weekend,
        r.region_key,
        r.region_name,
        l.load_mw,
        l.daily_avg_load_mw,
        l.daily_peak_load_mw,
        l.daily_min_load_mw,
        l.load_rank_in_day
    FROM iceberg.gold.fact_load_hourly l
    JOIN iceberg.gold.dim_date d ON l.date_key = d.date_key
    JOIN iceberg.gold.dim_time t ON l.time_key = t.time_key
    JOIN iceberg.gold.dim_region r ON l.region_key = r.region_key
),
national_hourly_load AS (
    SELECT 
        date_key,
        time_key,
        hour,
        full_date,
        month,
        season,
        is_weekend,
        SUM(load_mw) AS national_load_mw,
        SUM(CASE WHEN LOWER(region_name) = 'north' THEN load_mw ELSE 0 END) AS north_load_mw,
        SUM(CASE WHEN LOWER(region_name) = 'central' THEN load_mw ELSE 0 END) AS central_load_mw,
        SUM(CASE WHEN LOWER(region_name) = 'south' THEN load_mw ELSE 0 END) AS south_load_mw
    FROM hourly_load_by_region
    GROUP BY date_key, time_key, hour, full_date, month, season, is_weekend
),
daily_generation_summary AS (
    SELECT 
        g.date_key,
        SUM(g.generation_mwh) AS total_generation_mwh,
        SUM(CASE WHEN s.renewable_flag = true THEN g.generation_mwh ELSE 0 END) AS renewable_generation_mwh,
        SUM(CASE WHEN LOWER(s.source_code) = 'hydro' THEN g.generation_mwh ELSE 0 END) AS hydro_generation_mwh,
        SUM(CASE WHEN LOWER(s.source_code) = 'coal' THEN g.generation_mwh ELSE 0 END) AS coal_generation_mwh,
        SUM(CASE WHEN LOWER(s.source_code) = 'gas' THEN g.generation_mwh ELSE 0 END) AS gas_generation_mwh,
        SUM(CASE WHEN LOWER(s.source_code) = 'solar' THEN g.generation_mwh ELSE 0 END) AS solar_generation_mwh,
        SUM(CASE WHEN LOWER(s.source_code) = 'wind' THEN g.generation_mwh ELSE 0 END) AS wind_generation_mwh,
        ROUND(SUM(CASE WHEN s.renewable_flag = true THEN g.generation_mwh ELSE 0 END) * 100.0 / 
              NULLIF(SUM(g.generation_mwh), 0), 2) AS renewable_share_pct
    FROM iceberg.gold.fact_generation_daily g
    JOIN iceberg.gold.dim_energy_source s ON g.energy_source_key = s.energy_source_key
    GROUP BY g.date_key
)
SELECT 
    n.date_key,
    n.time_key,
    n.hour,
    n.full_date,
    n.month,
    n.season,
    n.is_weekend,
    n.national_load_mw,
    n.north_load_mw,
    n.central_load_mw,
    n.south_load_mw,
    -- Ước lượng công suất phát bình quân theo giờ từ tổng sản lượng ngày (MWh / 24h = MW)
    ROUND(gen.total_generation_mwh / 24.0, 1) AS avg_hourly_generation_mw,
    ROUND(gen.solar_generation_mwh / 24.0, 1) AS avg_hourly_solar_mw,
    ROUND(gen.wind_generation_mwh / 24.0, 1) AS avg_hourly_wind_mw,
    ROUND(gen.hydro_generation_mwh / 24.0, 1) AS avg_hourly_hydro_mw,
    ROUND(gen.coal_generation_mwh / 24.0, 1) AS avg_hourly_coal_mw,
    ROUND(gen.gas_generation_mwh / 24.0, 1) AS avg_hourly_gas_mw,
    gen.total_generation_mwh,
    gen.renewable_generation_mwh,
    gen.renewable_share_pct,
    gen.hydro_generation_mwh,
    gen.coal_generation_mwh,
    gen.gas_generation_mwh,
    gen.solar_generation_mwh,
    gen.wind_generation_mwh,
    -- Phụ tải ròng Net Load (Phụ tải trừ đi NLTT Gió & Mặt trời phục vụ phân tích Duck Curve)
    ROUND(n.national_load_mw - (
        -- Phân bổ điện mặt trời hình chuông từ 06h - 18h đỉnh điểm 12h
        CASE 
            WHEN n.hour BETWEEN 6 AND 18 
            THEN (gen.solar_generation_mwh * 1.8 / 12.0) * SIN(3.14159 * (n.hour - 6) / 12.0)
            ELSE 0 
        END + (gen.wind_generation_mwh / 24.0)
    ), 1) AS net_load_mw
FROM national_hourly_load n
LEFT JOIN daily_generation_summary gen ON n.date_key = gen.date_key;
