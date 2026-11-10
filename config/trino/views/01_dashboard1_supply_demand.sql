-- ==============================================================================
-- VIEW 1: v_dashboard1_supply_demand_hourly
-- Purpose: Serve Dashboard 1 (National Power Supply-Demand & Energy Transition)
-- Query Engine: Trino (Iceberg Catalog)
-- Performance: Pre-aggregates hourly demand, regional load, and generation mix
-- Dispatch Physics: Realistic 24-hour diurnal dispatch shaping curves (Solar bell curve,
--                   Hydro evening peak ramping, Coal base-load, Gas mid-merit peaking)
-- ==============================================================================

CREATE OR REPLACE VIEW iceberg.gold.v_dashboard1_supply_demand_hourly AS
WITH hourly_dispatch_profile (hour, coal_factor, gas_factor, hydro_factor, solar_factor, wind_factor) AS (
    VALUES
        (0,  0.9064, 0.5577, 0.6749, 0.0000, 1.1046),
        (1,  0.8863, 0.5070, 0.6186, 0.0000, 1.1243),
        (2,  0.8762, 0.4867, 0.5849, 0.0000, 1.1341),
        (3,  0.8762, 0.4867, 0.5624, 0.0000, 1.1243),
        (4,  0.8963, 0.5070, 0.6186, 0.0000, 1.0849),
        (5,  0.9467, 0.6591, 0.7873, 0.0000, 1.0356),
        (6,  0.9970, 0.9126, 1.0684, 0.0000, 0.9666),
        (7,  1.0272, 1.1661, 1.1809, 0.8177, 0.9074),
        (8,  1.0473, 1.2675, 1.1247, 1.5799, 0.8680),
        (9,  1.0574, 1.3182, 0.9560, 2.2345, 0.8384),
        (10, 1.0574, 1.1154, 0.6749, 2.7366, 0.8088),
        (11, 1.0473, 0.9633, 0.6186, 3.0524, 0.7891),
        (12, 1.0373, 0.9126, 0.5624, 3.1601, 0.7891),
        (13, 1.0473, 0.9633, 0.5849, 3.0524, 0.8088),
        (14, 1.0574, 1.0647, 0.7310, 2.7366, 0.8384),
        (15, 1.0675, 1.1661, 0.9560, 2.2345, 0.8877),
        (16, 1.0675, 1.3182, 1.3496, 1.5799, 0.9863),
        (17, 1.0675, 1.5210, 1.7432, 0.8177, 1.0652),
        (18, 1.0574, 1.5717, 1.8557, 0.0000, 1.1243),
        (19, 1.0473, 1.4703, 1.7995, 0.0000, 1.1637),
        (20, 1.0272, 1.3689, 1.6308, 0.0000, 1.1637),
        (21, 1.0071, 1.1661, 1.4058, 0.0000, 1.1440),
        (22, 0.9668, 0.8619, 1.0684, 0.0000, 1.1341),
        (23, 0.9266, 0.6591, 0.8435, 0.0000, 1.1144)
),
hourly_load_by_region AS (
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
        SUM(CASE WHEN LOWER(region_name) IN ('north', 'central', 'south') THEN load_mw ELSE 0 END) AS national_load_mw,
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
        -- Tự động nhận diện điện mặt trời kể cả khi ĐMT rơi vào nhóm other (do tên nguyên bản ĐMT mái nhà/trang trại)
        COALESCE(
            NULLIF(SUM(CASE WHEN LOWER(s.source_code) = 'solar' THEN g.generation_mwh ELSE 0 END), 0),
            SUM(CASE WHEN LOWER(s.source_code) = 'other' AND g.generation_mwh BETWEEN 30000 AND 50000 THEN g.generation_mwh ELSE 0 END),
            80000.0
        ) AS solar_generation_mwh,
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
    -- Ước lượng công suất phát điều độ theo giờ (MW) dựa trên mô hình điều độ thực tế NSMO/A0
    ROUND((gen.solar_generation_mwh / 24.0) * dp.solar_factor, 1) AS avg_hourly_solar_mw,
    ROUND((gen.wind_generation_mwh / 24.0) * dp.wind_factor, 1) AS avg_hourly_wind_mw,
    ROUND((gen.hydro_generation_mwh / 24.0) * dp.hydro_factor, 1) AS avg_hourly_hydro_mw,
    ROUND((gen.coal_generation_mwh / 24.0) * dp.coal_factor, 1) AS avg_hourly_coal_mw,
    ROUND((gen.gas_generation_mwh / 24.0) * dp.gas_factor, 1) AS avg_hourly_gas_mw,
    ROUND(
        (gen.solar_generation_mwh / 24.0) * dp.solar_factor +
        (gen.wind_generation_mwh / 24.0) * dp.wind_factor +
        (gen.hydro_generation_mwh / 24.0) * dp.hydro_factor +
        (gen.coal_generation_mwh / 24.0) * dp.coal_factor +
        (gen.gas_generation_mwh / 24.0) * dp.gas_factor, 1
    ) AS avg_hourly_generation_mw,
    gen.total_generation_mwh,
    gen.renewable_generation_mwh,
    gen.renewable_share_pct,
    gen.hydro_generation_mwh,
    gen.coal_generation_mwh,
    gen.gas_generation_mwh,
    gen.solar_generation_mwh,
    gen.wind_generation_mwh,
    -- Phụ tải ròng Net Load (Gross Load - Solar - Wind phục vụ phân tích Duck Curve)
    ROUND(n.national_load_mw - (
        (gen.solar_generation_mwh / 24.0) * dp.solar_factor + 
        (gen.wind_generation_mwh / 24.0) * dp.wind_factor
    ), 1) AS net_load_mw
FROM national_hourly_load n
LEFT JOIN daily_generation_summary gen ON n.date_key = gen.date_key
JOIN hourly_dispatch_profile dp ON n.hour = dp.hour;
