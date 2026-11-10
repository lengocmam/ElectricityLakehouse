-- ==============================================================================
-- VIEW 6: ML & Deep Learning Forecast Views for Dashboard 06
-- Catalog: Iceberg Catalog (Gold Layer)
-- Target: Load Forecasting (MW) & Hydro Dispatch (MWh) via S4D, TimeXer, S-Mamba
-- ==============================================================================

-- 1. View Timeline Dự báo Phụ tải Next-Day (h=1) và 7-Day (h=7)
CREATE OR REPLACE VIEW iceberg.gold.v_dashboard6_load_timeline AS
SELECT 
    target_date,
    target_date_key,
    forecast_date,
    horizon_step,
    actual_value AS thuc_te_mw,
    s4d_pred AS s4d_du_bao_mw,
    ensemble_paper_pred AS ensemble_du_bao_mw,
    lightgbm_pred AS lightgbm_du_bao_mw,
    timexer_pred AS timexer_du_bao_mw,
    smamba_pred AS smamba_du_bao_mw,
    error_s4d AS sai_so_s4d_mw,
    error_ensemble AS sai_so_ensemble_mw,
    error_lightgbm AS sai_so_lightgbm_mw,
    is_holiday_break
FROM iceberg.gold.fact_forecast_daily
WHERE target_name = 'load_mean_mw';

-- 2. View Timeline Dự báo Thủy điện Next-Day (h=1) và 7-Day (h=7)
CREATE OR REPLACE VIEW iceberg.gold.v_dashboard6_hydro_timeline AS
SELECT 
    target_date,
    target_date_key,
    forecast_date,
    horizon_step,
    actual_value AS thuc_te_mwh,
    s4d_pred AS s4d_du_bao_mwh,
    ensemble_paper_pred AS ensemble_du_bao_mwh,
    lightgbm_pred AS lightgbm_du_bao_mwh,
    timexer_pred AS timexer_du_bao_mwh,
    smamba_pred AS smamba_du_bao_mwh,
    error_s4d AS sai_so_s4d_mwh,
    error_ensemble AS sai_so_ensemble_mwh,
    error_lightgbm AS sai_so_lightgbm_mwh,
    is_holiday_break
FROM iceberg.gold.fact_forecast_daily
WHERE target_name = 'gen_hydro';

-- 3. View So sánh Sai số MAE theo Bước Dự báo Horizon (h=1..7)
CREATE OR REPLACE VIEW iceberg.gold.v_dashboard6_horizon_comparison AS
SELECT 
    task_code,
    horizon_step,
    horizon_name,
    lightgbm_mae AS LightGBM_MAE,
    s4d_mae AS S4D_MAE,
    timexer_mae AS TimeXer_MAE,
    smamba_mae AS SMamba_MAE,
    ensemble_mae AS Ensemble_MAE,
    ROUND((lightgbm_mae - ensemble_mae) * 100.0 / NULLIF(lightgbm_mae, 0), 1) AS Ensembe_Giam_Sai_So_Pct
FROM iceberg.gold.fact_forecast_horizon_mae;

-- 4. View Bảng Xếp Hạng & Chỉ Số Khoa Học Các Thuật Toán
CREATE OR REPLACE VIEW iceberg.gold.v_dashboard6_model_benchmark AS
SELECT 
    task_code,
    task_name AS Bai_Toan_Du_Bao,
    unit AS Don_Vi,
    model_name AS Mo_Hinh,
    model_type AS Phan_Loai,
    mae AS MAE,
    rmse AS RMSE,
    wape_pct AS WAPE_Pct,
    accuracy_pct AS Do_Chinh_Xac_Pct,
    r2_score AS He_So_R2,
    dm_stat_vs_lgb AS DM_Stat_vs_LGB,
    dm_p_value_vs_lgb AS DM_p_value,
    CASE 
        WHEN dm_p_value_vs_lgb < 0.01 THEN '*** Khác biệt cực kỳ rõ (p < 0.01)'
        WHEN dm_p_value_vs_lgb < 0.05 THEN '** Khác biệt rõ rệt (p < 0.05)'
        WHEN dm_p_value_vs_lgb < 0.10 THEN '* Khác biệt có xu hướng (p < 0.10)'
        ELSE 'Tương đương thống kê'
    END AS Ket_Luan_Thong_Ke,
    rank_order AS Thu_Hang
FROM iceberg.gold.fact_forecast_benchmark;
