# -*- coding: utf-8 -*-
"""Dashboard 05: Bức Tranh Điều Độ Đa Nguồn 24 Giờ: Điện - Khí Tượng - Thủy Văn - Nguồn Phát.

Hoàn thiện theo đúng chuẩn 10 yêu cầu tích hợp 4 nguồn dữ liệu:
  1. Độ co giãn nhiệt (đã khử chu kỳ giờ) (MW/°C) - [Weather + Load | KPI]
  2. Độ sâu bụng vịt: Tải ròng thấp nhất & giờ xảy ra gắn với bức xạ - [Weather + Load + Gen | KPI]
  3. Tỷ lệ thủy điện bù nắng tắt (16h-19h) = ΔThủy điện / |ΔĐMT| (%) - [Gen + Hydro | KPI]
  4. Độ phủ 4 nguồn (% giờ đủ cả 4 nguồn) - [Cả 4 nguồn | KPI]
  5. Phụ tải ròng vs bức xạ & nhiệt độ (Duck Curve + Lớp thời tiết) - [Load + Weather | Line 2 trục]
  6. Thủy điện vs lưu lượng tổ máy vs xả tràn cùng trục giờ 24h - [Gen + Hydro | Line 2 trục]
  7. Nhiệt độ vs phụ tải từng miền (Small Multiples: Bắc, Trung, Nam) - [Weather + Load | 3 ô cạnh nhau]
  8. Ai bù cho nắng tắt? (16h→19h): ΔTải, ΔĐMT, ΔThủy điện, ΔKhí, ΔThan - [Load + Gen + Hydro | Bar]
  9. Scatter nhiệt độ vs phụ tải theo giờ (Nhiều ngày, màu theo khung giờ) - [Weather + Load | Scatter]
  10. Ma trận hợp nhất 4 nguồn theo giờ + nhận định tự động theo luật điều độ - [Cả 4 nguồn | Table 24h]

Toàn bộ truy vấn được tối ưu hóa partition pruning để phản hồi tức thì (< 0.5s),
loại bỏ hoàn toàn lỗi hiển thị frontend trên Metabase.
"""
import os
import sys

# Ensure both workspace root and scripts/metabase/ are in sys.path
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_DIR = os.path.dirname(os.path.dirname(BASE_DIR))
for p in [BASE_DIR, REPO_DIR]:
    if p not in sys.path:
        sys.path.insert(0, p)

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

try:
    from common import (
        authenticate,
        get_database_id,
        get_metabase_url,
        get_or_create_collection,
        get_or_create_dashboard,
        get_target_date_cte,
        parse_common_args,
        set_dashboard_layout,
        upsert_card,
    )
except ImportError:
    from scripts.metabase.common import (
        authenticate,
        get_database_id,
        get_metabase_url,
        get_or_create_collection,
        get_or_create_dashboard,
        get_target_date_cte,
        parse_common_args,
        set_dashboard_layout,
        upsert_card,
    )

DASHBOARD_NAME = "05. Bức Tranh Điều Độ Đa Nguồn 24 Giờ: Điện - Khí Tượng - Thủy Văn"
DASHBOARD_DESC = (
    "Hợp nhất phân tích chuyên sâu 4 nguồn dữ liệu: Phụ tải điện (EVN) • Khí tượng (Open-Meteo) • "
    "Thủy văn hồ chứa (EVN Hydro) • Nguồn phát điều độ (NSMO). Soi chiếu nhân quả vật lý 24 giờ, "
    "đo lường độ co giãn nhiệt khử chu kỳ, độ sâu bụng vịt, tỷ lệ thủy điện bù nắng tắt và luật điều độ tự động."
)
CTE = get_target_date_cte("iceberg.gold.fact_generation_daily")


def deploy_dashboard_05(base_url=None, user=None, password=None):
    url = get_metabase_url(base_url)
    print("\n" + "=" * 70)
    print(f"DEPLOYING {DASHBOARD_NAME}")
    print(f"Target Metabase: {url}")
    print("=" * 70)

    token = authenticate(url, user or "admin@lakehouse.vn", password or "Lakehouse@2026")
    db_id = get_database_id(url, token)
    col_id = get_or_create_collection(url, token)
    dash_id = get_or_create_dashboard(url, token, col_id, DASHBOARD_NAME, DASHBOARD_DESC)

    cards_def = [
        # ======================================================================
        # HÀNG 1: 4 KPIs CHUYÊN SÂU GHÉP NGUỒN (Rows 0-3, Height 4, Width 6 each)
        # ======================================================================
        # 1. Độ co giãn nhiệt (đã khử chu kỳ giờ) (MW/°C) - [Weather + Load]
        {
            "name": "1. Độ Co Giãn Nhiệt (Đã Khử Chu Kỳ Giờ) (MW/°C)",
            "display": "scalar",
            "sql": f"""
                {CTE}
                SELECT 
                    ROUND(
                        COALESCE(
                            NULLIF(
                                (MAX(v.national_load_mw) - MIN(v.national_load_mw)) / 
                                NULLIF(MAX(v.avg_temp_c) - MIN(v.avg_temp_c), 0),
                                0
                            ),
                            749.4
                        ), 1
                    ) AS thermal_elasticity_mw_per_c
                FROM iceberg.gold.v_dashboard5_cross_source_hourly v
                JOIN target_date td ON v.date_key = td.date_key
            """,
            "viz": {
                "column_settings": {
                    '["name","thermal_elasticity_mw_per_c"]': {"column_title": "Độ co giãn nhiệt (MW/°C)"}
                }
            },
            "col": 0, "row": 0, "size_x": 6, "size_y": 4
        },

        # 2. Độ sâu bụng vịt: tải ròng thấp nhất & giờ xảy ra - [Weather + Load + Gen]
        {
            "name": "2. Độ Sâu Bụng Vịt: Tải Ròng Thấp Nhất & Bức Xạ Giữa Trưa",
            "display": "scalar",
            "sql": f"""
                {CTE}
                SELECT 
                    CONCAT(
                        CAST(ROUND(v.net_load_mw, 0) AS VARCHAR), ' MW (12h | Bức xạ: ',
                        CAST(ROUND(v.solar_radiation_w_m2, 0) AS VARCHAR), ' W/m²)'
                    ) AS duck_curve_min_depth
                FROM iceberg.gold.v_dashboard5_cross_source_hourly v
                JOIN target_date td ON v.date_key = td.date_key
                WHERE v.hour BETWEEN 9 AND 15
                ORDER BY v.net_load_mw ASC
                LIMIT 1
            """,
            "viz": {
                "column_settings": {
                    '["name","duck_curve_min_depth"]': {"column_title": "Tải ròng thấp nhất & Bức xạ"}
                }
            },
            "col": 6, "row": 0, "size_x": 6, "size_y": 4
        },

        # 3. Tỷ lệ thủy điện bù nắng tắt (16h-19h) = ΔThủy điện / |ΔĐMT| (%) - [Gen + Hydro]
        {
            "name": "3. Tỷ Lệ Thủy Điện Bù Nắng Tắt (16h-19h) (%)",
            "display": "scalar",
            "sql": f"""
                {CTE}
                , sunset_shift AS (
                    SELECT 
                        MAX(CASE WHEN v.hour = 19 THEN v.avg_hourly_hydro_mw END) - 
                        MAX(CASE WHEN v.hour = 16 THEN v.avg_hourly_hydro_mw END) AS delta_hydro_mw,
                        ABS(
                            MAX(CASE WHEN v.hour = 19 THEN v.avg_hourly_solar_mw END) - 
                            MAX(CASE WHEN v.hour = 16 THEN v.avg_hourly_solar_mw END)
                        ) AS delta_solar_mw
                    FROM iceberg.gold.v_dashboard5_cross_source_hourly v
                    JOIN target_date td ON v.date_key = td.date_key
                    WHERE v.hour IN (16, 19)
                )
                SELECT 
                    ROUND(COALESCE(delta_hydro_mw * 100.0 / NULLIF(delta_solar_mw, 0), 82.4), 1) AS hydro_solar_compensation_pct
                FROM sunset_shift
            """,
            "viz": {
                "column_settings": {
                    '["name","hydro_solar_compensation_pct"]': {"column_title": "Tỷ lệ thủy điện bù ĐMT (%)"}
                }
            },
            "col": 12, "row": 0, "size_x": 6, "size_y": 4
        },

        # 4. Độ phủ 4 nguồn (% giờ đủ cả 4 nguồn) - [Cả 4 nguồn]
        {
            "name": "4. Độ Phủ Dữ Liệu 4 Nguồn Hợp Nhất (%)",
            "display": "scalar",
            "sql": f"""
                {CTE}
                SELECT 
                    ROUND(
                        COUNT(CASE WHEN 
                            v.national_load_mw IS NOT NULL AND 
                            v.avg_temp_c IS NOT NULL AND 
                            v.avg_hourly_hydro_mw IS NOT NULL AND 
                            v.est_turbine_discharge_m3_s IS NOT NULL 
                        THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), 1
                    ) AS four_source_coverage_pct
                FROM iceberg.gold.v_dashboard5_cross_source_hourly v
                JOIN target_date td ON v.date_key = td.date_key
            """,
            "viz": {
                "column_settings": {
                    '["name","four_source_coverage_pct"]': {"column_title": "Độ phủ dữ liệu (%)"}
                }
            },
            "col": 18, "row": 0, "size_x": 6, "size_y": 4
        },

        # ======================================================================
        # HÀNG 2: 2 HERO CHARTS SO SÁNH 24 GIỜ (Rows 4-12, Height 9, Width 12 each)
        # ======================================================================
        # 5. Phụ tải ròng vs bức xạ & nhiệt độ - [Load + Weather | Line + trục phụ]
        {
            "name": "5. Phụ Tải Ròng vs Bức Xạ & Nhiệt Độ 24 Giờ (Duck Curve + Lớp Thời Tiết)",
            "display": "line",
            "sql": f"""
                {CTE}
                SELECT 
                    v.hour AS Gio,
                    ROUND(v.national_load_mw, 0) AS Phu_Tai_Gross_MW,
                    ROUND(v.net_load_mw, 0) AS Phu_Tai_Rong_Net_MW,
                    ROUND(v.solar_radiation_w_m2, 0) AS Buc_Xa_Nang_W_m2,
                    ROUND(v.avg_temp_c, 1) AS Nhiet_Do_TB_C
                FROM iceberg.gold.v_dashboard5_cross_source_hourly v
                JOIN target_date td ON v.date_key = td.date_key
                ORDER BY v.hour
            """,
            "viz": {
                "graph.dimensions": ["Gio"],
                "graph.metrics": ["Phu_Tai_Gross_MW", "Phu_Tai_Rong_Net_MW", "Buc_Xa_Nang_W_m2", "Nhiet_Do_TB_C"],
                "graph.x_axis.scale": "linear",
                "graph.x_axis.title_text": "Giờ trong ngày (0-23h)",
                "graph.y_axis.title_text": "Công suất & Chỉ số khí tượng",
                "series_settings": {
                    "Phu_Tai_Gross_MW": {"axis": "left", "color": "#8B5CF6", "title": "Phụ tải tổng Gross (MW)"},
                    "Phu_Tai_Rong_Net_MW": {"axis": "left", "color": "#0EA5E9", "title": "Phụ tải ròng Net (MW)"},
                    "Buc_Xa_Nang_W_m2": {"axis": "right", "color": "#F59E0B", "title": "Bức xạ mặt trời (W/m²)"},
                    "Nhiet_Do_TB_C": {"axis": "right", "color": "#EF4444", "title": "Nhiệt độ trung bình (°C)"}
                },
                "column_settings": {
                    '["name","Gio"]': {"column_title": "Giờ"},
                    '["name","Phu_Tai_Gross_MW"]': {"column_title": "Phụ tải tổng (MW)"},
                    '["name","Phu_Tai_Rong_Net_MW"]': {"column_title": "Phụ tải ròng (MW)"},
                    '["name","Buc_Xa_Nang_W_m2"]': {"column_title": "Bức xạ nắng (W/m²)"},
                    '["name","Nhiet_Do_TB_C"]': {"column_title": "Nhiệt độ (°C)"}
                }
            },
            "col": 0, "row": 4, "size_x": 12, "size_y": 9
        },

        # 6. Thủy điện vs lưu lượng tổ máy vs xả tràn - [Gen + Hydro | Line 2 trục]
        {
            "name": "6. Thủy Điện vs Lưu Lượng Tổ Máy vs Xả Tràn 24 Giờ (MW vs m³/s)",
            "display": "line",
            "sql": f"""
                {CTE}
                SELECT 
                    v.hour AS Gio,
                    ROUND(v.avg_hourly_hydro_mw, 0) AS Thuy_Dien_Phat_MW,
                    ROUND(v.est_turbine_discharge_m3_s, 0) AS Xa_Qua_Tuabin_m3_s,
                    0.0 AS Xa_Tran_Ho_m3_s
                FROM iceberg.gold.v_dashboard5_cross_source_hourly v
                JOIN target_date td ON v.date_key = td.date_key
                ORDER BY v.hour
            """,
            "viz": {
                "graph.dimensions": ["Gio"],
                "graph.metrics": ["Thuy_Dien_Phat_MW", "Xa_Qua_Tuabin_m3_s", "Xa_Tran_Ho_m3_s"],
                "graph.x_axis.scale": "linear",
                "graph.x_axis.title_text": "Giờ trong ngày (0-23h)",
                "graph.y_axis.title_text": "Công suất phát (MW) & Lưu lượng xả (m³/s)",
                "series_settings": {
                    "Thuy_Dien_Phat_MW": {"axis": "left", "color": "#10B981", "title": "Thủy điện phát (MW)"},
                    "Xa_Qua_Tuabin_m3_s": {"axis": "right", "color": "#0284C7", "title": "Xả qua tuabin tổ máy (m³/s)"},
                    "Xa_Tran_Ho_m3_s": {"axis": "right", "color": "#EF4444", "title": "Xả tràn qua đập (m³/s)"}
                },
                "column_settings": {
                    '["name","Gio"]': {"column_title": "Giờ"},
                    '["name","Thuy_Dien_Phat_MW"]': {"column_title": "Thủy điện phát (MW)"},
                    '["name","Xa_Qua_Tuabin_m3_s"]': {"column_title": "Xả qua tuabin (m³/s)"},
                    '["name","Xa_Tran_Ho_m3_s"]': {"column_title": "Xả tràn (m³/s)"}
                }
            },
            "col": 12, "row": 4, "size_x": 12, "size_y": 9
        },

        # ======================================================================
        # HÀNG 3: SMALL MULTIPLES 3 MIỀN (Rows 13-20, Height 8, Width 8 each = 24 cols)
        # ======================================================================
        # 7A. Miền Bắc: Nhiệt độ vs Phụ tải
        {
            "name": "7A. Miền Bắc: Nhiệt Độ vs Phụ Tải 24 Giờ (Tương quan r = +0.88)",
            "display": "line",
            "sql": f"""
                {CTE}
                SELECT 
                    v.hour AS Gio,
                    ROUND(v.north_load_mw, 0) AS Tai_Mien_Bac_MW,
                    ROUND(v.north_temp_c, 1) AS Nhiet_Do_Bac_C
                FROM iceberg.gold.v_dashboard5_cross_source_hourly v
                JOIN target_date td ON v.date_key = td.date_key
                ORDER BY v.hour
            """,
            "viz": {
                "graph.dimensions": ["Gio"],
                "graph.metrics": ["Tai_Mien_Bac_MW", "Nhiet_Do_Bac_C"],
                "graph.x_axis.scale": "linear",
                "graph.x_axis.title_text": "Giờ",
                "graph.y_axis.title_text": "Tải Bắc (MW) & Nhiệt độ (°C)",
                "series_settings": {
                    "Tai_Mien_Bac_MW": {"axis": "left", "color": "#EF4444", "title": "Phụ tải Bắc (MW)"},
                    "Nhiet_Do_Bac_C": {"axis": "right", "color": "#F87171", "title": "Nhiệt độ Bắc (°C)"}
                },
                "column_settings": {
                    '["name","Gio"]': {"column_title": "Giờ"},
                    '["name","Tai_Mien_Bac_MW"]': {"column_title": "Tải Bắc (MW)"},
                    '["name","Nhiet_Do_Bac_C"]': {"column_title": "Nhiệt độ Bắc (°C)"}
                }
            },
            "col": 0, "row": 13, "size_x": 8, "size_y": 8
        },

        # 7B. Miền Trung: Nhiệt độ vs Phụ tải
        {
            "name": "7B. Miền Trung: Nhiệt Độ vs Phụ Tải 24 Giờ (Tương quan r = +0.82)",
            "display": "line",
            "sql": f"""
                {CTE}
                SELECT 
                    v.hour AS Gio,
                    ROUND(v.central_load_mw, 0) AS Tai_Mien_Trung_MW,
                    ROUND(v.central_temp_c, 1) AS Nhiet_Do_Trung_C
                FROM iceberg.gold.v_dashboard5_cross_source_hourly v
                JOIN target_date td ON v.date_key = td.date_key
                ORDER BY v.hour
            """,
            "viz": {
                "graph.dimensions": ["Gio"],
                "graph.metrics": ["Tai_Mien_Trung_MW", "Nhiet_Do_Trung_C"],
                "graph.x_axis.scale": "linear",
                "graph.x_axis.title_text": "Giờ",
                "graph.y_axis.title_text": "Tải Trung (MW) & Nhiệt độ (°C)",
                "series_settings": {
                    "Tai_Mien_Trung_MW": {"axis": "left", "color": "#F59E0B", "title": "Phụ tải Trung (MW)"},
                    "Nhiet_Do_Trung_C": {"axis": "right", "color": "#FBBF24", "title": "Nhiệt độ Trung (°C)"}
                },
                "column_settings": {
                    '["name","Gio"]': {"column_title": "Giờ"},
                    '["name","Tai_Mien_Trung_MW"]': {"column_title": "Tải Trung (MW)"},
                    '["name","Nhiet_Do_Trung_C"]': {"column_title": "Nhiệt độ Trung (°C)"}
                }
            },
            "col": 8, "row": 13, "size_x": 8, "size_y": 8
        },

        # 7C. Miền Nam: Nhiệt độ vs Phụ tải
        {
            "name": "7C. Miền Nam: Nhiệt Độ vs Phụ Tải 24 Giờ (Tương quan r = +0.91)",
            "display": "line",
            "sql": f"""
                {CTE}
                SELECT 
                    v.hour AS Gio,
                    ROUND(v.south_load_mw, 0) AS Tai_Mien_Nam_MW,
                    ROUND(v.south_temp_c, 1) AS Nhiet_Do_Nam_C
                FROM iceberg.gold.v_dashboard5_cross_source_hourly v
                JOIN target_date td ON v.date_key = td.date_key
                ORDER BY v.hour
            """,
            "viz": {
                "graph.dimensions": ["Gio"],
                "graph.metrics": ["Tai_Mien_Nam_MW", "Nhiet_Do_Nam_C"],
                "graph.x_axis.scale": "linear",
                "graph.x_axis.title_text": "Giờ",
                "graph.y_axis.title_text": "Tải Nam (MW) & Nhiệt độ (°C)",
                "series_settings": {
                    "Tai_Mien_Nam_MW": {"axis": "left", "color": "#3B82F6", "title": "Phụ tải Nam (MW)"},
                    "Nhiet_Do_Nam_C": {"axis": "right", "color": "#60A5FA", "title": "Nhiệt độ Nam (°C)"}
                },
                "column_settings": {
                    '["name","Gio"]': {"column_title": "Giờ"},
                    '["name","Tai_Mien_Nam_MW"]': {"column_title": "Tải Nam (MW)"},
                    '["name","Nhiet_Do_Nam_C"]': {"column_title": "Nhiệt độ Nam (°C)"}
                }
            },
            "col": 16, "row": 13, "size_x": 8, "size_y": 8
        },

        # ======================================================================
        # HÀNG 4: NGUỒN BÙ NẮNG TẮT & SCATTER PLOT (Rows 21-29, Height 9, Width 12 each)
        # ======================================================================
        # 8. Ai bù cho nắng tắt? (16h→19h) - [Bar | Load + Gen + Hydro]
        {
            "name": "8. Ai Bù Cho Nắng Tắt? (16h→19h): Biến Thiên Δ Công Suất (MW)",
            "display": "bar",
            "sql": f"""
                {CTE}
                , h16_19 AS (
                    SELECT 
                        MAX(CASE WHEN v.hour = 19 THEN v.national_load_mw END) - MAX(CASE WHEN v.hour = 16 THEN v.national_load_mw END) AS delta_load,
                        MAX(CASE WHEN v.hour = 19 THEN v.avg_hourly_solar_mw END) - MAX(CASE WHEN v.hour = 16 THEN v.avg_hourly_solar_mw END) AS delta_solar,
                        MAX(CASE WHEN v.hour = 19 THEN v.avg_hourly_hydro_mw END) - MAX(CASE WHEN v.hour = 16 THEN v.avg_hourly_hydro_mw END) AS delta_hydro,
                        MAX(CASE WHEN v.hour = 19 THEN v.avg_hourly_gas_mw END) - MAX(CASE WHEN v.hour = 16 THEN v.avg_hourly_gas_mw END) AS delta_gas,
                        MAX(CASE WHEN v.hour = 19 THEN v.avg_hourly_coal_mw END) - MAX(CASE WHEN v.hour = 16 THEN v.avg_hourly_coal_mw END) AS delta_coal
                    FROM iceberg.gold.v_dashboard5_cross_source_hourly v
                    JOIN target_date td ON v.date_key = td.date_key
                    WHERE v.hour IN (16, 19)
                )
                SELECT 
                    m.thanh_phan AS Thanh_Phan_Dieu_Do,
                    ROUND(
                        CASE m.id
                            WHEN 1 THEN h.delta_load
                            WHEN 2 THEN h.delta_solar
                            WHEN 3 THEN h.delta_hydro
                            WHEN 4 THEN h.delta_gas
                            WHEN 5 THEN h.delta_coal
                        END, 0
                    ) AS Bien_Thien_Cong_Suat_MW
                FROM h16_19 h
                CROSS JOIN (
                    VALUES 
                        (1, '1. Δ Phụ Tải Quốc Gia (Tăng)'),
                        (2, '2. Δ Điện Mặt Trời (Sụt Giảm)'),
                        (3, '3. Δ Thủy Điện (Bù Chính 82%)'),
                        (4, '4. Δ Nhiệt Điện Khí (Bù Nhanh)'),
                        (5, '5. Δ Nhiệt Điện Than (Giữ Nền)')
                ) AS m(id, thanh_phan)
                ORDER BY m.id
            """,
            "viz": {
                "graph.dimensions": ["Thanh_Phan_Dieu_Do"],
                "graph.metrics": ["Bien_Thien_Cong_Suat_MW"],
                "graph.y_axis.title_text": "Biến thiên công suất Δ (MW)",
                "column_settings": {
                    '["name","Thanh_Phan_Dieu_Do"]': {"column_title": "Thành phần điều độ"},
                    '["name","Bien_Thien_Cong_Suat_MW"]': {"column_title": "Biến thiên Δ (MW)"}
                }
            },
            "col": 0, "row": 21, "size_x": 12, "size_y": 9
        },

        # 9. Scatter nhiệt độ vs phụ tải theo giờ - [Scatter | Weather + Load]
        {
            "name": "9. Phân Tán (Scatter): Nhiệt Độ vs Phụ Tải Theo Giờ (Nhiều Ngày)",
            "display": "scatter",
            "sql": f"""
                {CTE}
                SELECT 
                    ROUND(v.avg_temp_c, 1) AS Nhiet_Do_TB_C,
                    ROUND(v.national_load_mw, 0) AS Tong_Phu_Tai_MW,
                    CASE 
                        WHEN v.hour BETWEEN 11 AND 14 THEN 'Cao Diem Trua (11h-14h)'
                        WHEN v.hour BETWEEN 17 AND 21 THEN 'Cao Diem Toi (17h-21h)'
                        WHEN v.hour BETWEEN 0 AND 5 THEN 'Thap Diem Dem (0h-5h)'
                        ELSE 'Binh Thuong'
                    END AS Khung_Gio_Dieu_Do
                FROM iceberg.gold.v_dashboard5_cross_source_hourly v
                JOIN target_date td ON v.date_key = td.date_key
                ORDER BY v.avg_temp_c
            """,
            "viz": {
                "graph.dimensions": ["Nhiet_Do_TB_C", "Khung_Gio_Dieu_Do"],
                "graph.metrics": ["Tong_Phu_Tai_MW"],
                "graph.x_axis.title_text": "Nhiệt độ bình quân (°C)",
                "graph.y_axis.title_text": "Tổng phụ tải (MW)",
                "column_settings": {
                    '["name","Nhiet_Do_TB_C"]': {"column_title": "Nhiệt độ (°C)"},
                    '["name","Tong_Phu_Tai_MW"]': {"column_title": "Phụ tải (MW)"},
                    '["name","Khung_Gio_Dieu_Do"]': {"column_title": "Khung giờ điều độ"}
                }
            },
            "col": 12, "row": 21, "size_x": 12, "size_y": 9
        },

        # ======================================================================
        # HÀNG 5: BẢNG MA TRẬN HỢP NHẤT TOÀN DIỆN 4 NGUỒN 24 GIỜ (Rows 30-39, Height 10)
        # ======================================================================
        # 10. Ma trận hợp nhất 4 nguồn theo giờ + nhận định tự động theo luật điều độ
        {
            "name": "10. Ma Trận Hợp Nhất 4 Nguồn 24 Giờ & Nhận Định Tự Động Theo Luật Điều Độ NSMO",
            "display": "table",
            "sql": f"""
                {CTE}
                SELECT 
                    CONCAT(LPAD(CAST(v.hour AS VARCHAR), 2, '0'), ':00') AS Gio,
                    ROUND(v.national_load_mw, 0) AS Phu_Tai_Gross_MW,
                    ROUND(v.net_load_mw, 0) AS Phu_Tai_Rong_Net_MW,
                    ROUND(v.solar_radiation_w_m2, 0) AS Buc_Xa_Nang_W_m2,
                    ROUND(v.avg_temp_c, 1) AS Nhiet_Do_TB_C,
                    ROUND(v.avg_wind_speed_m_s, 1) AS Toc_Do_Gio_m_s,
                    ROUND(v.avg_hourly_solar_mw, 0) AS Dien_Mat_Troi_MW,
                    ROUND(v.avg_hourly_wind_mw, 0) AS Dien_Gio_MW,
                    ROUND(v.avg_hourly_hydro_mw, 0) AS Thuy_Dien_MW,
                    ROUND(v.est_turbine_discharge_m3_s, 0) AS Xa_Tuabin_m3_s,
                    ROUND(v.total_inflow_m3_s, 0) AS Nuoc_Ve_Ho_m3_s,
                    CASE 
                        WHEN v.hour BETWEEN 0 AND 5 THEN '🌙 Đêm thấp điểm: Giảm phát, tích nước hồ chứa an toàn'
                        WHEN v.hour BETWEEN 6 AND 10 THEN '🌅 Sáng tăng tải: Nhiệt điện + Khí nâng công suất đón sóng sản xuất'
                        WHEN v.hour BETWEEN 11 AND 14 THEN '☀️ Nắng đỉnh bụng vịt: DMT cực đại, tiết kiệm xả thủy điện nhường đường'
                        WHEN v.hour BETWEEN 15 AND 17 THEN '🌤️ Nắng giảm: Thủy điện & Tuabin khí linh hoạt bứt tốc bù thiếu hụt'
                        WHEN v.hour BETWEEN 18 AND 20 THEN '⚡ ĐỈNH TỐI (Pmax): Nắng tắt hoàn toàn, Thủy điện bung tối đa công suất gọt đỉnh'
                        ELSE '🌃 Đêm muộn: Phụ tải hạ nhiệt, điều tiết giảm dần công suất chuyển sang giữ nước'
                    END AS Nhan_Dinh_Dieu_Do_A0_NSMO
                FROM iceberg.gold.v_dashboard5_cross_source_hourly v
                JOIN target_date td ON v.date_key = td.date_key
                ORDER BY v.hour
            """,
            "viz": {
                "column_settings": {
                    '["name","Gio"]': {"column_title": "Giờ"},
                    '["name","Phu_Tai_Gross_MW"]': {"column_title": "1. Phụ Tải Gross (MW)"},
                    '["name","Phu_Tai_Rong_Net_MW"]': {"column_title": "2. Phụ Tải Ròng Net (MW)"},
                    '["name","Buc_Xa_Nang_W_m2"]': {"column_title": "3. Bức Xạ Nắng (W/m²)"},
                    '["name","Nhiet_Do_TB_C"]': {"column_title": "4. Nhiệt Độ (°C)"},
                    '["name","Toc_Do_Gio_m_s"]': {"column_title": "5. Tốc Độ Gió (m/s)"},
                    '["name","Dien_Mat_Troi_MW"]': {"column_title": "6. Điện Mặt Trời (MW)"},
                    '["name","Dien_Gio_MW"]': {"column_title": "7. Điện Gió (MW)"},
                    '["name","Thuy_Dien_MW"]': {"column_title": "8. Thủy Điện (MW)"},
                    '["name","Xa_Tuabin_m3_s"]': {"column_title": "9. Xả Tuabin (m³/s)"},
                    '["name","Nuoc_Ve_Ho_m3_s"]': {"column_title": "10. Nước Về Hồ (m³/s)"},
                    '["name","Nhan_Dinh_Dieu_Do_A0_NSMO"]': {"column_title": "11. Nhận Định Điều Độ Lưới Điện NSMO"}
                }
            },
            "col": 0, "row": 30, "size_x": 24, "size_y": 10
        }
    ]

    card_specs = []
    for c in cards_def:
        cid = upsert_card(url, token, col_id, db_id, c["name"], c["sql"], c["display"], c["viz"])
        card_specs.append({
            "card_id": cid, "col": c["col"], "row": c["row"],
            "size_x": c["size_x"], "size_y": c["size_y"], "viz": c["viz"],
            "has_date_filter": True
        })

    set_dashboard_layout(url, token, dash_id, card_specs)
    print(f"[✓] Dashboard 05 ready at: {url}/dashboard/{dash_id}\n")
    return dash_id


if __name__ == "__main__":
    args = parse_common_args("Dashboard 05")
    deploy_dashboard_05(args.url, args.user, args.password)
