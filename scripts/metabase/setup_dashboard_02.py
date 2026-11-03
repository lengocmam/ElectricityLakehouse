# -*- coding: utf-8 -*-
"""Dashboard 02: Khí Hậu Cực Đoan, An Ninh Nguồn Nước & Thủy Điện.

Tác động kép của khí hậu cực đoan đến hệ thống điện:
- Nắng nóng kích hoạt nhu cầu làm mát (Độ nhạy phụ tải theo dải nhiệt độ máy lạnh).
- Mưa lũ điều tiết an toàn hồ đập bậc thang Sông Đà và tối ưu hóa xả phát điện tổ máy vs xả tràn.
- Giám sát an toàn, dung tích hữu ích và số cửa xả lũ mở của 30 đập thủy điện toàn quốc.
"""
import os
import sys

# Ensure local imports work regardless of execution directory
sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

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

DASHBOARD_NAME = "02. Khí Hậu Cực Đoan, An Ninh Nguồn Nước & Thủy Điện"
DASHBOARD_DESC = (
    "Tác động kép của biến đổi khí hậu: Nắng nóng bùng nổ phụ tải máy lạnh, "
    "điều tiết an toàn bậc thang Sông Đà và ma trận xả lũ 30 đập quốc gia."
)
CTE = get_target_date_cte("iceberg.gold.fact_hydro_status")


def deploy_dashboard_02(base_url=None, user=None, password=None):
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
        # ----------------------------------------------------------------------
        # NHỊP 1: 4 KPIs VẬN HÀNH THỜI GIAN THỰC (Rows 0-3, Height 4)
        # ----------------------------------------------------------------------
        {
            "name": "Nhiệt độ Cực Đại Toàn Quốc (°C)",
            "display": "scalar",
            "sql": f"""
                {CTE}
                SELECT ROUND(MAX(w.temperature_2m_max), 1) AS max_temp_c 
                FROM iceberg.gold.fact_weather_daily w
                JOIN target_date td ON w.date_key = td.date_key
            """,
            "viz": {}, "col": 0, "row": 0, "size_x": 6, "size_y": 4
        },
        {
            "name": "Hệ Số Co Giãn Nhiệt Độ (MW / °C)",
            "display": "scalar",
            "sql": f"""
                {CTE}
                SELECT ROUND((MAX(p.peak_load_mw) - MIN(p.peak_load_mw)) / NULLIF(MAX(w.temperature_2m_mean) - MIN(w.temperature_2m_mean), 0), 0) AS mw_per_c 
                FROM iceberg.gold.fact_weather_daily w 
                JOIN target_date td ON w.date_key <= td.date_key
                JOIN (
                    SELECT date_key, MAX(daily_peak_load_mw) AS peak_load_mw 
                    FROM iceberg.gold.fact_load_hourly 
                    GROUP BY date_key
                ) p ON w.date_key = p.date_key 
                WHERE w.temperature_2m_mean >= 30.0
            """,
            "viz": {}, "col": 6, "row": 0, "size_x": 6, "size_y": 4
        },
        {
            "name": "Tổng Lưu Lượng Nước Về 30 Hồ Thủy Điện (m³/s)",
            "display": "scalar",
            "sql": f"""
                {CTE}
                SELECT ROUND(SUM(h.reservoir_inflow_m3_per_s), 0) AS total_inflow_m3_s 
                FROM iceberg.gold.fact_hydro_status h
                JOIN target_date td ON h.date_key = td.date_key
            """,
            "viz": {}, "col": 12, "row": 0, "size_x": 6, "size_y": 4
        },
        {
            "name": "Cảnh Báo An Toàn & Số Cửa Xả Lũ Đang Mở",
            "display": "scalar",
            "sql": f"""
                {CTE}
                SELECT CASE 
                    WHEN SUM(h.deep_discharge_gate_count + h.surface_discharge_gate_count) > 0 
                    THEN '⚠️ ĐANG MỞ ' || CAST(SUM(h.deep_discharge_gate_count + h.surface_discharge_gate_count) AS VARCHAR) || ' CỬA XẢ LŨ' 
                    ELSE '✅ AN TOÀN - ĐÓNG CỬA XẢ' 
                END AS alert_status 
                FROM iceberg.gold.fact_hydro_status h
                JOIN target_date td ON h.date_key = td.date_key
            """,
            "viz": {}, "col": 18, "row": 0, "size_x": 6, "size_y": 4
        },

        # ----------------------------------------------------------------------
        # NHỊP 2: 2 HERO CHARTS - HỒI 1: KHÍ HẬU & BẬC THANG SÔNG ĐÀ (Rows 4-13)
        # ----------------------------------------------------------------------
        {
            "name": "Độ Nhạy Phụ Tải Theo Dải Nhiệt Độ Máy Lạnh (MW)",
            "display": "bar",
            "sql": f"""
                {CTE},
                daily_stats AS (
                    SELECT 
                        w.date_key,
                        AVG(w.temperature_2m_mean) AS nat_temp,
                        MAX(p.daily_peak_load_mw) AS peak_mw
                    FROM iceberg.gold.fact_weather_daily w
                    JOIN target_date td ON w.date_key <= td.date_key
                    JOIN iceberg.gold.fact_load_hourly p ON w.date_key = p.date_key
                    GROUP BY w.date_key
                )
                SELECT 
                    CASE 
                        WHEN nat_temp < 20.0 THEN '1. Dưới 20°C (Tiết trời lạnh)'
                        WHEN nat_temp < 24.0 THEN '2. 20 - 24°C (Mát mẻ)'
                        WHEN nat_temp < 27.0 THEN '3. 24 - 27°C (Dễ chịu)'
                        WHEN nat_temp < 30.0 THEN '4. 27 - 30°C (Bật máy lạnh)'
                        ELSE '5. Trên 30°C (Nắng nóng cao điểm)'
                    END AS Dai_Nhiet_Do,
                    ROUND(AVG(peak_mw), 0) AS Phu_Tai_Dinh_Binh_Quan_MW,
                    ROUND(MAX(peak_mw), 0) AS Dinh_Tai_Ky_Luc_MW
                FROM daily_stats
                GROUP BY 1
                ORDER BY 1
            """,
            "viz": {
                "graph.dimensions": ["Dai_Nhiet_Do"],
                "graph.metrics": ["Phu_Tai_Dinh_Binh_Quan_MW", "Dinh_Tai_Ky_Luc_MW"],
                "graph.y_axis.title_text": "Phụ tải Đỉnh Pmax (MW)",
                "graph.x_axis.title_text": "Dải Nhiệt Độ Khí Tượng (°C)",
                "column_settings": {
                    '["name","Dai_Nhiet_Do"]': {"column_title": "Dải Nhiệt Độ Khí Tượng"},
                    '["name","Phu_Tai_Dinh_Binh_Quan_MW"]': {"column_title": "Phụ Tải Đỉnh Bình Quân (MW)"},
                    '["name","Dinh_Tai_Ky_Luc_MW"]': {"column_title": "Đỉnh Tải Kỷ Lục (MW)"}
                }
            },
            "col": 0, "row": 4, "size_x": 12, "size_y": 10
        },
        {
            "name": "Bậc Thang Sông Đà: Giám Sát Mực Nước So Với Ngưỡng An Toàn",
            "display": "bar",
            "sql": f"""
                {CTE}
                SELECT 
                    split_part(p.plant_name, ' Đồng bộ', 1) AS Nha_May_Thuy_Dien,
                    ROUND(h.upstream_water_level_m, 1) AS Muc_Nuoc_Hien_Tai_m,
                    ROUND(h.normal_water_level_m, 1) AS Muc_Nuoc_Dang_BT_m,
                    ROUND(h.dead_water_level_m, 1) AS Muc_Nuoc_Chet_m
                FROM iceberg.gold.fact_hydro_status h
                JOIN target_date td ON h.date_key = td.date_key
                JOIN iceberg.gold.dim_plant p ON h.plant_key = p.plant_key
                WHERE p.river_basin = 'Sông Đà'
                ORDER BY h.normal_water_level_m DESC
            """,
            "viz": {
                "graph.dimensions": ["Nha_May_Thuy_Dien"],
                "graph.metrics": ["Muc_Nuoc_Hien_Tai_m", "Muc_Nuoc_Dang_BT_m", "Muc_Nuoc_Chet_m"],
                "graph.y_axis.title_text": "Mực nước hồ (mét)",
                "graph.x_axis.title_text": "Bậc Thang Thủy Điện Sông Đà",
                "column_settings": {
                    '["name","Nha_May_Thuy_Dien"]': {"column_title": "Đập Thủy Điện"},
                    '["name","Muc_Nuoc_Hien_Tai_m"]': {"column_title": "Mực Nước Hiện Tại (m)"},
                    '["name","Muc_Nuoc_Dang_BT_m"]': {"column_title": "Mực Nước Dâng Bình Thường (m)"},
                    '["name","Muc_Nuoc_Chet_m"]': {"column_title": "Mực Nước Chết (m)"}
                }
            },
            "col": 12, "row": 4, "size_x": 12, "size_y": 10
        },

        # ----------------------------------------------------------------------
        # NHỊP 3: HỒI 2: ĐIỀU TIẾT XẢ PHÁT ĐIỆN & PHÂN BỔ LƯU VỰC (Rows 14-23)
        # ----------------------------------------------------------------------
        {
            "name": "Điều Tiết Hồ Hòa Bình 30 Ngày: Xả Phát Điện Tổ Máy vs Xả Tràn Chống Lũ",
            "display": "line",
            "sql": f"""
                {CTE}
                SELECT 
                    d.full_date AS Ngay, 
                    ROUND(h.reservoir_inflow_m3_per_s, 1) AS Q_Ve_m3_s, 
                    ROUND(h.powerhouse_discharge_m3_per_s, 1) AS Q_Phat_Dien_m3_s,
                    ROUND(h.spillway_discharge_m3_per_s, 1) AS Q_Xa_Tran_m3_s
                FROM iceberg.gold.fact_hydro_status h 
                JOIN target_date td ON h.date_key <= td.date_key
                JOIN iceberg.gold.dim_plant p ON h.plant_key = p.plant_key 
                JOIN iceberg.gold.dim_date d ON h.date_key = d.date_key 
                WHERE p.plant_name LIKE '%Hòa Bình%' 
                ORDER BY h.date_key DESC 
                LIMIT 30
            """,
            "viz": {
                "graph.dimensions": ["Ngay"],
                "graph.metrics": ["Q_Ve_m3_s", "Q_Phat_Dien_m3_s", "Q_Xa_Tran_m3_s"],
                "graph.y_axis.title_text": "Lưu lượng (m³/s)",
                "graph.x_axis.title_text": "Ngày theo dõi",
                "column_settings": {
                    '["name","Ngay"]': {"column_title": "Ngày"},
                    '["name","Q_Ve_m3_s"]': {"column_title": "Lưu lượng về hồ Qvề (m³/s)"},
                    '["name","Q_Phat_Dien_m3_s"]': {"column_title": "Xả phát điện qua tổ máy (m³/s)"},
                    '["name","Q_Xa_Tran_m3_s"]': {"column_title": "Xả tràn chống lũ (m³/s)"}
                }
            },
            "col": 0, "row": 14, "size_x": 12, "size_y": 10
        },
        {
            "name": "Phân Bổ Lưu Lượng Nước Về (Qvề) & Xả Phát Điện Theo 8 Lưu Vực Sông",
            "display": "bar",
            "sql": f"""
                {CTE}
                SELECT 
                    p.river_basin AS Luu_Vuc_Song,
                    ROUND(SUM(h.reservoir_inflow_m3_per_s), 0) AS Tong_Nuoc_Ve_m3_s,
                    ROUND(SUM(h.powerhouse_discharge_m3_per_s), 0) AS Xa_Phat_Dien_m3_s,
                    ROUND(SUM(h.spillway_discharge_m3_per_s), 0) AS Xa_Tran_m3_s
                FROM iceberg.gold.fact_hydro_status h
                JOIN target_date td ON h.date_key = td.date_key
                JOIN iceberg.gold.dim_plant p ON h.plant_key = p.plant_key
                GROUP BY p.river_basin
                ORDER BY Tong_Nuoc_Ve_m3_s DESC
            """,
            "viz": {
                "graph.dimensions": ["Luu_Vuc_Song"],
                "graph.metrics": ["Tong_Nuoc_Ve_m3_s", "Xa_Phat_Dien_m3_s", "Xa_Tran_m3_s"],
                "graph.y_axis.title_text": "Lưu lượng dòng chảy (m³/s)",
                "graph.x_axis.title_text": "Lưu Vực Sông Quốc Gia",
                "column_settings": {
                    '["name","Luu_Vuc_Song"]': {"column_title": "Lưu Vực Sông"},
                    '["name","Tong_Nuoc_Ve_m3_s"]': {"column_title": "Tổng Nước Về Qvề (m³/s)"},
                    '["name","Xa_Phat_Dien_m3_s"]': {"column_title": "Xả Phát Điện Tổ Máy (m³/s)"},
                    '["name","Xa_Tran_m3_s"]': {"column_title": "Xả Tràn Chống Lũ (m³/s)"}
                }
            },
            "col": 12, "row": 14, "size_x": 12, "size_y": 10
        },

        # ----------------------------------------------------------------------
        # NHỊP 4: BẢNG MA TRẬN GIÁM SÁT CHI TIẾT 30 ĐẬP (Rows 24-31, Height 8)
        # ----------------------------------------------------------------------
        {
            "name": "Ma Trận Giám Sát An Toàn & Điều Tiết 30 Đập Thủy Điện Quốc Gia",
            "display": "table",
            "sql": f"""
                {CTE}
                SELECT 
                    split_part(p.plant_name, ' Đồng bộ', 1) AS "Nhà Máy Thủy Điện", 
                    p.river_basin AS "Lưu Vực Sông", 
                    ROUND(h.upstream_water_level_m, 2) AS "Mực Nước (m)", 
                    h.normal_water_level_m AS "Dâng BT (m)", 
                    h.dead_water_level_m AS "Mực Nước Chết (m)", 
                    ROUND((h.upstream_water_level_m - h.dead_water_level_m) * 100.0 / NULLIF(h.normal_water_level_m - h.dead_water_level_m, 0), 1) AS "Dung Tích Hữu Ích (%)", 
                    ROUND(h.reservoir_inflow_m3_per_s, 1) AS "Nước Về Qvề (m³/s)",
                    ROUND(h.powerhouse_discharge_m3_per_s, 1) AS "Xả Tổ Máy (m³/s)",
                    ROUND(h.spillway_discharge_m3_per_s, 1) AS "Xả Tràn (m³/s)",
                    (h.deep_discharge_gate_count + h.surface_discharge_gate_count) AS "Cửa Xả Mở", 
                    CASE 
                        WHEN (h.deep_discharge_gate_count + h.surface_discharge_gate_count) > 0 THEN '⚠️ Đang xả lũ' 
                        WHEN h.upstream_water_level_m >= (h.normal_water_level_m - 1.5) THEN '🔴 Đầy hồ' 
                        WHEN h.upstream_water_level_m <= (h.dead_water_level_m + 3.0) THEN '🟡 Nguy cơ cạn nước' 
                        ELSE '🟢 Bình thường' 
                    END AS "Trạng Thái Cảnh Báo" 
                FROM iceberg.gold.fact_hydro_status h 
                JOIN target_date td ON h.date_key = td.date_key
                JOIN iceberg.gold.dim_plant p ON h.plant_key = p.plant_key 
                ORDER BY (h.deep_discharge_gate_count + h.surface_discharge_gate_count) DESC, "Dung Tích Hữu Ích (%)" DESC
            """,
            "viz": {}, "col": 0, "row": 24, "size_x": 24, "size_y": 8
        }
    ]

    card_specs = []
    for c in cards_def:
        cid = upsert_card(url, token, col_id, db_id, c["name"], c["sql"], c["display"], c["viz"])
        card_specs.append({
            "card_id": cid, "col": c["col"], "row": c["row"],
            "size_x": c["size_x"], "size_y": c["size_y"], "viz": c["viz"]
        })

    set_dashboard_layout(url, token, dash_id, card_specs)
    print(f"[✓] Dashboard 02 ready at: {url}/dashboard/{dash_id}\n")
    return dash_id


if __name__ == "__main__":
    args = parse_common_args("Dashboard 02")
    deploy_dashboard_02(args.url, args.user, args.password)
