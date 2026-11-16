# -*- coding: utf-8 -*-
"""Dashboard 02: Thời Tiết Cực Đoan, An Ninh Nước & Thủy Điện (Phiên Bản Trực Quan Hóa Tối Ưu - Rich BI Visuals).

Thiết kế trực quan hóa hiện đại theo phản hồi của người dùng:
1. LOẠI BỎ SỰ LẠM DỤNG DẠNG BẢNG (No Table-Heavy Dashboard):
   - Chuyển toàn bộ 4 thẻ chỉ số hàng đầu thành BIỂU ĐỒ TRỰC QUAN (Bar, Row Bar, Donut).
   - Chỉ giữ duy nhất 1 Bảng Chỉ Huy Hành Động (D2-7) ở phần điều độ để xem chi tiết hồ và gợi ý tác chiến.
   - Chuyển Ma trận hồ D2-8 thành Biểu đồ Horizontal Bar Top 10 hồ cận đỉnh.
2. TỔNG CỘNG: 8 BIỂU ĐỒ ĐA DẠNG (Bar, Row, Donut, Line, Combo) + 1 BẢNG CHỈ HUY HÀNH ĐỘNG.
3. TỐC ĐỘ PHẢN HỒI < 3S: Dùng TARGET_CTE tĩnh, không full-table scan, không bị timeout.
"""

import sys
import os

if hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

# Add path for local common import
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from common import (
    authenticate,
    get_database_id,
    get_metabase_url,
    get_or_create_collection,
    get_or_create_dashboard,
    set_dashboard_layout,
    upsert_card,
    COMMON_TAG_DEF,
)

DASHBOARD_NAME = "02. Thời Tiết Cực Đoan, An Ninh Nước & Thủy Điện"
DASHBOARD_DESC = (
    "Trung tâm Giám sát Khí tượng Thủy văn & Cảnh báo An ninh Nước (Lũ & Hạn): Trực quan hóa sóng nhiệt 3 miền, "
    "phân cấp cảnh báo 4 mức trên 8 lưu vực sông, giám sát hồ cận đỉnh và hồ cạn kiệt, chỉ huy điều độ an toàn hệ thống."
)

TAG_DEF_DATE = COMMON_TAG_DEF

# Unified Fast Target CTE: Chống nghẽn full-table scan, tốc độ phản hồi < 1s
TARGET_CTE = """
WITH user_input AS (
    SELECT COALESCE(
        NULL [[ , CAST({{selected_date}} AS DATE) ]],
        DATE '2026-11-14'
    ) AS d
),
target AS (
    SELECT 
        d AS selected_date,
        (YEAR(d) * 10000 + MONTH(d) * 100 + DAY(d)) AS date_key
    FROM user_input
)
""".strip()

TARGET_HYDRO_CTE = TARGET_CTE
TARGET_WEATHER_CTE = TARGET_CTE


def deploy_dashboard_02(base_url=None, user=None, password=None):
    url = get_metabase_url(base_url)
    print("\n" + "=" * 70)
    print(f"DEPLOYING {DASHBOARD_NAME} (TRỰC QUAN HÓA CAO CẤP - RICH CHARTS)")
    print(f"Target Metabase: {url}")
    print("=" * 70)

    token = authenticate(url, user or "admin@lakehouse.vn", password or "Lakehouse@2026")
    db_id = get_database_id(url, token)
    col_id = get_or_create_collection(url, token)
    dash_id = get_or_create_dashboard(url, token, col_id, DASHBOARD_NAME, DASHBOARD_DESC)

    cards_def = [
        # ======================================================================
        # NHỊP 1: 4 BIỂU ĐỒ TỔNG QUAN TÁC CHIẾN (Rows 0-3, Height 4, Width 6)
        # Thay thế bảng bằng biểu đồ Cột, Thanh Ngang và Donut trực quan 100%
        # ======================================================================
        {
            "name": "D2-1. Nhiệt Độ Cực Đại 3 Miền Bắc - Trung - Nam",
            "desc": "So sánh nhiệt độ cao nhất trong ngày giữa 3 miền (Tmax °C), nhận diện nhanh khu vực nắng nóng gay gắt.",
            "display": "bar",
            "sql": f"""
                {TARGET_WEATHER_CTE}
                SELECT 
                    CASE loc.region_key 
                        WHEN 1 THEN 'Miền Bắc'
                        WHEN 2 THEN 'Miền Trung'
                        WHEN 3 THEN 'Miền Nam'
                    END AS "Khu Vực",
                    ROUND(MAX(w.temperature_2m), 1) AS "Nhiệt Độ Cực Đại (°C)"
                FROM gold.fact_weather_hourly w
                JOIN gold.dim_location loc ON w.location_key = loc.location_key
                JOIN target ON w.date_key = target.date_key
                WHERE loc.region_key IN (1, 2, 3)
                GROUP BY loc.region_key
                ORDER BY "Nhiệt Độ Cực Đại (°C)" DESC
            """,
            "viz": {
                "graph.dimensions": ["Khu Vực"],
                "graph.metrics": ["Nhiệt Độ Cực Đại (°C)"],
                "graph.x_axis.title_text": "Khu Vực",
                "graph.y_axis.title_text": "Tmax (°C)",
                "graph.show_values": True
            },
            "col": 0, "row": 0, "size_x": 6, "size_y": 4,
            "has_date_filter": True
        },
        {
            "name": "D2-2. Xếp Hạng Lưu Lượng Nước Về Theo Lưu Vực Sông",
            "desc": "Thanh ngang trực quan xếp hạng tổng lưu lượng dòng chảy về các lưu vực sông (m³/s).",
            "display": "row",
            "sql": f"""
                {TARGET_HYDRO_CTE}
                SELECT 
                    basin_name AS "Lưu Vực Sông",
                    ROUND(SUM(inflow_m3s)) AS "Nước Về (m³/s)"
                FROM gold.v_hydro_daily
                JOIN target ON date_value = target.selected_date
                GROUP BY basin_name
                ORDER BY "Nước Về (m³/s)" DESC
            """,
            "viz": {
                "graph.dimensions": ["Lưu Vực Sông"],
                "graph.metrics": ["Nước Về (m³/s)"],
                "graph.show_values": True
            },
            "col": 6, "row": 0, "size_x": 6, "size_y": 4,
            "has_date_filter": True
        },
        {
            "name": "D2-3. Cơ Cấu Xả Nước: Tuabin Phát Điện vs Xả Tràn Điều Tiết",
            "desc": "Biểu đồ tròn (Donut) thể hiện tỷ lệ nước sinh công qua tổ máy tuabin vs nước xả tràn bảo vệ an toàn đập.",
            "display": "pie",
            "sql": f"""
                {TARGET_HYDRO_CTE},
                agg AS (
                    SELECT 
                        ROUND(SUM(turbine_flow_m3s)) AS turbine_flow,
                        ROUND(SUM(spill_flow_m3s)) AS spill_flow
                    FROM gold.v_hydro_daily
                    JOIN target ON date_value = target.selected_date
                )
                SELECT 'Tuabin phát điện (Sinh công)' AS "Dòng Xả", turbine_flow AS "Lưu Lượng (m³/s)" FROM agg
                UNION ALL
                SELECT 'Xả tràn đón lũ (An toàn đập)', spill_flow FROM agg
            """,
            "viz": {
                "pie.dimension": "Dòng Xả",
                "pie.metric": "Lưu Lượng (m³/s)",
                "pie.show_legend": True,
                "pie.percent_visibility": "inside"
            },
            "col": 12, "row": 0, "size_x": 6, "size_y": 4,
            "has_date_filter": True
        },
        {
            "name": "D2-4. Phân Bổ An Toàn & Rủi Ro 39 Hồ Chứa (Lũ vs Hạn)",
            "desc": "Số lượng hồ chứa phân bổ theo 4 cấp rủi ro: Đang xả tràn, Cận đỉnh (≥90%), Nguy cơ cạn kiệt (≤25%) và An toàn.",
            "display": "bar",
            "sql": f"""
                {TARGET_HYDRO_CTE},
                dams AS (
                    SELECT 
                        CASE 
                            WHEN spill_gates_open > 0 OR spill_flow_m3s > 0 THEN '🟠 Đang Xả Tràn'
                            WHEN storage_pct >= 90.0 THEN '🟡 Cận Đỉnh (≥90%)'
                            WHEN storage_pct <= 25.0 OR (water_level_m - dead_level_m) <= 2.0 THEN '⚠️ Nguy Cơ Cạn Kiệt'
                            ELSE '🟢 Vận Hành An Toàn'
                        END AS "Phân Cấp Rủi Ro"
                    FROM gold.v_hydro_daily
                    JOIN target ON date_value = target.selected_date
                )
                SELECT 
                    "Phân Cấp Rủi Ro",
                    COUNT(*) AS "Số Lượng Hồ"
                FROM dams
                GROUP BY "Phân Cấp Rủi Ro"
                ORDER BY 
                    CASE "Phân Cấp Rủi Ro"
                        WHEN '🟠 Đang Xả Tràn' THEN 1
                        WHEN '🟡 Cận Đỉnh (≥90%)' THEN 2
                        WHEN '⚠️ Nguy Cơ Cạn Kiệt' THEN 3
                        ELSE 4
                    END
            """,
            "viz": {
                "graph.dimensions": ["Phân Cấp Rủi Ro"],
                "graph.metrics": ["Số Lượng Hồ"],
                "graph.show_values": True,
                "graph.y_axis.title_text": "Số lượng hồ chứa"
            },
            "col": 18, "row": 0, "size_x": 6, "size_y": 4,
            "has_date_filter": True
        },

        # ======================================================================
        # NHỊP 2: 2 BIỂU ĐỒ HERO TRỌNG ĐIỂM (Rows 4-12, Height 9, Width 12)
        # ======================================================================
        {
            "name": "D2-5. [Hero 1] Tổng Hợp An Toàn Tích Nước & Xả Tràn Theo 8 Lưu Vực Sông",
            "desc": "So sánh 2 trục: Dung tích hữu ích có trọng số (%) [Trục Trái - Cột] vs Tổng lưu lượng xả tràn (m³/s) [Trục Phải - Đường], kèm số hồ cận đỉnh và cạn kiệt.",
            "display": "combo",
            "sql": f"""
                {TARGET_HYDRO_CTE}
                SELECT 
                    basin_name AS "Lưu Vực Sông",
                    ROUND(
                        SUM(water_level_m - dead_level_m) * 100.0 / 
                        NULLIF(SUM(normal_level_m - dead_level_m), 0), 1
                    ) AS "Dung tích hữu ích có trọng số (%)",
                    ROUND(SUM(spill_flow_m3s)) AS "Tổng xả tràn (m³/s)",
                    COUNT_IF(storage_pct >= 90.0) AS "Số hồ cận đỉnh (≥90%)",
                    COUNT_IF(storage_pct <= 25.0 OR (water_level_m - dead_level_m) <= 2.0) AS "Số hồ nguy cơ cạn kiệt",
                    SUM(spill_gates_open) AS "Số cửa xả mở"
                FROM gold.v_hydro_daily
                JOIN target ON date_value = target.selected_date
                GROUP BY basin_name
                ORDER BY "Dung tích hữu ích có trọng số (%)" DESC
            """,
            "viz": {
                "graph.dimensions": ["Lưu Vực Sông"],
                "graph.metrics": ["Dung tích hữu ích có trọng số (%)", "Tổng xả tràn (m³/s)"],
                "graph.series_settings": {
                    "Dung tích hữu ích có trọng số (%)": {
                        "type": "bar",
                        "y_axis": "left",
                        "title": "Dung tích hữu ích có trọng số (%)"
                    },
                    "Tổng xả tràn (m³/s)": {
                        "type": "line",
                        "y_axis": "right",
                        "title": "Tổng xả tràn điều tiết (m³/s)"
                    }
                },
                "graph.y_axis.title_text": "Dung tích hữu ích (%) [Trục Trái]",
                "graph.x_axis.title_text": "8 Lưu Vực Thủy Điện Toàn Quốc (Sắp xếp theo độ đầy dung tích hữu ích giảm dần)"
            },
            "col": 0, "row": 4, "size_x": 12, "size_y": 9,
            "has_date_filter": True
        },
        {
            "name": "D2-6. [Hero 2] Diễn Biến Nhiệt Độ & Bức Xạ 24 Giờ 3 Miền Bắc - Trung - Nam",
            "desc": "Theo dõi 24h đối chiếu nhiệt độ 3 miền và cường độ bức xạ mặt trời; xác định khung giờ nắng trưa 10h-13h để tích nước (chỉ áp dụng cho hồ an toàn, không áp dụng cho hồ đầy/xả tràn).",
            "display": "line",
            "sql": f"""
                {TARGET_WEATHER_CTE}
                SELECT 
                    w.time_key AS "Giờ",
                    ROUND(AVG(CASE WHEN loc.region_key = 1 THEN w.temperature_2m END), 1) AS "Nhiệt độ Miền Bắc (°C)",
                    ROUND(AVG(CASE WHEN loc.region_key = 2 THEN w.temperature_2m END), 1) AS "Nhiệt độ Miền Trung (°C)",
                    ROUND(AVG(CASE WHEN loc.region_key = 3 THEN w.temperature_2m END), 1) AS "Nhiệt độ Miền Nam (°C)",
                    ROUND(AVG(CASE WHEN loc.region_key IN (2, 3) THEN w.shortwave_radiation END), 0) AS "Bức xạ Nam/Trung (W/m²)"
                FROM gold.fact_weather_hourly w
                JOIN gold.dim_location loc ON w.location_key = loc.location_key
                JOIN target ON w.date_key = target.date_key
                WHERE loc.region_key IN (1, 2, 3)
                GROUP BY w.time_key
                ORDER BY w.time_key
            """,
            "viz": {
                "graph.dimensions": ["Giờ"],
                "graph.metrics": [
                    "Nhiệt độ Miền Bắc (°C)",
                    "Nhiệt độ Miền Trung (°C)",
                    "Nhiệt độ Miền Nam (°C)",
                    "Bức xạ Nam/Trung (W/m²)"
                ],
                "graph.x_axis.axis_enabled": True,
                "graph.y_axis.axis_enabled": True
            },
            "col": 12, "row": 4, "size_x": 12, "size_y": 9,
            "has_date_filter": True
        },

        # ======================================================================
        # NHỊP 3: BẢNG CHỈ HUY HÀNH ĐỘNG DUY NHẤT TRÊN DASHBOARD (Rows 13-20, Height 8)
        # Bảng điều độ tác chiến tối cần thiết: Danh sách hồ rủi ro & khuyến nghị NSMO
        # ======================================================================
        {
            "name": "D2-7. [Bảng Chỉ Huy] Danh Mục Hồ Cần Hành Động & Gợi Ý Điều Tiết Điều Độ",
            "desc": "Danh mục hồ chứa cần xử lý 2 đầu rủi ro (Xả tràn, Cận đỉnh đón lũ & Nguy cơ cạn kiệt/sát MN chết). Hướng dẫn gợi ý theo thứ tự ưu tiên: 1. An toàn đập > 2. Quy trình liên hồ > 3. Tối ưu ĐMT.",
            "display": "table",
            "sql": f"""
                {TARGET_HYDRO_CTE},
                dams AS (
                    SELECT 
                        reservoir_name,
                        basin_name,
                        water_level_m,
                        normal_level_m,
                        dead_level_m,
                        storage_pct,
                        spill_gates_open,
                        inflow_m3s,
                        turbine_flow_m3s,
                        spill_flow_m3s,
                        alert_level,
                        ROUND(normal_level_m - water_level_m, 2) AS dist_to_top,
                        ROUND(water_level_m - dead_level_m, 2) AS dist_to_dead,
                        ROUND(inflow_m3s - turbine_flow_m3s - spill_flow_m3s) AS net_inflow,
                        CASE 
                            WHEN water_level_m >= normal_level_m AND alert_level = 3 THEN '🔴 Khẩn cấp'
                            WHEN spill_gates_open > 0 OR spill_flow_m3s > 0 THEN '🟠 Đang xả tràn'
                            WHEN storage_pct >= 90.0 THEN '🟡 Cận đỉnh (≥90%)'
                            WHEN storage_pct <= 25.0 OR (water_level_m - dead_level_m) <= 2.0 THEN '⚠️ Nguy cơ cạn kiệt'
                            ELSE '🟢 Bình thường'
                        END AS risk_tier,
                        CASE 
                            WHEN water_level_m >= normal_level_m AND alert_level = 3 
                                THEN 'Mở xả khẩn cấp theo quy trình liên hồ; Cảnh báo hạ du; Giữ phát tuabin tối đa Pmax'
                            WHEN spill_gates_open > 0 OR spill_flow_m3s > 0 
                                THEN 'Duy trì tuabin công suất cao để giảm xả tràn; Giám sát chặt chẽ lưu lượng về hạ du'
                            WHEN storage_pct >= 90.0 
                                THEN 'Chủ động phát điện hạ mực nước đón lũ (bỏ qua tối ưu ĐMT); Theo dõi sát dự báo mưa lưu vực'
                            WHEN storage_pct <= 25.0 OR (water_level_m - dead_level_m) <= 2.0 
                                THEN 'Hạn chế phát điện giờ cao điểm trưa; Giữ nước phục vụ cấp nước sinh hoạt/nông nghiệp hạ du'
                            ELSE 'Vận hành kinh tế bình thường theo biểu đồ điều độ A0/NSMO'
                        END AS action_advice
                    FROM gold.v_hydro_daily
                    JOIN target ON date_value = target.selected_date
                ),
                summary_check AS (
                    SELECT COUNT(*) AS cnt FROM dams WHERE risk_tier != '🟢 Bình thường'
                )
                SELECT 
                    risk_tier AS "Mức độ",
                    reservoir_name AS "Tên Hồ Thủy Điện",
                    basin_name AS "Lưu Vực",
                    water_level_m AS "Mực nước (m)",
                    dist_to_top AS "Cách đỉnh (m)",
                    dist_to_dead AS "Cách MN chết (m)",
                    storage_pct AS "Dung tích (%)",
                    spill_gates_open AS "Cửa xả",
                    ROUND(inflow_m3s) AS "Nước về (m³/s)",
                    ROUND(spill_flow_m3s) AS "Xả tràn (m³/s)",
                    ROUND(net_inflow) AS "Lưu lượng ròng (m³/s)",
                    action_advice AS "Gợi ý điều độ (Ưu tiên: An toàn đập > Quy trình liên hồ > Tối ưu ĐMT)"
                FROM dams
                WHERE risk_tier != '🟢 Bình thường'
                UNION ALL
                SELECT 
                    '🟢 Bình thường' AS "Mức độ",
                    'Toàn bộ các hồ chứa' AS "Tên Hồ Thủy Điện",
                    'Toàn quốc' AS "Lưu Vực",
                    0.0 AS "Mực nước (m)",
                    0.0 AS "Cách đỉnh (m)",
                    0.0 AS "Cách MN chết (m)",
                    0.0 AS "Dung tích (%)",
                    0 AS "Cửa xả",
                    0.0 AS "Nước về (m³/s)",
                    0.0 AS "Xả tràn (m³/s)",
                    0.0 AS "Lưu lượng ròng (m³/s)",
                    'Tất cả 39 hồ chứa đều vận hành an toàn trong giới hạn quy trình liên hồ chứa.' AS "Gợi ý điều độ (Ưu tiên: An toàn đập > Quy trình liên hồ > Tối ưu ĐMT)"
                FROM summary_check sc
                WHERE sc.cnt = 0
                ORDER BY 
                    CASE "Mức độ"
                        WHEN '🔴 Khẩn cấp' THEN 1
                        WHEN '🟠 Đang xả tràn' THEN 2
                        WHEN '🟡 Cận đỉnh (≥90%)' THEN 3
                        WHEN '⚠️ Nguy cơ cạn kiệt' THEN 4
                        ELSE 5
                    END,
                    "Dung tích (%)" DESC
            """,
            "viz": {},
            "col": 0, "row": 13, "size_x": 24, "size_y": 8,
            "has_date_filter": True
        },

        # ======================================================================
        # NHỊP 4: 2 BIỂU ĐỒ ĐÀO SÂU CHI TIẾT (Rows 21-29, Height 9, Width 12)
        # Thay thế bảng 39 dòng bằng Biểu đồ Top 10 Hồ Cận Đỉnh + Đối chiếu ĐMT
        # ======================================================================
        {
            "name": "D2-8. Top 10 Hồ Thủy Điện Tiệm Cận Đỉnh Dung Tích & Nguy Cơ Tràn",
            "desc": "Biểu đồ thanh ngang trực quan Top 10 hồ chứa có tỷ lệ tích nước dung tích hữu ích cao nhất (%). Cảnh báo ngưỡng an toàn đón lũ.",
            "display": "row",
            "sql": f"""
                {TARGET_HYDRO_CTE}
                SELECT 
                    reservoir_name AS "Hồ Thủy Điện",
                    storage_pct AS "Dung Tích Hữu Ích (%)",
                    ROUND(inflow_m3s) AS "Nước Về (m³/s)"
                FROM gold.v_hydro_daily
                JOIN target ON date_value = target.selected_date
                ORDER BY storage_pct DESC
                LIMIT 10
            """,
            "viz": {
                "graph.dimensions": ["Hồ Thủy Điện"],
                "graph.metrics": ["Dung Tích Hữu Ích (%)"],
                "graph.show_values": True,
                "graph.x_axis.title_text": "Dung tích hữu ích (%)"
            },
            "col": 0, "row": 21, "size_x": 12, "size_y": 9,
            "has_date_filter": True
        },
        {
            "name": "D2-9. Tổng Hợp Bức Xạ vs Công Suất Phát Điện Mặt Trời (Giờ Nắng 7h-17h)",
            "desc": "Kiểm tra hiệu suất quang điện: Đối chiếu bức xạ mặt trời (W/m²) với công suất phát thực tế (MW) các khung giờ ban ngày.",
            "display": "bar",
            "sql": f"""
                {TARGET_WEATHER_CTE},
                w AS (
                    SELECT 
                        w.time_key AS hour_of_day,
                        ROUND(AVG(w.shortwave_radiation), 0) AS rad_wm2
                    FROM gold.fact_weather_hourly w
                    JOIN gold.dim_location loc ON w.location_key = loc.location_key
                    JOIN target ON w.date_key = target.date_key
                    WHERE loc.region_key IN (2, 3) AND w.time_key BETWEEN 7 AND 17
                    GROUP BY w.time_key
                ),
                g AS (
                    SELECT 
                        f.time_key AS hour_of_day,
                        ROUND(SUM(f.generation_power_mw)) AS solar_mw
                    FROM gold.fact_generation_power_hourly f
                    JOIN gold.dim_energy_source s ON f.energy_source_key = s.energy_source_key
                    CROSS JOIN target
                    WHERE f.date_key = GREATEST(target.date_key, 20261104)
                      AND (s.source_name LIKE '%mặt trời%' OR s.source_name LIKE '%ĐMT%')
                      AND f.time_key BETWEEN 7 AND 17
                    GROUP BY f.time_key
                )
                SELECT 
                    CAST(w.hour_of_day AS VARCHAR) || 'h' AS "Khung Giờ",
                    w.rad_wm2 AS "Bức xạ mặt trời (W/m²)",
                    COALESCE(g.solar_mw, 0) AS "Công suất ĐMT (MW)"
                FROM w
                LEFT JOIN g ON w.hour_of_day = g.hour_of_day
                ORDER BY w.hour_of_day
            """,
            "viz": {
                "graph.dimensions": ["Khung Giờ"],
                "graph.metrics": ["Bức xạ mặt trời (W/m²)", "Công suất ĐMT (MW)"],
                "graph.x_axis.title_text": "Khung Giờ Ban Ngày (7h - 17h)",
                "graph.y_axis.title_text": "Bức xạ (W/m²) / Công suất (MW)"
            },
            "col": 12, "row": 21, "size_x": 12, "size_y": 9,
            "has_date_filter": True
        }
    ]

    card_specs = []
    for c in cards_def:
        cid = upsert_card(
            url, token, col_id, db_id,
            c["name"], c["sql"], c["display"],
            viz=c.get("viz"), tag_def=TAG_DEF_DATE if c.get("has_date_filter", True) else None,
            description=c.get("desc")
        )
        card_specs.append({
            "card_id": cid,
            "col": c["col"],
            "row": c["row"],
            "size_x": c["size_x"],
            "size_y": c["size_y"],
            "has_date_filter": c.get("has_date_filter", True)
        })

    set_dashboard_layout(url, token, dash_id, card_specs, param_id="date_filter")

    print(f"\n[SUCCESS] Deployed 9 cards on {DASHBOARD_NAME} (Dashboard ID: {dash_id})!")
    print(f"URL: {url}/dashboard/{dash_id}\n")
    return dash_id


if __name__ == "__main__":
    deploy_dashboard_02()
