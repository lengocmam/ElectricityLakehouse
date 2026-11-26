# -*- coding: utf-8 -*-
"""Dashboard 02: Năng Lượng Tự Nhiên 3 Miền (Thủy Điện, Nắng & Gió).

Chuẩn mực học thuật theo phê duyệt của người dùng:
1. LOẠI BỎ SỰ LẠM DỤNG DẠNG BẢNG (No Table-Heavy Dashboard):
   - 8 Biểu đồ trực quan cao cấp (Bar, Row Bar, Donut, Multi-Line) + DUY NHẤT 1 Bảng Chỉ Huy Hành Động (D2-7).
2. TUYỆT ĐỐI KHÔNG TÍNH TRUNG BÌNH (NO AVG):
   - 100% chỉ số sử dụng SUM, MAX hoặc giá trị đo thực tế từng điểm giờ từ các bảng Fact.
3. PHÂN VÙNG ĐIỀU ĐỘ CHUẨN QUỐC GIA:
   - Thay thế 8 lưu vực mơ hồ bằng 3 Trung tâm Điều độ Hệ thống điện Miền: A1 (Bắc), A2 (Trung & Tây Nguyên), A3 (Nam).
4. ĐỐI XỨNG CUNG - CẦU 3 MIỀN:
   - D2-1: Cung Thủy điện 3 Miền.
   - D2-2: Cung Nắng & Gió 3 Miền.
   - D2-3: Cầu Phụ tải Đỉnh 3 Miền.
   - D2-4: Cơ cấu Nguồn Tự nhiên Toàn quốc.
5. PHÂN TẦNG THỜI GIAN KHOA HỌC:
   - Hàng 1, D2-5, D2-8: Đo lường theo NGÀY (Daily Aggregate: SUM / MAX).
   - D2-6, D2-9: Diễn biến 24 GIỜ (Hourly Dynamic) phản ánh quy luật bù trừ "Nắng gánh trưa, Nước giữ đỉnh tối".
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

DASHBOARD_NAME = "02. Khí Tượng & Thủy Văn Năng Lượng Tái Tạo"
DASHBOARD_DESC = (
    "Trung tâm Giám sát Điều độ Năng lượng Tự nhiên (Thủy điện, Nắng, Gió) theo 3 Trung tâm A1 - A2 - A3 "
    "và Diễn biến Phối hợp Bù trừ 24 Giờ Toàn quốc."
)

TAG_DEF_DATE = COMMON_TAG_DEF

# Unified Fast Target CTE: Chống nghẽn full-table scan, tốc độ phản hồi < 1s
TARGET_CTE = """
WITH user_input AS (
    SELECT COALESCE(
        NULL [[ , CAST({{selected_date}} AS DATE) ]],
        DATE '2026-11-16'
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
        # NHỊP 1: 4 BIỂU ĐỒ ĐỐI XỨNG CUNG - CẦU 3 MIỀN TRONG NGÀY (Rows 0-3, Height 4, Width 6)
        # 100% Không tính trung bình (NO AVG) - Chỉ dùng SUM, MAX
        # ======================================================================
        {
            "name": "D2-1. Thủy văn hồ chứa 3 miền",
            "desc": "So sánh tổng lưu lượng nước về hồ và lưu lượng xả qua tổ máy tuabin phát điện sinh công của 3 phân vùng điều độ A1 (Bắc), A2 (Trung & Tây Nguyên), A3 (Nam).",
            "display": "bar",
            "sql": f"""
                {TARGET_HYDRO_CTE},
                dams AS (
                    SELECT 
                        CASE 
                            WHEN basin_name IN ('Sông Đà', 'Sông Lô - Gâm') OR reservoir_name IN ('Bản Vẽ', 'Trung Sơn', 'KHE BỐ')
                                THEN 'A1 - Miền Bắc'
                            WHEN basin_name IN ('Sông Đồng Nai') OR reservoir_name IN ('Đồng Nai 3', 'Đồng Nai 4', 'Đa Mi', 'Đơn Dương', 'Đại Ninh')
                                THEN 'A3 - Miền Nam'
                            ELSE 'A2 - Miền Trung & Tây Nguyên'
                        END AS dispatch_region,
                        inflow_m3s,
                        turbine_flow_m3s
                    FROM gold.v_hydro_daily
                    JOIN target ON date_value = target.selected_date
                )
                SELECT 
                    dispatch_region AS "Phân Vùng Điều Độ",
                    ROUND(SUM(inflow_m3s)) AS "Nước Về (m³/s)",
                    ROUND(SUM(turbine_flow_m3s)) AS "Xả Tuabin Phát Điện (m³/s)"
                FROM dams
                GROUP BY dispatch_region
                ORDER BY "Nước Về (m³/s)" DESC
            """,
            "viz": {
                "graph.dimensions": ["Phân Vùng Điều Độ"],
                "graph.metrics": ["Nước Về (m³/s)", "Xả Tuabin Phát Điện (m³/s)"],
                "graph.x_axis.title_text": "Phân Vùng Điều Độ",
                "graph.y_axis.title_text": "Lưu lượng (m³/s)",
                "graph.show_values": True
            },
            "col": 0, "row": 0, "size_x": 6, "size_y": 4,
            "has_date_filter": True
        },
        {
            "name": "D2-2. Tiềm năng nắng gió 3 miền",
            "desc": "So sánh tổng bức xạ mặt trời tích lũy (MJ/m²) và tốc độ gió cực đại (MAX m/s) giữa 3 miền trong ngày (Không dùng AVG).",
            "display": "row",
            "sql": f"""
                {TARGET_WEATHER_CTE}
                SELECT 
                    CASE loc.region_key 
                        WHEN 1 THEN 'Miền Bắc'
                        WHEN 2 THEN 'Miền Trung'
                        WHEN 3 THEN 'Miền Nam'
                    END AS "Khu Vực",
                    ROUND(SUM(w.shortwave_radiation * 3600 / 1000000.0), 1) AS "Bức Xạ Tích Lũy (MJ/m²)",
                    ROUND(MAX(w.wind_speed_10m), 1) AS "Gió Cực Đại (m/s)"
                FROM gold.fact_weather_hourly w
                JOIN gold.dim_location loc ON w.location_key = loc.location_key
                JOIN target ON w.date_key = target.date_key
                WHERE loc.region_key IN (1, 2, 3)
                GROUP BY loc.region_key
                ORDER BY "Bức Xạ Tích Lũy (MJ/m²)" DESC
            """,
            "viz": {
                "graph.dimensions": ["Khu Vực"],
                "graph.metrics": ["Bức Xạ Tích Lũy (MJ/m²)", "Gió Cực Đại (m/s)"],
                "graph.show_values": True
            },
            "col": 6, "row": 0, "size_x": 6, "size_y": 4,
            "has_date_filter": True
        },
        {
            "name": "D2-3. Phụ tải đỉnh Pmax 3 miền",
            "desc": "So sánh công suất tiêu thụ điện cực đại (MAX load_mw) trong ngày của 3 miền Bắc - Trung - Nam, làm rõ sự chênh lệch nhu cầu tiêu thụ.",
            "display": "bar",
            "sql": f"""
                {TARGET_CTE}
                SELECT 
                    CASE f.region_key 
                        WHEN 1 THEN 'Miền Bắc'
                        WHEN 2 THEN 'Miền Trung'
                        WHEN 3 THEN 'Miền Nam'
                    END AS "Khu Vực",
                    ROUND(MAX(f.load_mw)) AS "Phụ Tải Đỉnh Pmax (MW)"
                FROM gold.fact_load_hourly f
                JOIN target ON f.date_key = target.date_key
                WHERE f.region_key IN (1, 2, 3)
                GROUP BY f.region_key
                ORDER BY "Phụ Tải Đỉnh Pmax (MW)" DESC
            """,
            "viz": {
                "graph.dimensions": ["Khu Vực"],
                "graph.metrics": ["Phụ Tải Đỉnh Pmax (MW)"],
                "graph.y_axis.title_text": "Pmax (MW)",
                "graph.show_values": True
            },
            "col": 12, "row": 0, "size_x": 6, "size_y": 4,
            "has_date_filter": True
        },
        {
            "name": "D2-4. Cơ cấu nguồn tái tạo tức thời",
            "desc": "Biểu đồ tròn (Donut) so sánh tỷ trọng tổng sản lượng ngày (MWh) giữa 3 nguồn năng lượng tự nhiên theo thứ bậc.",
            "display": "pie",
            "sql": f"""
                {TARGET_CTE}
                SELECT 
                    CASE 
                        WHEN s.source_name LIKE '%Thủy điện%' THEN '1. Thủy điện'
                        WHEN s.source_name LIKE '%mặt trời%' OR s.source_name LIKE '%ĐMT%' THEN '2. Điện mặt trời'
                        WHEN s.source_name LIKE '%Gió%' THEN '3. Điện gió'
                    END AS "Nguồn Tự Nhiên",
                    ROUND(SUM(f.generation_power_mw)) AS "Sản Lượng (MWh)"
                FROM gold.fact_generation_power_hourly f
                JOIN gold.dim_energy_source s ON f.energy_source_key = s.energy_source_key
                CROSS JOIN target
                WHERE f.date_key = target.date_key
                  AND (s.source_name LIKE '%Thủy điện%' OR s.source_name LIKE '%mặt trời%' OR s.source_name LIKE '%ĐMT%' OR s.source_name LIKE '%Gió%')
                GROUP BY 1
                ORDER BY "Sản Lượng (MWh)" DESC
            """,
            "viz": {
                "pie.dimension": "Nguồn Tự Nhiên",
                "pie.metric": "Sản Lượng (MWh)",
                "pie.show_legend": True,
                "pie.percent_visibility": "inside"
            },
            "col": 18, "row": 0, "size_x": 6, "size_y": 4,
            "has_date_filter": True
        },

        # ======================================================================
        # NHỊP 2: 2 BIỂU ĐỒ HERO TRỌNG ĐIỂM (Rows 4-12, Height 9, Width 12)
        # ======================================================================
        {
            "name": "D2-5. Tích nước hồ chứa 3 miền",
            "desc": "Tỷ lệ tích nước dung tích hữu ích có trọng số (%) và Tổng lưu lượng xả tràn điều tiết của 3 phân vùng điều độ quốc gia A1, A2, A3.",
            "display": "row",
            "sql": f"""
                {TARGET_HYDRO_CTE},
                dams AS (
                    SELECT 
                        CASE 
                            WHEN basin_name IN ('Sông Đà', 'Sông Lô - Gâm') OR reservoir_name IN ('Bản Vẽ', 'Trung Sơn', 'KHE BỐ')
                                THEN 'A1 - Miền Bắc'
                            WHEN basin_name IN ('Sông Đồng Nai') OR reservoir_name IN ('Đồng Nai 3', 'Đồng Nai 4', 'Đa Mi', 'Đơn Dương', 'Đại Ninh')
                                THEN 'A3 - Miền Nam'
                            ELSE 'A2 - Miền Trung & Tây Nguyên'
                        END AS dispatch_region,
                        water_level_m,
                        normal_level_m,
                        dead_level_m,
                        spill_flow_m3s
                    FROM gold.v_hydro_daily
                    JOIN target ON date_value = target.selected_date
                )
                SELECT 
                    dispatch_region AS "Phân Vùng Điều Độ",
                    ROUND(
                        SUM(water_level_m - dead_level_m) * 100.0 / 
                        NULLIF(SUM(normal_level_m - dead_level_m), 0), 1
                    ) AS "Tỷ Lệ Tích Nước Hữu Ích (%)",
                    ROUND(SUM(spill_flow_m3s)) AS "Tổng Xả Tràn (m³/s)"
                FROM dams
                GROUP BY dispatch_region
                ORDER BY "Tỷ Lệ Tích Nước Hữu Ích (%)" DESC
            """,
            "viz": {
                "graph.dimensions": ["Phân Vùng Điều Độ"],
                "graph.metrics": ["Tỷ Lệ Tích Nước Hữu Ích (%)", "Tổng Xả Tràn (m³/s)"],
                "graph.show_values": True,
                "graph.x_axis.title_text": "Phân Vùng Điều Độ Quốc Gia"
            },
            "col": 0, "row": 4, "size_x": 12, "size_y": 9,
            "has_date_filter": True
        },
        {
            "name": "D2-6. Phối hợp nguồn tái tạo 24h",
            "desc": "Diễn biến công suất phát 24 giờ của Thủy điện, Điện mặt trời, Điện gió và Phụ tải toàn quốc (Tất cả là SUM thực tế, không dùng AVG). Làm rõ quy luật: Nắng gánh trưa, Nước giữ đỉnh tối.",
            "display": "line",
            "sql": f"""
                {TARGET_CTE},
                g AS (
                    SELECT 
                        f.time_key AS hour_val,
                        ROUND(SUM(CASE WHEN s.source_name LIKE '%Thủy điện%' THEN f.generation_power_mw END)) AS hydro_mw,
                        ROUND(SUM(CASE WHEN s.source_name LIKE '%mặt trời%' OR s.source_name LIKE '%ĐMT%' THEN f.generation_power_mw END)) AS solar_mw,
                        ROUND(SUM(CASE WHEN s.source_name LIKE '%Gió%' THEN f.generation_power_mw END)) AS wind_mw
                    FROM gold.fact_generation_power_hourly f
                    JOIN gold.dim_energy_source s ON f.energy_source_key = s.energy_source_key
                    CROSS JOIN target
                    WHERE f.date_key = target.date_key
                    GROUP BY f.time_key
                ),
                l AS (
                    SELECT 
                        f.time_key AS hour_val,
                        ROUND(SUM(f.load_mw)) AS load_mw
                    FROM gold.fact_load_hourly f
                    JOIN target ON f.date_key = target.date_key
                    WHERE f.region_key IN (1, 2, 3)
                    GROUP BY f.time_key
                )
                SELECT 
                    CAST(g.hour_val AS VARCHAR) || 'h' AS "Giờ",
                    COALESCE(g.hydro_mw, 0) AS "Thủy Điện (MW)",
                    COALESCE(g.solar_mw, 0) AS "Điện Mặt Trời (MW)",
                    COALESCE(g.wind_mw, 0) AS "Điện Gió (MW)",
                    COALESCE(l.load_mw, 0) AS "Phụ Tải Quốc Gia (MW)"
                FROM g
                JOIN l ON g.hour_val = l.hour_val
                ORDER BY g.hour_val
            """,
            "viz": {
                "graph.dimensions": ["Giờ"],
                "graph.metrics": [
                    "Thủy Điện (MW)",
                    "Điện Mặt Trời (MW)",
                    "Điện Gió (MW)",
                    "Phụ Tải Quốc Gia (MW)"
                ],
                "graph.x_axis.axis_enabled": True,
                "graph.y_axis.axis_enabled": True
            },
            "col": 12, "row": 4, "size_x": 12, "size_y": 9,
            "has_date_filter": True
        },

        # ======================================================================
        # NHỊP 3: BẢNG CHỈ HUY HÀNH ĐỘNG DUY NHẤT TRÊN DASHBOARD (Rows 13-20, Height 8)
        # ======================================================================
        {
            "name": "D2-7. Giám sát an toàn hồ chứa",
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
        # ======================================================================
        {
            "name": "D2-8. Top 10 hồ nguy cơ xả tràn",
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
            "name": "D2-9. Hiệu suất công suất mặt trời",
            "desc": "Kiểm tra hiệu suất quang điện: Đối chiếu bức xạ mặt trời cực đại (MAX W/m²) với công suất phát thực tế (MW) các khung giờ ban ngày (Không dùng AVG).",
            "display": "bar",
            "sql": f"""
                {TARGET_WEATHER_CTE},
                w AS (
                    SELECT 
                        w.time_key AS hour_of_day,
                        ROUND(MAX(w.shortwave_radiation)) AS max_rad_wm2
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
                    WHERE f.date_key = target.date_key
                      AND (s.source_name LIKE '%mặt trời%' OR s.source_name LIKE '%ĐMT%')
                      AND f.time_key BETWEEN 7 AND 17
                    GROUP BY f.time_key
                )
                SELECT 
                    CAST(w.hour_of_day AS VARCHAR) || 'h' AS "Khung Giờ",
                    w.max_rad_wm2 AS "Bức Xạ Cực Đại (W/m²)",
                    COALESCE(g.solar_mw, 0) AS "Công Suất ĐMT (MW)"
                FROM w
                LEFT JOIN g ON w.hour_of_day = g.hour_of_day
                ORDER BY w.hour_of_day
            """,
            "viz": {
                "graph.dimensions": ["Khung Giờ"],
                "graph.metrics": ["Bức Xạ Cực Đại (W/m²)", "Công Suất ĐMT (MW)"],
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
