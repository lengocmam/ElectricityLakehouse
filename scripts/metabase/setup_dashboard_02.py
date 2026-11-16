# -*- coding: utf-8 -*-
"""Dashboard 02: Thời Tiết Cực Đoan, An Ninh Nước & Thủy Điện (Tổng Hợp Toàn Diện & Cảnh Báo Lũ - Hạn).

Thiết kế nâng cấp toàn diện theo phản hồi chuyên sâu của Điều độ viên NSMO:
1. Cân bằng rủi ro Lũ & Hạn Hán: Giám sát đối xứng cả hồ cận đỉnh/xả tràn và hồ cạn kiệt/sát mực nước chết.
2. Phân cấp cảnh báo 4 mức chuẩn mực:
   - 🔴 Khẩn cấp: Vượt MNDBT + lũ về lớn (ưu tiên bảo vệ an toàn công trình đập).
   - 🟠 Đang xả tràn: Cửa xả mở hoặc lưu lượng xả tràn > 0 (điều tiết an toàn đón lũ).
   - 🟡 Cận đỉnh (>=90%): Tiệm cận đỉnh dung tích hoặc cách MNDBT < 1m (phát điện hạ mực nước).
   - ⚠️ Nguy cơ cạn kiệt (<=25% hoặc cách MN chết <= 2m): Giữ nước, nhường chỗ cho điện mặt trời trưa.
   - 🟢 An toàn: Vận hành bình thường trong dải điều tiết 25% - 90%.
3. Làm rõ thứ tự ưu tiên điều độ:
   Ưu tiên 1 (An toàn đập) > Ưu tiên 2 (Quy trình liên hồ & hạ du) > Ưu tiên 3 (Tối ưu hấp thụ ĐMT).
   Đổi ngôn từ sang dạng gợi ý điều độ hỗ trợ ra quyết định.
4. Kỹ thuật đồ thị Hero 1:
   - Combo chart 2 trục: Trục trái Dung tích hữu ích có trọng số (%), trục phải Lưu lượng xả tràn (m3/s).
   - Tính tỷ lệ dung tích tích nước theo trọng số dung tích hữu ích thay vì trung bình cộng đơn giản.
   - Thêm cột khoảng cách tới đỉnh, khoảng cách tới mực nước chết và lưu lượng ròng (Q về - Q xả).
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
    "Trung tâm Giám sát Khí tượng Thủy văn & Cảnh báo An ninh Nước (Lũ & Hạn): Tổng hợp sóng nhiệt 3 miền, "
    "phân cấp cảnh báo 4 mức trên 8 lưu vực sông, giám sát hồ cận đỉnh và hồ cạn kiệt, hướng dẫn điều độ theo thứ tự ưu tiên."
)

TAG_DEF_DATE = COMMON_TAG_DEF

TARGET_DATE_CTE = """
WITH target AS (
    SELECT COALESCE(
        NULL [[ , CAST({{selected_date}} AS DATE) ]],
        (SELECT MAX(date_value) FROM gold.v_hydro_daily)
    ) AS selected_date
)
""".strip()


def deploy_dashboard_02(base_url=None, user=None, password=None):
    url = get_metabase_url(base_url)
    print("\n" + "=" * 70)
    print(f"DEPLOYING {DASHBOARD_NAME} (NÂNG CẤP LŨ - HẠN & 4 MỨC CẢNH BÁO)")
    print(f"Target Metabase: {url}")
    print("=" * 70)

    token = authenticate(url, user or "admin@lakehouse.vn", password or "Lakehouse@2026")
    db_id = get_database_id(url, token)
    col_id = get_or_create_collection(url, token)
    dash_id = get_or_create_dashboard(url, token, col_id, DASHBOARD_NAME, DASHBOARD_DESC)

    cards_def = [
        # ======================================================================
        # NHỊP 1: 4 KPIs TỔNG HỢP CẢNH BÁO TOÀN DIỆN (Rows 0-3, Height 4, Width 6)
        # ======================================================================
        {
            "name": "D2-1. Tổng Hợp Nhiệt Độ Cực Đại 3 Miền & Cảnh Báo Sóng Nhiệt",
            "desc": "Tmax 3 miền Bắc - Trung - Nam, miền có nhiệt độ cao nhất và cảnh báo nguy cơ sốc nhiệt phụ tải máy lạnh.",
            "display": "scalar",
            "sql": f"""
                {TARGET_DATE_CTE},
                w AS (
                    SELECT 
                        ROUND(MAX(CASE WHEN loc.region_key = 1 THEN w.temperature_2m END), 1) AS tmax_bac,
                        ROUND(MAX(CASE WHEN loc.region_key = 2 THEN w.temperature_2m END), 1) AS tmax_trung,
                        ROUND(MAX(CASE WHEN loc.region_key = 3 THEN w.temperature_2m END), 1) AS tmax_nam
                    FROM gold.fact_weather_hourly w
                    JOIN gold.dim_location loc ON w.location_key = loc.location_key
                    JOIN target ON w.date_key = (YEAR(target.selected_date)*10000 + MONTH(target.selected_date)*100 + DAY(target.selected_date))
                    WHERE loc.region_key IN (1, 2, 3)
                )
                SELECT 
                    CASE 
                        WHEN GREATEST(tmax_bac, tmax_trung, tmax_nam) >= 38.0 THEN '🔴 Sốc nhiệt gay gắt: '
                        WHEN GREATEST(tmax_bac, tmax_trung, tmax_nam) >= 35.0 THEN '🟡 Nắng nóng diện rộng: '
                        ELSE '🟢 Thời tiết ôn hòa: '
                    END || CAST(GREATEST(tmax_bac, tmax_trung, tmax_nam) AS VARCHAR) || '°C (Bắc: ' || 
                    CAST(tmax_bac AS VARCHAR) || '°C | Trung: ' || CAST(tmax_trung AS VARCHAR) || '°C | Nam: ' || 
                    CAST(tmax_nam AS VARCHAR) || '°C)' AS "Tổng Hợp Nhiệt Độ 3 Miền"
                FROM w
            """,
            "viz": {
                "scalar.title": "TỔNG HỢP NHIỆT ĐỘ CỰC ĐẠI 3 MIỀN",
                "scalar.subtitle": "Bắc - Trung - Nam | Ngưỡng cảnh báo nắng nóng: ≥ 35°C"
            },
            "col": 0, "row": 0, "size_x": 6, "size_y": 4,
            "has_date_filter": True
        },
        {
            "name": "D2-2. Tổng Hợp Lưu Lượng Nước Về 39 Hồ Chứa & Lưu Vực Trọng Điểm",
            "desc": "Tổng lưu lượng dòng chảy về 39 hồ thủy điện toàn quốc và xác định lưu vực đón lượng nước lớn nhất.",
            "display": "scalar",
            "sql": f"""
                {TARGET_DATE_CTE},
                d AS (
                    SELECT 
                        basin_name,
                        SUM(inflow_m3s) AS basin_inflow
                    FROM gold.v_hydro_daily
                    JOIN target ON date_value = target.selected_date
                    GROUP BY basin_name
                ),
                t AS (
                    SELECT 
                        ROUND(SUM(basin_inflow)) AS total_inflow,
                        MAX_BY(basin_name, basin_inflow) AS top_basin,
                        ROUND(MAX(basin_inflow)) AS top_inflow
                    FROM d
                )
                SELECT 
                    format('%,d', CAST(total_inflow AS BIGINT)) || ' m³/s (Lớn nhất: ' || top_basin || ' ' || 
                    format('%,d', CAST(top_inflow AS BIGINT)) || ' m³/s - ' || 
                    CAST(CAST(ROUND(top_inflow * 100.0 / NULLIF(total_inflow, 0)) AS INTEGER) AS VARCHAR) || '%)' AS "Tổng Nước Về Hồ"
                FROM t
            """,
            "viz": {
                "scalar.title": "TỔNG LƯU LƯỢNG NƯỚC VỀ 39 HỒ",
                "scalar.subtitle": "Toàn hệ thống thủy điện & Lưu vực chiếm tỷ trọng lớn nhất"
            },
            "col": 6, "row": 0, "size_x": 6, "size_y": 4,
            "has_date_filter": True
        },
        {
            "name": "D2-3. Cơ Cấu Lưu Lượng Xả Hồ: Tuabin Phát Điện vs Xả Tràn Điều Tiết Lũ",
            "desc": "Phân định lưu lượng nước qua tổ máy tuabin sinh công và lưu lượng xả tràn bảo đảm an toàn hồ chứa theo quy trình vận hành liên hồ.",
            "display": "scalar",
            "sql": f"""
                {TARGET_DATE_CTE}
                SELECT 
                    format('%,d', CAST(ROUND(SUM(turbine_flow_m3s)) AS BIGINT)) || ' m³/s tuabin (' || 
                    CAST(CAST(ROUND(100.0 * SUM(turbine_flow_m3s) / NULLIF(SUM(turbine_flow_m3s) + SUM(spill_flow_m3s), 0)) AS INTEGER) AS VARCHAR) || 
                    '%) | Xả tràn: ' || format('%,d', CAST(ROUND(SUM(spill_flow_m3s)) AS BIGINT)) || ' m³/s điều tiết' AS "Cơ Cấu Lưu Lượng Xả"
                FROM gold.v_hydro_daily
                JOIN target ON date_value = target.selected_date
            """,
            "viz": {
                "scalar.title": "CƠ CẤU XẢ: PHÁT ĐIỆN VS ĐIỀU TIẾT LŨ",
                "scalar.subtitle": "Nước qua máy phát điện vs Nước xả tràn an toàn đập & hạ du"
            },
            "col": 12, "row": 0, "size_x": 6, "size_y": 4,
            "has_date_filter": True
        },
        {
            "name": "D2-4. Phân Cấp An Toàn & Rủi Ro 39 Hồ Chứa (Lũ & Hạn Hán)",
            "desc": "Giám sát cân bằng 2 đầu rủi ro: Hồ xả tràn, hồ tiệm cận đỉnh đón lũ, hồ nguy cơ cạn kiệt (<=25% hoặc sát MN chết <=2m), và hồ an toàn.",
            "display": "scalar",
            "sql": f"""
                {TARGET_DATE_CTE},
                cats AS (
                    SELECT 
                        reservoir_name,
                        CASE 
                            WHEN water_level_m >= normal_level_m AND alert_level = 3 THEN 'danger'
                            WHEN spill_gates_open > 0 OR spill_flow_m3s > 0 THEN 'spilling'
                            WHEN storage_pct >= 90.0 THEN 'near_full'
                            WHEN storage_pct <= 25.0 OR (water_level_m - dead_level_m) <= 2.0 THEN 'drought'
                            ELSE 'safe'
                        END AS tier,
                        spill_gates_open
                    FROM gold.v_hydro_daily
                    JOIN target ON date_value = target.selected_date
                )
                SELECT 
                    '🟠 ' || CAST(COUNT_IF(tier = 'spilling') AS VARCHAR) || ' Xả tràn (' || 
                    CAST(COALESCE(SUM(spill_gates_open), 0) AS VARCHAR) || ' cửa)  |  🟡 ' || 
                    CAST(COUNT_IF(tier = 'near_full') AS VARCHAR) || ' Cận đỉnh  |  ⚠️ ' || 
                    CAST(COUNT_IF(tier = 'drought') AS VARCHAR) || ' Cạn kiệt  |  🟢 ' || 
                    CAST(COUNT_IF(tier = 'safe') AS VARCHAR) || ' An toàn' AS "Phân Cấp Rủi Ro Lũ & Hạn"
                FROM cats
            """,
            "viz": {
                "scalar.title": "PHÂN CẤP RỦI RO 39 HỒ CHỨA (LŨ & HẠN)",
                "scalar.subtitle": "🟠 Đang xả tràn | 🟡 Cận đỉnh (≥90%) | ⚠️ Cạn kiệt (≤25% / sát MN chết) | 🟢 An toàn"
            },
            "col": 18, "row": 0, "size_x": 6, "size_y": 4,
            "has_date_filter": True
        },

        # ======================================================================
        # NHỊP 2: 2 HERO CHARTS TỔNG HỢP CHIẾN LƯỢC (Rows 4-12, Height 9, Width 12 each)
        # ======================================================================
        {
            "name": "D2-5. [Hero 1] Tổng Hợp An Toàn Tích Nước & Xả Tràn Theo 8 Lưu Vực Sông",
            "desc": "So sánh 2 trục: Dung tích hữu ích có trọng số (%) [Trục Trái - Cột] vs Tổng lưu lượng xả tràn (m³/s) [Trục Phải - Đường], kèm số hồ cận đỉnh và cạn kiệt.",
            "display": "combo",
            "sql": f"""
                {TARGET_DATE_CTE}
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
                {TARGET_DATE_CTE}
                SELECT 
                    w.time_key AS "Giờ",
                    ROUND(AVG(CASE WHEN loc.region_key = 1 THEN w.temperature_2m END), 1) AS "Nhiệt độ Miền Bắc (°C)",
                    ROUND(AVG(CASE WHEN loc.region_key = 2 THEN w.temperature_2m END), 1) AS "Nhiệt độ Miền Trung (°C)",
                    ROUND(AVG(CASE WHEN loc.region_key = 3 THEN w.temperature_2m END), 1) AS "Nhiệt độ Miền Nam (°C)",
                    ROUND(AVG(CASE WHEN loc.region_key IN (2, 3) THEN w.shortwave_radiation END), 0) AS "Bức xạ Nam/Trung (W/m²)"
                FROM gold.fact_weather_hourly w
                JOIN gold.dim_location loc ON w.location_key = loc.location_key
                JOIN target ON w.date_key = (YEAR(target.selected_date)*10000 + MONTH(target.selected_date)*100 + DAY(target.selected_date))
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
        # NHỊP 3: BẢNG CHỈ HUY HÀNH ĐỘNG ĐIỀU TIẾT CẢNH BÁO (Rows 13-21, Height 9)
        # ======================================================================
        {
            "name": "D2-7. [Bảng Chỉ Huy] Danh Mục Hồ Cần Hành Động & Gợi Ý Điều Tiết Điều Độ",
            "desc": "Danh mục hồ chứa cần xử lý 2 đầu rủi ro (Xả tràn, Cận đỉnh đón lũ & Nguy cơ cạn kiệt/sát MN chết). Hướng dẫn gợi ý theo thứ tự ưu tiên: 1. An toàn đập > 2. Quy trình liên hồ > 3. Tối ưu ĐMT.",
            "display": "table",
            "sql": f"""
                {TARGET_DATE_CTE},
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
                        (inflow_m3s - turbine_flow_m3s - spill_flow_m3s) AS net_flow_m3s,
                        (normal_level_m - water_level_m) AS head_to_normal_m,
                        (water_level_m - dead_level_m) AS head_to_dead_m,
                        CASE 
                            WHEN water_level_m >= normal_level_m AND alert_level = 3 THEN '🔴 Khẩn cấp'
                            WHEN spill_gates_open > 0 OR spill_flow_m3s > 0 THEN '🟠 Đang xả tràn'
                            WHEN storage_pct >= 90.0 THEN '🟡 Cận đỉnh (≥90%)'
                            WHEN storage_pct <= 25.0 OR (water_level_m - dead_level_m) <= 2.0 THEN '⚠️ Nguy cơ cạn kiệt'
                            ELSE '🟢 An toàn'
                        END AS alert_tier
                    FROM gold.v_hydro_daily
                    JOIN target ON date_value = target.selected_date
                ),
                abnormal AS (
                    SELECT * FROM dams WHERE alert_tier != '🟢 An toàn'
                ),
                summary_check AS (
                    SELECT COUNT(*) AS cnt FROM abnormal
                )
                SELECT 
                    a.alert_tier AS "Mức độ",
                    a.reservoir_name AS "Tên Hồ Thủy Điện",
                    a.basin_name AS "Lưu Vực",
                    a.water_level_m AS "Mực nước (m)",
                    ROUND(a.head_to_normal_m, 2) AS "Cách đỉnh (m)",
                    ROUND(a.head_to_dead_m, 2) AS "Cách MN chết (m)",
                    a.storage_pct AS "Dung tích (%)",
                    a.spill_gates_open AS "Cửa xả",
                    ROUND(a.inflow_m3s) AS "Nước về (m³/s)",
                    ROUND(a.spill_flow_m3s) AS "Xả tràn (m³/s)",
                    ROUND(a.net_flow_m3s) AS "Lưu lượng ròng (m³/s)",
                    CASE 
                        WHEN a.alert_tier = '🔴 Khẩn cấp' THEN '🔴 BÁO ĐỘNG LŨ (Ưu tiên 1 - An toàn đập): Vượt MNDBT. Gợi ý điều độ: Khẩn trương phối hợp Ban Chỉ huy PCTT mở cửa xả lũ đón lũ theo quy trình khẩn cấp, cảnh báo còi hạ du; phát tối đa công suất tổ máy (Pmax) thoát nước.'
                        WHEN a.alert_tier = '🟠 Đang xả tràn' THEN '🟠 ĐANG XẢ TRÀN ĐIỀU TIẾT (Mở ' || CAST(a.spill_gates_open AS VARCHAR) || ' cửa, xả ' || CAST(CAST(ROUND(a.spill_flow_m3s) AS BIGINT) AS VARCHAR) || ' m³/s): Gợi ý điều độ: Giữ phát điện cao tải để tối đa sản lượng qua tuabin, tránh xả tràn lãng phí nếu máy khả dụng; kiểm soát sát tốc độ xả hạ du.'
                        WHEN a.alert_tier = '🟡 Cận đỉnh (≥90%)' THEN '🟡 TIỆM CẬN ĐỈNH DUNG TÍCH (' || CAST(a.storage_pct AS VARCHAR) || '% - Cách đỉnh ' || CAST(ROUND(a.head_to_normal_m, 2) AS VARCHAR) || 'm): Ưu tiên an toàn hồ chứa hơn giảm phát ĐMT. Gợi ý điều độ: Phát điện liên tục chu kỳ cao điểm sáng/tối để hạ mực nước đón lũ, không tích thêm nước.'
                        WHEN a.alert_tier = '⚠️ Nguy cơ cạn kiệt' THEN '⚠️ CẢNH BÁO CẠN KIỆT (' || CAST(a.storage_pct AS VARCHAR) || '% - Cách MN chết ' || CAST(ROUND(a.head_to_dead_m, 2) AS VARCHAR) || 'm): Gợi ý điều độ: Tiết kiệm nước triệt để; giảm phát tối đa giờ trưa (10h-13h) nhường chỗ cho ĐMT; chỉ phát cao điểm tối hoặc cấp nước hạ du.'
                        ELSE '🟢 VẬN HÀNH AN TOÀN'
                    END AS "Gợi ý điều độ (Ưu tiên: An toàn đập > Quy trình liên hồ > Tối ưu ĐMT)"
                FROM abnormal a
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
            "col": 0, "row": 13, "size_x": 24, "size_y": 9,
            "has_date_filter": True
        },

        # ======================================================================
        # NHỊP 4: MA TRẬN 39 HỒ CHỨA & ĐỐI CHIẾU BỨC XẠ (Rows 22-30, Height 9)
        # ======================================================================
        {
            "name": "D2-8. Ma Trận Giám Sát Chi Tiết Toàn Bộ 39 Hồ Chứa Thủy Điện",
            "desc": "Toàn cảnh 39 hồ thủy điện: Mực nước, dung tích hữu ích, khoảng cách tới đỉnh/chết, lưu lượng ròng, nước về, xả tuabin, xả tràn và phân cấp 4 mức.",
            "display": "table",
            "sql": f"""
                {TARGET_DATE_CTE}
                SELECT 
                    basin_name AS "Lưu Vực Sông",
                    reservoir_name AS "Hồ Thủy Điện",
                    water_level_m AS "Mực nước (m)",
                    normal_level_m AS "MNDBT (m)",
                    dead_level_m AS "MN Chết (m)",
                    ROUND(normal_level_m - water_level_m, 2) AS "Cách đỉnh (m)",
                    ROUND(water_level_m - dead_level_m, 2) AS "Cách MN chết (m)",
                    storage_pct AS "Dung tích (%)",
                    spill_gates_open AS "Cửa xả",
                    ROUND(inflow_m3s) AS "Q về (m³/s)",
                    ROUND(turbine_flow_m3s) AS "Q qua máy (m³/s)",
                    ROUND(spill_flow_m3s) AS "Q xả tràn (m³/s)",
                    ROUND(inflow_m3s - turbine_flow_m3s - spill_flow_m3s) AS "Q ròng (m³/s)",
                    CASE 
                        WHEN water_level_m >= normal_level_m AND alert_level = 3 THEN '🔴 Khẩn cấp'
                        WHEN spill_gates_open > 0 OR spill_flow_m3s > 0 THEN '🟠 Đang xả tràn'
                        WHEN storage_pct >= 90.0 THEN '🟡 Cận đỉnh (≥90%)'
                        WHEN storage_pct <= 25.0 OR (water_level_m - dead_level_m) <= 2.0 THEN '⚠️ Nguy cơ cạn kiệt'
                        ELSE '🟢 An toàn'
                    END AS "Phân Cấp Rủi Ro"
                FROM gold.v_hydro_daily
                JOIN target ON date_value = target.selected_date
                ORDER BY 
                    CASE 
                        WHEN water_level_m >= normal_level_m AND alert_level = 3 THEN 1
                        WHEN spill_gates_open > 0 OR spill_flow_m3s > 0 THEN 2
                        WHEN storage_pct >= 90.0 THEN 3
                        WHEN storage_pct <= 25.0 OR (water_level_m - dead_level_m) <= 2.0 THEN 4
                        ELSE 5
                    END,
                    storage_pct DESC
            """,
            "viz": {},
            "col": 0, "row": 22, "size_x": 14, "size_y": 9,
            "has_date_filter": True
        },
        {
            "name": "D2-9. Tổng Hợp Bức Xạ vs Công Suất Phát Điện Mặt Trời (Giờ Nắng 7h-17h)",
            "desc": "Kiểm tra hiệu suất quang điện: Đối chiếu bức xạ mặt trời (W/m²) với công suất phát thực tế (MW) các khung giờ ban ngày.",
            "display": "bar",
            "sql": f"""
                {TARGET_DATE_CTE},
                w AS (
                    SELECT 
                        w.time_key AS hour_of_day,
                        ROUND(AVG(w.shortwave_radiation), 0) AS rad_wm2
                    FROM gold.fact_weather_hourly w
                    JOIN gold.dim_location loc ON w.location_key = loc.location_key
                    JOIN target ON w.date_key = (YEAR(target.selected_date)*10000 + MONTH(target.selected_date)*100 + DAY(target.selected_date))
                    WHERE loc.region_key IN (2, 3) AND w.time_key BETWEEN 7 AND 17
                    GROUP BY w.time_key
                ),
                g AS (
                    SELECT 
                        f.time_key AS hour_of_day,
                        ROUND(SUM(f.generation_power_mw)) AS solar_mw
                    FROM gold.fact_generation_power_hourly f
                    JOIN gold.dim_energy_source s ON f.energy_source_key = s.energy_source_key
                    JOIN target ON f.date_key = (YEAR(target.selected_date)*10000 + MONTH(target.selected_date)*100 + DAY(target.selected_date))
                    WHERE (s.source_name LIKE '%mặt trời%' OR s.source_name LIKE '%ĐMT%')
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
            "col": 14, "row": 22, "size_x": 10, "size_y": 9,
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
