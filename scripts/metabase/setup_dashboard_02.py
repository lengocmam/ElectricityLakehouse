# -*- coding: utf-8 -*-
"""Dashboard 02: Thời Tiết Cực Đoan, An Ninh Nước & Thủy Điện (Tổng Hợp Toàn Diện & Cảnh Báo Đa Vùng).

Thiết kế nâng cấp theo chuẩn Data & Risk Storytelling:
- Màn hình 1 (NHỊP 1): 4 Thẻ KPI Tổng Hợp Cảnh Báo Toàn Diện:
  1. Tổng hợp Nhiệt độ Cực đại 3 Miền & Cảnh báo Sóng nhiệt (Tmax Bắc, Trung, Nam).
  2. Tổng hợp Lưu lượng Nước về Hệ thống Thủy điện & Lưu vực Dòng chảy Lớn nhất.
  3. Tổng hợp Lưu lượng Xả & Tỷ lệ Tận dụng Phát điện (Q tuabin vs Q xả tràn).
  4. Phân cấp An toàn 39 Hồ chứa & Tổng số Cửa xả Lũ đang mở.
- Màn hình 2 (NHỊP 2): 2 Hero Charts Chiến Lược Tổng Hợp:
  1. [Hero 1] Tổng hợp An toàn Tích nước & Xả tràn theo 8 Lưu vực Sông (Bar Chart).
  2. [Hero 2] Diễn biến Nhiệt độ & Bức xạ 24 Giờ 3 Miền Bắc - Trung - Nam (Line Chart).
- Màn hình 3 (NHỊP 3): [Bảng Chỉ Huy] Danh Mục Hồ Có Cảnh Báo & Hướng Dẫn Điều Tiết Điều Độ (Actionable Dam Triage).
- Màn hình 4 (NHỊP 4):
  1. Ma trận Giám sát Chi tiết Toàn bộ 39 Hồ chứa Thủy điện (Phân nhóm theo Lưu vực).
  2. Đối chiếu Bức xạ Mặt trời vs Công suất Phát Điện Mặt trời Giờ Cao điểm (7h - 17h).
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
    "Trung tâm Giám sát Khí tượng Thủy văn & Cảnh báo Thiên tai: Tổng hợp sóng nhiệt 3 miền, "
    "an toàn tích nước và xả lũ liên hồ chứa trên 8 lưu vực sông, tối ưu hóa phát điện tuabin và phòng chống ngập lụt hạ du."
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
    print(f"DEPLOYING {DASHBOARD_NAME} (TỔNG HỢP & CẢNH BÁO TOÀN DIỆN)")
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
            "name": "D2-3. Tổng Hợp Lưu Lượng Xả & Tỷ Lệ Tận Dụng Phát Điện",
            "desc": "Hiệu quả sử dụng nước: Tổng lưu lượng xả qua tổ máy tuabin vs xả tràn và tỷ lệ nước sinh ra điện.",
            "display": "scalar",
            "sql": f"""
                {TARGET_DATE_CTE}
                SELECT 
                    format('%,d', CAST(ROUND(SUM(turbine_flow_m3s)) AS BIGINT)) || ' m³/s tuabin (' || 
                    CAST(ROUND(100.0 * SUM(turbine_flow_m3s) / NULLIF(SUM(turbine_flow_m3s) + SUM(spill_flow_m3s), 0), 1) AS VARCHAR) || 
                    '%) | Xả tràn: ' || format('%,d', CAST(ROUND(SUM(spill_flow_m3s)) AS BIGINT)) || ' m³/s' AS "Lưu Lượng Xả & Tận Dụng"
                FROM gold.v_hydro_daily
                JOIN target ON date_value = target.selected_date
            """,
            "viz": {
                "scalar.title": "TỔNG LƯU LƯỢNG XẢ & TỶ LỆ TẬN DỤNG",
                "scalar.subtitle": "Nước qua máy phát điện vs Nước xả tràn hạ du"
            },
            "col": 12, "row": 0, "size_x": 6, "size_y": 4,
            "has_date_filter": True
        },
        {
            "name": "D2-4. Phân Cấp An Toàn 39 Hồ Chứa & Tổng Số Cửa Xả Lũ",
            "desc": "Phân loại tổng hợp mức độ rủi ro 39 hồ: Số hồ Đỏ, Vàng, Xanh và số cửa xả lũ đang mở.",
            "display": "scalar",
            "sql": f"""
                {TARGET_DATE_CTE}
                SELECT 
                    '🔴 ' || CAST(COUNT_IF(alert_level = 3) AS VARCHAR) || ' Đỏ  |  🟡 ' || 
                    CAST(COUNT_IF(alert_level = 2 OR spill_gates_open > 0 OR storage_pct >= 95.0) AS VARCHAR) || ' Vàng  |  🟢 ' || 
                    CAST(COUNT_IF(alert_level = 1 AND spill_gates_open = 0 AND storage_pct < 95.0) AS VARCHAR) || 
                    ' Xanh (' || CAST(COALESCE(SUM(spill_gates_open), 0) AS VARCHAR) || ' cửa xả mở)' AS "Phân Cấp An Toàn Hồ Chứa"
                FROM gold.v_hydro_daily
                JOIN target ON date_value = target.selected_date
            """,
            "viz": {
                "scalar.title": "PHÂN CẤP AN TOÀN 39 HỒ CHỨA",
                "scalar.subtitle": "🔴 Nguy hiểm | 🟡 Cảnh báo (xả tràn/đầy) | 🟢 Bình thường"
            },
            "col": 18, "row": 0, "size_x": 6, "size_y": 4,
            "has_date_filter": True
        },

        # ======================================================================
        # NHỊP 2: 2 HERO CHARTS TỔNG HỢP CHIẾN LƯỢC (Rows 4-12, Height 9, Width 12 each)
        # ======================================================================
        {
            "name": "D2-5. [Hero 1] Tổng Hợp An Toàn Tích Nước & Xả Tràn Theo 8 Lưu Vực Sông",
            "desc": "Bức tranh tổng hợp theo lưu vực: So sánh tỷ lệ dung tích tích nước hữu ích (%) và lưu lượng xả tràn (m³/s) giúp nhận biết lưu vực căng thẳng.",
            "display": "bar",
            "sql": f"""
                {TARGET_DATE_CTE}
                SELECT 
                    basin_name AS "Lưu Vực Sông",
                    ROUND(AVG(storage_pct), 1) AS "Dung tích tích nước TB (%)",
                    ROUND(SUM(spill_flow_m3s)) AS "Tổng xả tràn (m³/s)",
                    SUM(spill_gates_open) AS "Số cửa xả mở"
                FROM gold.v_hydro_daily
                JOIN target ON date_value = target.selected_date
                GROUP BY basin_name
                ORDER BY "Dung tích tích nước TB (%)" DESC
            """,
            "viz": {
                "graph.dimensions": ["Lưu Vực Sông"],
                "graph.metrics": ["Dung tích tích nước TB (%)", "Tổng xả tràn (m³/s)"],
                "graph.y_axis.title_text": "Tỷ lệ tích nước (%) / Lưu lượng (m³/s)",
                "graph.x_axis.title_text": "Lưu Vực Sông (Sắp xếp theo độ đầy hồ giảm dần)"
            },
            "col": 0, "row": 4, "size_x": 12, "size_y": 9,
            "has_date_filter": True
        },
        {
            "name": "D2-6. [Hero 2] Diễn Biến Nhiệt Độ & Bức Xạ 24 Giờ 3 Miền Bắc - Trung - Nam",
            "desc": "Theo dõi 24h đối chiếu nhiệt độ 3 miền và cường độ bức xạ mặt trời, nhận diện tức thời thời điểm sốc nhiệt và đỉnh quang điện.",
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
            "name": "D2-7. [Bảng Chỉ Huy] Danh Mục Hồ Có Cảnh Báo & Hướng Dẫn Điều Tiết Điều Độ",
            "desc": "Liệt kê chính xác các hồ chứa đang có cửa xả mở hoặc dung tích tiệm cận đỉnh kèm quy trình hành động cụ thể cho Điều độ viên.",
            "display": "table",
            "sql": f"""
                {TARGET_DATE_CTE},
                abnormal AS (
                    SELECT 
                        alert_label,
                        alert_level,
                        reservoir_name,
                        basin_name,
                        water_level_m,
                        normal_level_m,
                        storage_pct,
                        spill_gates_open,
                        inflow_m3s,
                        turbine_flow_m3s,
                        spill_flow_m3s
                    FROM gold.v_hydro_daily
                    JOIN target ON date_value = target.selected_date
                    WHERE alert_level >= 2 OR spill_gates_open > 0 OR storage_pct >= 90.0
                ),
                summary_check AS (
                    SELECT COUNT(*) AS cnt FROM abnormal
                )
                SELECT 
                    a.alert_label AS "Mức độ",
                    a.reservoir_name AS "Tên Hồ Thủy Điện",
                    a.basin_name AS "Lưu Vực",
                    a.water_level_m AS "Mực nước (m)",
                    a.normal_level_m AS "Mực nước dâng BT (m)",
                    a.storage_pct AS "Dung tích tích nước (%)",
                    a.spill_gates_open AS "Cửa xả mở",
                    ROUND(a.inflow_m3s) AS "Nước về (m³/s)",
                    ROUND(a.spill_flow_m3s) AS "Xả tràn (m³/s)",
                    CASE 
                        WHEN a.alert_level = 3 THEN '🔴 BÁO ĐỘNG LŨ: Mực nước vượt ngưỡng dâng BT. Phát lệnh mở cửa xả lũ hạ du và phát tối đa công suất tổ máy.'
                        WHEN a.spill_gates_open > 0 THEN '🟡 ĐANG XẢ TRÀN: Đang mở ' || CAST(a.spill_gates_open AS VARCHAR) || ' cửa xả. Giám sát cảnh báo hạ du và điều tiết xả an toàn đón lũ.'
                        WHEN a.storage_pct >= 95.0 THEN '🟡 TIỆM CẬN ĐỈNH: Dung tích đạt ' || CAST(a.storage_pct AS VARCHAR) || '%. Ưu tiên phát tối đa tuabin để hạ mực nước hồ về ngưỡng đón lũ.'
                        ELSE '🟡 THEO DÕI TÍCH NƯỚC: Dung tích cao (>90%), sẵn sàng chuyển chế độ đón lũ nếu có mưa lớn thượng nguồn.'
                    END AS "Khuyến nghị điều tiết cho Điều độ viên"
                FROM abnormal a
                UNION ALL
                SELECT 
                    '🟢 Bình thường' AS "Mức độ",
                    'Toàn bộ các hồ chứa' AS "Tên Hồ Thủy Điện",
                    'Toàn quốc' AS "Lưu Vực",
                    0.0 AS "Mực nước (m)",
                    0.0 AS "Mực nước dâng BT (m)",
                    0.0 AS "Dung tích tích nước (%)",
                    0 AS "Cửa xả mở",
                    0.0 AS "Nước về (m³/s)",
                    0.0 AS "Xả tràn (m³/s)",
                    'Tất cả 39 hồ chứa đều vận hành an toàn trong giới hạn quy trình liên hồ chứa.' AS "Khuyến nghị điều tiết cho Điều độ viên"
                FROM summary_check sc
                WHERE sc.cnt = 0
                ORDER BY 1, 7 DESC, 6 DESC
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
            "desc": "Toàn cảnh 39 hồ thủy điện: Mực nước, dung tích hữu ích, nước về, xả qua máy, xả tràn và trạng thái an toàn.",
            "display": "table",
            "sql": f"""
                {TARGET_DATE_CTE}
                SELECT 
                    basin_name AS "Lưu Vực Sông",
                    reservoir_name AS "Hồ Thủy Điện",
                    water_level_m AS "Mực nước (m)",
                    normal_level_m AS "Mực nước dâng BT (m)",
                    dead_level_m AS "Mực nước chết (m)",
                    storage_pct AS "Dung tích tích nước (%)",
                    spill_gates_open AS "Cửa xả mở",
                    ROUND(inflow_m3s) AS "Q về (m³/s)",
                    ROUND(turbine_flow_m3s) AS "Q qua máy (m³/s)",
                    ROUND(spill_flow_m3s) AS "Q xả tràn (m³/s)",
                    alert_label AS "Trạng Thái An Toàn"
                FROM gold.v_hydro_daily
                JOIN target ON date_value = target.selected_date
                ORDER BY basin_name, alert_level DESC, storage_pct DESC
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
