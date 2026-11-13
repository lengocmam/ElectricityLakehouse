# -*- coding: utf-8 -*-
"""Dashboard 02: Thời Tiết Cực Đoan, An Ninh Nước & Thủy Điện.

Storytelling Architecture:
- Thông điệp: Thời tiết tác động theo hai hướng: nhiệt độ ảnh hưởng nhu cầu điện,
  mưa ảnh hưởng lượng nước về hồ và an toàn vận hành.
- Nguồn kết hợp: Open-Meteo (nhiệt độ, bức xạ, mưa) × Phụ tải NSMO × Thủy văn EVN × Điện mặt trời NSMO.
- Bộ lọc: {{selected_date}} (chọn ngày) và {{reservoir}} (chọn hồ thủy điện).
"""
import sys
import os

if hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from common import (
    authenticate,
    get_database_id,
    get_metabase_url,
    get_or_create_collection,
    get_or_create_dashboard,
    parse_common_args,
    set_dashboard_layout,
    upsert_card,
    COMMON_TAG_DEF,
)

DASHBOARD_NAME = "02. Thời Tiết Cực Đoan, An Ninh Nước & Thủy Điện"
DASHBOARD_DESC = (
    "Thời tiết tác động theo hai hướng: nhiệt độ ảnh hưởng nhu cầu điện máy lạnh, "
    "mưa ảnh hưởng lượng nước về hồ và an toàn vận hành hồ đập thủy điện quốc gia."
)

TARGET_DATE_CTE = """
WITH target AS (
    SELECT COALESCE(
        NULL [[ , CAST({{selected_date}} AS DATE) ]],
        (SELECT MAX(date_value) FROM gold.v_gen_hourly)
    ) AS selected_date
)
""".strip()

HYDRO_PLANTS = [
    'Bản Chát', 'Huội Quảng', 'Hòa Bình', 'Lai Châu', 'Sơn La', 'Thác Bà', 'Tuyên Quang',
    'Đa Mi', 'Đại Ninh', 'Đơn Dương', 'Đồng Nai 3', 'Đồng Nai 4', 'Hàm Thuận', 'Thác Mơ', 'Trị An',
    'A Vương', 'An Khê', 'Buôn Kuốp', 'Buôn Tua Srah', 'Bản Vẽ', 'Ialy', 'Kanak', 'Khe Bố',
    'Pleikrông', 'Quảng Trị', 'Sê San 3', 'Sê San 3A', 'Sê San 4', 'Sông Ba Hạ', 'Sông Bung 2',
    'Sông Bung 4', 'Sông Hinh', 'Sông Tranh 2', 'Thượng Kon Tum', 'Trung Sơn', 'Vĩnh Sơn A'
]

TAGS_DATE_AND_RESERVOIR = {
    "selected_date": {
        "id": "tag_selected_date",
        "name": "selected_date",
        "display-name": "Chọn Ngày",
        "type": "date",
        "required": False
    },
    "reservoir": {
        "id": "tag_reservoir",
        "name": "reservoir",
        "display-name": "Chọn Hồ Thủy Điện",
        "type": "text",
        "required": False
    }
}

DASHBOARD_PARAMETERS = [
    {
        "id": "date_filter",
        "name": "Chọn Ngày (Để trống = Tự động lấy ngày mới nhất)",
        "slug": "selected_date",
        "type": "date/single",
        "default": None
    },
    {
        "id": "reservoir_filter",
        "name": "Chọn Hồ Thủy Điện",
        "slug": "reservoir",
        "type": "string/=",
        "default": "Hòa Bình",
        "values_source_type": "static-list",
        "values_source_config": {
            "values": sorted(HYDRO_PLANTS)
        }
    }
]


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
        # ======================================================================
        # NHỊP 1: 4 KPIs THỜI TIẾT & THỦY VĂN (Rows 0-3, Height 4, Width 6)
        # ======================================================================
        {
            "name": "D2-1. Nhiệt độ cực đại & Bình quân trọng số phụ tải",
            "desc": "Tmax toàn quốc kèm vùng/giờ xảy ra và nhiệt độ bình quân có trọng số theo phụ tải 3 miền.",
            "display": "scalar",
            "sql": f"""
                {TARGET_DATE_CTE},
                w AS (
                  SELECT * FROM gold.v_weather_hourly WHERE date_value = (SELECT selected_date FROM target)
                ),
                l AS (
                  SELECT * FROM gold.v_load_region_hourly WHERE date_value = (SELECT selected_date FROM target)
                ),
                j AS (
                  SELECT w.temperature_c, w.region_name, w.hour_of_day, l.load_mw
                  FROM w JOIN l ON w.hour_of_day = l.hour_of_day AND w.region_name = l.region_name
                )
                SELECT ROUND(MAX(temperature_c), 1) AS tmax_c,
                       MAX_BY(region_name, temperature_c) AS tmax_region,
                       MAX_BY(hour_of_day, temperature_c) AS tmax_hour,
                       ROUND(SUM(temperature_c * load_mw) / NULLIF(SUM(load_mw), 0), 1) AS temp_weighted_c
                FROM j
            """,
            "viz": {
                "column_settings": {
                    '["name","tmax_c"]': {"column_title": "Tmax Toàn Quốc (°C)"},
                    '["name","tmax_region"]': {"column_title": "Miền Tmax"},
                    '["name","tmax_hour"]': {"column_title": "Giờ Tmax"},
                    '["name","temp_weighted_c"]': {"column_title": "Nhiệt độ trọng số (°C)"}
                }
            },
            "col": 0, "row": 0, "size_x": 6, "size_y": 4
        },
        {
            "name": "D2-2. Tổng lưu lượng nước về hồ & Biến động 7 ngày",
            "desc": "Tổng lưu lượng dòng chảy về các hồ thủy điện (m³/s) so với trung bình 7 ngày trước.",
            "display": "scalar",
            "sql": f"""
                {TARGET_DATE_CTE},
                d AS (
                  SELECT date_value, SUM(inflow_m3s) AS q_in
                  FROM gold.v_hydro_daily
                  WHERE date_value BETWEEN (SELECT selected_date FROM target) - INTERVAL '7' DAY AND (SELECT selected_date FROM target)
                  GROUP BY 1
                )
                SELECT ROUND(MAX(CASE WHEN date_value = (SELECT selected_date FROM target) THEN q_in END)) AS q_in_today,
                       ROUND(AVG(CASE WHEN date_value < (SELECT selected_date FROM target) THEN q_in END)) AS q_in_avg7,
                       ROUND(100.0 * (MAX(CASE WHEN date_value = (SELECT selected_date FROM target) THEN q_in END)
                             / NULLIF(AVG(CASE WHEN date_value < (SELECT selected_date FROM target) THEN q_in END), 0) - 1), 1) AS change_pct
                FROM d
            """,
            "viz": {
                "column_settings": {
                    '["name","q_in_today"]': {"column_title": "Lưu lượng về hôm nay (m³/s)"},
                    '["name","q_in_avg7"]': {"column_title": "TB 7 ngày trước (m³/s)"},
                    '["name","change_pct"]': {"column_title": "Tăng giảm (%)"}
                }
            },
            "col": 6, "row": 0, "size_x": 6, "size_y": 4
        },
        {
            "name": "D2-3. Tổng lưu lượng xả & Tỷ lệ qua tổ máy",
            "desc": "Hiệu quả sử dụng nước: Q qua tổ máy phát điện, Q xả tràn và tỷ lệ nước được tận dụng.",
            "display": "scalar",
            "sql": f"""
                {TARGET_DATE_CTE}
                SELECT ROUND(SUM(turbine_flow_m3s)) AS q_turbine,
                       ROUND(SUM(spill_flow_m3s))   AS q_spill,
                       ROUND(100.0 * SUM(turbine_flow_m3s)
                             / NULLIF(SUM(turbine_flow_m3s) + SUM(spill_flow_m3s), 0), 1) AS turbine_share_pct
                FROM gold.v_hydro_daily
                WHERE date_value = (SELECT selected_date FROM target)
            """,
            "viz": {
                "column_settings": {
                    '["name","q_turbine"]': {"column_title": "Xả qua tổ máy (m³/s)"},
                    '["name","q_spill"]': {"column_title": "Xả tràn (m³/s)"},
                    '["name","turbine_share_pct"]': {"column_title": "% Tận dụng phát điện"}
                }
            },
            "col": 12, "row": 0, "size_x": 6, "size_y": 4
        },
        {
            "name": "D2-4. Mức cảnh báo an toàn hồ chứa (3 phân cấp)",
            "desc": "Phân cấp cảnh báo hồ đập (Đỏ: Mực nước cao + lũ dồn | Vàng: Mở cửa xả hoặc dung tích >= 95% | Xanh: An toàn).",
            "display": "scalar",
            "sql": f"""
                {TARGET_DATE_CTE}
                SELECT MAX(alert_level)                       AS overall_level,
                       COUNT_IF(alert_level = 3)              AS n_red,
                       COUNT_IF(alert_level = 2)              AS n_yellow,
                       COUNT_IF(alert_level = 1)              AS n_green,
                       SUM(spill_gates_open)                  AS gates_open
                FROM gold.v_hydro_daily
                WHERE date_value = (SELECT selected_date FROM target)
            """,
            "viz": {
                "column_settings": {
                    '["name","overall_level"]': {"column_title": "Mức cao nhất (1=Xanh, 2=Vàng, 3=Đỏ)"},
                    '["name","n_red"]': {"column_title": "Số hồ Đỏ"},
                    '["name","n_yellow"]': {"column_title": "Số hồ Vàng"},
                    '["name","n_green"]': {"column_title": "Số hồ Xanh"},
                    '["name","gates_open"]': {"column_title": "Tổng cửa xả mở"}
                }
            },
            "col": 18, "row": 0, "size_x": 6, "size_y": 4
        },

        # ======================================================================
        # NHỊP 1: 2 HERO CHARTS CHIẾN LƯỢC (Rows 4-12, Height 9, Width 12 each)
        # ======================================================================
        {
            "name": "D2-5. [Hero 1] Độ nhạy phụ tải theo nhiệt độ (Scatter 13h-15h)",
            "desc": "Mối quan hệ nhiệt độ nắng nóng và nhu cầu phụ tải máy lạnh (cố định khung 13h-15h các ngày làm việc 90 ngày qua).",
            "display": "scatter",
            "sql": f"""
                {TARGET_DATE_CTE},
                hr AS (
                  SELECT l.date_value, l.hour_of_day,
                         SUM(l.load_mw) AS load_mw,
                         SUM(w.temperature_c * l.load_mw) / SUM(l.load_mw) AS temp_c
                  FROM gold.v_load_region_hourly l
                  JOIN gold.v_weather_hourly w
                    ON w.date_value = l.date_value AND w.hour_of_day = l.hour_of_day AND w.region_name = l.region_name
                  JOIN gold.dim_date dd ON dd.date_value = l.date_value
                  CROSS JOIN target
                  WHERE l.hour_of_day BETWEEN 13 AND 15
                    AND NOT dd.is_weekend AND NOT dd.is_holiday
                    AND l.date_value BETWEEN target.selected_date - INTERVAL '90' DAY AND target.selected_date
                  GROUP BY 1, 2
                ),
                d AS (
                  SELECT date_value, AVG(load_mw) AS load_mw, AVG(temp_c) AS temp_c
                  FROM hr GROUP BY 1
                )
                SELECT ROUND(temp_c, 1) AS "Nhiệt độ 13h-15h (°C)",
                       ROUND(load_mw) AS "Phụ tải Quốc gia (MW)",
                       date_value AS "Ngày"
                FROM d
                ORDER BY temp_c
            """,
            "viz": {
                "graph.dimensions": ["Nhiệt độ 13h-15h (°C)"],
                "graph.metrics": ["Phụ tải Quốc gia (MW)"]
            },
            "col": 0, "row": 4, "size_x": 12, "size_y": 9
        },
        {
            "name": "D2-6. [Hero 2] Thước đo mực nước an toàn hồ chứa",
            "desc": "Vị trí mực nước hiện tại so với Mực nước chết (0%) và Mực nước dâng bình thường (100%) kèm % dung tích hữu ích.",
            "display": "table",
            "sql": f"""
                {TARGET_DATE_CTE},
                target_res AS (
                    SELECT COALESCE(
                        NULL [[ , {{{{reservoir}}}} ]],
                        'Hòa Bình'
                    ) AS res_name
                )
                SELECT h.reservoir_name AS "Hồ thủy điện",
                       h.water_level_m AS "Mực nước hiện tại (m)",
                       h.dead_level_m AS "Mực nước chết (m)",
                       h.normal_level_m AS "Mực nước dâng BT (m)",
                       ROUND(100.0 * (h.water_level_m - h.dead_level_m)
                             / NULLIF(h.normal_level_m - h.dead_level_m, 0), 1) AS "Vị trí cột nước (%)",
                       h.storage_pct AS "Dung tích hữu ích (%)",
                       h.alert_label AS "Trạng thái an toàn"
                FROM gold.v_hydro_daily h
                CROSS JOIN target
                CROSS JOIN target_res
                WHERE h.date_value = target.selected_date AND h.reservoir_name = target_res.res_name
            """,
            "viz": {
                "table.pivot": False
            },
            "tag_def": TAGS_DATE_AND_RESERVOIR,
            "has_reservoir_filter": True,
            "col": 12, "row": 4, "size_x": 12, "size_y": 9
        },

        # ======================================================================
        # NHỊP 2: CUỘN XUỐNG - CÂN BẰNG THỦY VĂN & BỨC XẠ (Rows 13-20, Height 8)
        # ======================================================================
        {
            "name": "D2-7. Cân bằng thủy văn hồ được chọn (30 ngày)",
            "desc": "Diễn biến Q về, Q qua tổ máy, Q xả tràn và lượng mưa lưu vực 30 ngày qua của hồ được chọn.",
            "display": "line",
            "sql": f"""
                {TARGET_DATE_CTE},
                target_res AS (
                    SELECT COALESCE(
                        NULL [[ , {{{{reservoir}}}} ]],
                        'Hòa Bình'
                    ) AS res_name
                ),
                rain AS (
                  SELECT date_value, SUM(precipitation_mm) AS rain_mm
                  FROM gold.v_weather_hourly
                  WHERE region_name = 'Bắc'
                  GROUP BY 1
                )
                SELECT h.date_value AS "Ngày",
                       h.inflow_m3s AS "Q về (m³/s)",
                       h.turbine_flow_m3s AS "Q qua tổ máy (m³/s)",
                       h.spill_flow_m3s AS "Q xả tràn (m³/s)",
                       COALESCE(r.rain_mm, 0) AS "Lượng mưa lưu vực (mm)"
                FROM gold.v_hydro_daily h
                LEFT JOIN rain r ON h.date_value = r.date_value
                CROSS JOIN target
                CROSS JOIN target_res
                WHERE h.reservoir_name = target_res.res_name
                  AND h.date_value BETWEEN target.selected_date - INTERVAL '29' DAY AND target.selected_date
                ORDER BY 1
            """,
            "viz": {
                "graph.dimensions": ["Ngày"],
                "graph.metrics": [
                    "Q về (m³/s)",
                    "Q qua tổ máy (m³/s)",
                    "Q xả tràn (m³/s)",
                    "Lượng mưa lưu vực (mm)"
                ],
                "series_settings": {
                    "Q về (m³/s)": {"color": "#3B82F6"},
                    "Q qua tổ máy (m³/s)": {"color": "#10B981"},
                    "Q xả tràn (m³/s)": {"color": "#EF4444"},
                    "Lượng mưa lưu vực (mm)": {"display": "bar", "color": "#60A5FA"}
                }
            },
            "tag_def": TAGS_DATE_AND_RESERVOIR,
            "has_reservoir_filter": True,
            "col": 0, "row": 13, "size_x": 14, "size_y": 8
        },
        {
            "name": "D2-8. Bức xạ mặt trời vs Công suất điện mặt trời (Scatter)",
            "desc": "Kiểm tra độ tương quan bức xạ - công suất mặt trời và phát hiện các giờ công suất thấp bất thường.",
            "display": "scatter",
            "sql": f"""
                {TARGET_DATE_CTE},
                anchor AS (
                    SELECT CASE 
                        WHEN (SELECT selected_date FROM target) <= DATE '2024-06-03'
                        THEN (SELECT selected_date FROM target)
                        ELSE DATE '2024-06-03'
                    END AS ref_date
                ),
                s AS (
                  SELECT g.date_value, g.hour_of_day, g.generation_mw AS solar_mw,
                         AVG(w.radiation_wm2) AS rad_wm2
                  FROM gold.v_gen_hourly g
                  CROSS JOIN anchor
                  JOIN gold.v_weather_hourly w
                    ON w.date_value = g.date_value AND w.hour_of_day = g.hour_of_day
                   AND w.region_name IN ('Trung', 'Nam')
                   AND w.date_value BETWEEN anchor.ref_date - INTERVAL '30' DAY AND anchor.ref_date
                  WHERE g.source_group = 'Mặt trời'
                    AND g.date_value BETWEEN anchor.ref_date - INTERVAL '30' DAY AND anchor.ref_date
                    AND w.radiation_wm2 > 50
                  GROUP BY 1, 2, 3
                ),
                ref AS (
                  SELECT APPROX_PERCENTILE(solar_mw / rad_wm2, 0.5) AS median_ratio
                  FROM s WHERE rad_wm2 >= 500
                )
                SELECT ROUND(s.rad_wm2) AS "Bức xạ mặt trời (W/m²)",
                       ROUND(s.solar_mw) AS "Công suất phát (MW)",
                       s.date_value AS "Ngày",
                       s.hour_of_day AS "Giờ",
                       CASE WHEN (s.rad_wm2 >= 500 AND s.solar_mw / s.rad_wm2 < 0.8 * ref.median_ratio) 
                            THEN 'Nghi vấn phát thấp' 
                            ELSE 'Bình thường' 
                       END AS "Trạng thái"
                FROM s CROSS JOIN ref
                ORDER BY 1, 2
            """,
            "viz": {
                "graph.dimensions": ["Bức xạ mặt trời (W/m²)"],
                "graph.metrics": ["Công suất phát (MW)"]
            },
            "col": 14, "row": 13, "size_x": 10, "size_y": 8
        },

        # ======================================================================
        # NHỊP 2: MA TRẬN GIÁM SÁT HỒ CHỨA THEO LƯU VỰC (Rows 21-28, Height 8, Width 24)
        # ======================================================================
        {
            "name": "D2-9. Ma trận giám sát an toàn hồ chứa theo lưu vực",
            "desc": "Bức tranh tổng thể an toàn liên hồ chứa trên 8 lưu vực sông chính, phân cấp cảnh báo trực quan.",
            "display": "table",
            "sql": f"""
                {TARGET_DATE_CTE}
                SELECT basin_name AS "Lưu vực sông",
                       reservoir_name AS "Hồ thủy điện",
                       water_level_m AS "Mực nước (m)",
                       storage_pct AS "Dung tích hữu ích (%)",
                       ROUND(inflow_m3s) AS "Q về (m³/s)",
                       ROUND(turbine_flow_m3s) AS "Q tổ máy (m³/s)",
                       ROUND(spill_flow_m3s) AS "Q xả tràn (m³/s)",
                       spill_gates_open AS "Cửa xả mở",
                       alert_label AS "Trạng thái an toàn"
                FROM gold.v_hydro_daily
                WHERE date_value = (SELECT selected_date FROM target)
                ORDER BY basin_name, alert_level DESC, storage_pct DESC
            """,
            "viz": {
                "table.pivot": False
            },
            "col": 0, "row": 21, "size_x": 24, "size_y": 8
        }
    ]

    # Create/update cards
    for card in cards_def:
        tag_def = card.get("tag_def", COMMON_TAG_DEF)
        cid = upsert_card(
            url, token, col_id, db_id,
            card["name"], card["sql"], card["display"],
            card.get("viz"), tag_def, card.get("desc")
        )
        card["card_id"] = cid

    # Custom parameter mappings
    card_specs = []
    for c in cards_def:
        mappings = [
            {
                "parameter_id": "date_filter",
                "card_id": c["card_id"],
                "target": ["variable", ["template-tag", "selected_date"]]
            }
        ]
        if c.get("has_reservoir_filter", False):
            mappings.append({
                "parameter_id": "reservoir_filter",
                "card_id": c["card_id"],
                "target": ["variable", ["template-tag", "reservoir"]]
            })
        c["parameter_mappings"] = mappings
        card_specs.append(c)

    set_dashboard_layout(
        url, token, dash_id, card_specs,
        parameters=DASHBOARD_PARAMETERS
    )
    print(f"\n[SUCCESS] Deployed {len(cards_def)} cards on {DASHBOARD_NAME} (Dashboard ID: {dash_id})!")
    print(f"URL: {url}/dashboard/{dash_id}\n")


if __name__ == "__main__":
    args = parse_common_args(DASHBOARD_NAME)
    deploy_dashboard_02(args.url, args.user, args.password)
