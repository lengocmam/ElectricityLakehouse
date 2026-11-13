# -*- coding: utf-8 -*-
"""Dashboard 01: Cân Bằng Cung - Cầu & Chuyển Dịch Năng Lượng.

Storytelling Architecture:
- Thông điệp: Nhịp vận hành 24 giờ của hệ thống điện: phụ tải được đáp ứng bằng nguồn nào,
  và điện mặt trời làm thay đổi hình dạng phụ tải ròng ra sao.
- Nguồn kết hợp: Phụ tải NSMO × Cơ cấu phát NSMO (theo giờ) × Bức xạ/Gió Open-Meteo × Thủy điện EVN.
- Bộ lọc: {{selected_date}} (Mặc định tự động chọn ngày mới nhất).
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
    parse_common_args,
    set_dashboard_layout,
    upsert_card,
    COMMON_TAG_DEF,
)

DASHBOARD_NAME = "01. Cân Bằng Cung - Cầu & Chuyển Dịch Năng Lượng"
DASHBOARD_DESC = (
    "Nhịp vận hành 24 giờ của hệ thống điện: phụ tải được đáp ứng bằng nguồn nào, "
    "và điện mặt trời làm thay đổi hình dạng phụ tải ròng ra sao."
)

TARGET_DATE_CTE = """
WITH target AS (
    SELECT COALESCE(
        NULL [[ , CAST({{selected_date}} AS DATE) ]],
        DATE '2026-10-10'
    ) AS selected_date
)
""".strip()


def deploy_dashboard_01(base_url=None, user=None, password=None):
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
        # NHỊP 1: MÀN HÌNH ĐẦU - 4 KPIs VẬN HÀNH HỆ THỐNG (Rows 0-3, Height 4, Width 6)
        # ======================================================================
        {
            "name": "D1-1. Phụ tải đỉnh ngày (Pmax) & Dự phòng công suất",
            "desc": "Công suất đỉnh Pmax, giờ đỉnh, dự phòng công suất điều khiển được (không tính mặt trời/gió).",
            "display": "scalar",
            "sql": f"""
                {TARGET_DATE_CTE},
                p AS (
                  SELECT MAX(load_mw) AS pmax, MAX_BY(hour_of_day, load_mw) AS peak_hour
                  FROM gold.v_load_national_hourly
                  WHERE date_value = (SELECT selected_date FROM target)
                ),
                c AS (
                  SELECT SUM(available_mw) AS avail_mw
                  FROM gold.v_capacity_daily
                  WHERE date_value = (SELECT selected_date FROM target)
                    AND source_group NOT IN ('Mặt trời', 'Gió')
                )
                SELECT ROUND(pmax) AS pmax_mw, peak_hour,
                       ROUND(avail_mw - pmax) AS headroom_mw,
                       ROUND(100.0 * (avail_mw - pmax) / pmax, 1) AS headroom_pct
                FROM p CROSS JOIN c
            """,
            "viz": {
                "column_settings": {
                    '["name","pmax_mw"]': {"column_title": "Pmax Đỉnh (MW)"},
                    '["name","peak_hour"]': {"column_title": "Giờ Đỉnh (h)"},
                    '["name","headroom_mw"]': {"column_title": "Dự phòng (MW)"},
                    '["name","headroom_pct"]': {"column_title": "Dự phòng (%)"}
                }
            },
            "col": 0, "row": 0, "size_x": 6, "size_y": 4
        },
        {
            "name": "D1-2. Phụ tải đáy (Pmin) & Biên độ ΔP",
            "desc": "Đo độ co giãn phụ tải trong ngày: Pmin, giờ đáy, và biên độ chênh lệch Pmax - Pmin.",
            "display": "scalar",
            "sql": f"""
                {TARGET_DATE_CTE}
                SELECT ROUND(MIN(load_mw)) AS pmin_mw,
                       MIN_BY(hour_of_day, load_mw) AS trough_hour,
                       ROUND(MAX(load_mw) - MIN(load_mw)) AS delta_p_mw
                FROM gold.v_load_national_hourly
                WHERE date_value = (SELECT selected_date FROM target)
            """,
            "viz": {
                "column_settings": {
                    '["name","pmin_mw"]': {"column_title": "Pmin Đáy (MW)"},
                    '["name","trough_hour"]': {"column_title": "Giờ Đáy (h)"},
                    '["name","delta_p_mw"]': {"column_title": "Biên độ ΔP (MW)"}
                }
            },
            "col": 6, "row": 0, "size_x": 6, "size_y": 4
        },
        {
            "name": "D1-3. Tốc độ tăng dốc phụ tải ròng chiều tối (Max Ramp)",
            "desc": "Áp lực 'cổ vịt': Tốc độ tăng dốc phụ tải ròng lớn nhất trong cửa sổ 3 giờ (14h-20h) khi điện mặt trời tắt.",
            "display": "scalar",
            "sql": f"""
                {TARGET_DATE_CTE},
                n AS (
                  SELECT hour_of_day, load_mw FROM gold.v_load_national_hourly
                  WHERE date_value = (SELECT selected_date FROM target)
                ),
                re AS (
                  SELECT hour_of_day, SUM(generation_mw) AS re_mw
                  FROM gold.v_gen_hourly
                  WHERE date_value = (SELECT selected_date FROM target) AND source_group IN ('Mặt trời', 'Gió')
                  GROUP BY 1
                ),
                net AS (
                  SELECT n.hour_of_day, n.load_mw - COALESCE(re.re_mw, 0) AS net_mw
                  FROM n LEFT JOIN re ON n.hour_of_day = re.hour_of_day
                ),
                r AS (
                  SELECT hour_of_day,
                         (LEAD(net_mw, 3) OVER (ORDER BY hour_of_day) - net_mw) / 3.0 AS ramp_mw_h
                  FROM net
                )
                SELECT ROUND(MAX(ramp_mw_h)) AS max_ramp_mw_per_h,
                       MAX_BY(hour_of_day, ramp_mw_h) AS start_hour
                FROM r
                WHERE hour_of_day BETWEEN 14 AND 20 AND ramp_mw_h IS NOT NULL
            """,
            "viz": {
                "column_settings": {
                    '["name","max_ramp_mw_per_h"]': {"column_title": "Tốc độ dốc Ramp (MW/h)"},
                    '["name","start_hour"]': {"column_title": "Giờ bắt đầu dốc"}
                }
            },
            "col": 12, "row": 0, "size_x": 6, "size_y": 4
        },
        {
            "name": "D1-4. Tỷ trọng Năng lượng Tái tạo (Mặt trời + Gió)",
            "desc": "Mức đóng góp của năng lượng tái tạo: %RE cả ngày và %RE cao nhất theo giờ kèm giờ đạt đỉnh.",
            "display": "scalar",
            "sql": f"""
                {TARGET_DATE_CTE},
                h AS (
                  SELECT hour_of_day,
                         SUM(generation_mw) AS total_mw,
                         SUM(CASE WHEN source_group IN ('Mặt trời', 'Gió') THEN generation_mw ELSE 0 END) AS re_mw
                  FROM gold.v_gen_hourly
                  WHERE date_value = (SELECT selected_date FROM target)
                  GROUP BY 1
                )
                SELECT ROUND(100.0 * SUM(re_mw) / SUM(total_mw), 1) AS re_day_pct,
                       ROUND(MAX(100.0 * re_mw / total_mw), 1)      AS re_peak_hour_pct,
                       MAX_BY(hour_of_day, 1.0 * re_mw / total_mw)   AS re_peak_hour
                FROM h
            """,
            "viz": {
                "column_settings": {
                    '["name","re_day_pct"]': {"column_title": "% RE Cả Ngày"},
                    '["name","re_peak_hour_pct"]': {"column_title": "% RE Giờ Đỉnh"},
                    '["name","re_peak_hour"]': {"column_title": "Giờ RE Đỉnh"}
                }
            },
            "col": 18, "row": 0, "size_x": 6, "size_y": 4
        },

        # ======================================================================
        # NHỊP 1: 2 HERO CHARTS CHIẾN LƯỢC (Rows 4-12, Height 9, Width 12 each)
        # ======================================================================
        {
            "name": "D1-5. [Hero 1] Đường cong con vịt (Duck Curve 24h)",
            "desc": "Lượng hóa tác động của điện mặt trời: Phụ tải thực tế, Điện mặt trời, Điện gió và Phụ tải ròng (Net Load).",
            "display": "line",
            "sql": f"""
                {TARGET_DATE_CTE},
                n AS (
                  SELECT hour_of_day, load_mw FROM gold.v_load_national_hourly
                  WHERE date_value = (SELECT selected_date FROM target)
                ),
                g AS (
                  SELECT hour_of_day,
                         SUM(CASE WHEN source_group = 'Mặt trời' THEN generation_mw END) AS solar_mw,
                         SUM(CASE WHEN source_group = 'Gió'      THEN generation_mw END) AS wind_mw
                  FROM gold.v_gen_hourly
                  WHERE date_value = (SELECT selected_date FROM target)
                  GROUP BY 1
                )
                SELECT n.hour_of_day AS "Giờ trong ngày",
                       ROUND(n.load_mw, 1) AS "Phụ tải Quốc gia (MW)",
                       ROUND(n.load_mw - COALESCE(g.solar_mw, 0) - COALESCE(g.wind_mw, 0), 1) AS "Phụ tải Ròng - Net Load (MW)",
                       ROUND(COALESCE(g.solar_mw, 0), 1) AS "Điện Mặt trời (MW)",
                       ROUND(COALESCE(g.wind_mw, 0), 1) AS "Điện Gió (MW)"
                FROM n LEFT JOIN g ON n.hour_of_day = g.hour_of_day
                ORDER BY n.hour_of_day
            """,
            "viz": {
                "graph.dimensions": ["Giờ trong ngày"],
                "graph.metrics": [
                    "Phụ tải Quốc gia (MW)",
                    "Phụ tải Ròng - Net Load (MW)",
                    "Điện Mặt trời (MW)",
                    "Điện Gió (MW)"
                ],
                "graph.series_order": [
                    "Phụ tải Quốc gia (MW)",
                    "Phụ tải Ròng - Net Load (MW)",
                    "Điện Mặt trời (MW)",
                    "Điện Gió (MW)"
                ],
                "graph.colors": {
                    "Phụ tải Quốc gia (MW)": "#509EE3",
                    "Phụ tải Ròng - Net Load (MW)": "#FF9900",
                    "Điện Mặt trời (MW)": "#ED6E33",
                    "Điện Gió (MW)": "#7FCA9B"
                }
            },
            "col": 0, "row": 4, "size_x": 12, "size_y": 9
        },
        {
            "name": "D1-6. [Hero 2] Cân bằng huy động 24 giờ: Ai đáp ứng phụ tải",
            "desc": "Cơ cấu huy động nguồn theo giờ đáp ứng phụ tải: Than, Khí, Thủy điện, Mặt trời, Gió, Khác kèm đường Phụ tải & Chênh lệch.",
            "display": "area",
            "sql": f"""
                {TARGET_DATE_CTE},
                g_day AS (
                    SELECT hour_of_day, source_group, generation_mw
                    FROM gold.v_gen_hourly
                    WHERE date_value = (SELECT selected_date FROM target)
                ),
                l_day AS (
                    SELECT hour_of_day, load_mw
                    FROM gold.v_load_national_hourly
                    WHERE date_value = (SELECT selected_date FROM target)
                ),
                g_tot AS (
                    SELECT hour_of_day, SUM(generation_mw) AS total_mw
                    FROM g_day
                    GROUP BY hour_of_day
                )
                SELECT hour_of_day AS "Giờ", source_group AS "Nguồn", generation_mw AS "Công suất (MW)"
                FROM g_day
                UNION ALL
                SELECT hour_of_day AS "Giờ", 'Phụ tải' AS "Nguồn", load_mw AS "Công suất (MW)"
                FROM l_day
                UNION ALL
                SELECT l.hour_of_day AS "Giờ", 'Chênh lệch (Phụ tải − Tổng phát)' AS "Nguồn", ROUND(l.load_mw - g.total_mw, 1) AS "Công suất (MW)"
                FROM l_day l
                JOIN g_tot g ON l.hour_of_day = g.hour_of_day
                ORDER BY 1, 2
            """,
            "viz": {
                "graph.dimensions": ["Giờ", "Nguồn"],
                "graph.metrics": ["Công suất (MW)"],
                "series_settings": {
                    "Phụ tải": {"display": "line", "color": "#1F2937"},
                    "Chênh lệch (Phụ tải − Tổng phát)": {"display": "line", "color": "#EF4444"},
                    "Than": {"color": "#4B5563"},
                    "Khí": {"color": "#F59E0B"},
                    "Thủy điện": {"color": "#3B82F6"},
                    "Mặt trời": {"color": "#EAB308"},
                    "Gió": {"color": "#10B981"},
                    "Khác": {"color": "#8B5CF6"}
                }
            },
            "col": 12, "row": 4, "size_x": 12, "size_y": 9
        },

        # ======================================================================
        # NHỊP 2: CUỘN XUỐNG - CHUYỂN DỊCH THEO MÙA & 3 MIỀN (Rows 13-20, Height 8)
        # ======================================================================
        {
            "name": "D1-7. Cơ cấu sản lượng theo tháng (12 tháng gần nhất)",
            "desc": "Chuyển dịch giữa mùa mưa và mùa khô: Tỷ trọng Thủy điện, Than, Khí, Tái tạo 12 tháng qua.",
            "display": "bar",
            "sql": f"""
                {TARGET_DATE_CTE}
                SELECT date_trunc('month', data_date) AS "Tháng",
                       CASE 
                           WHEN LOWER(energy_source) LIKE '%than%' THEN 'Than'
                           WHEN LOWER(energy_source) LIKE '%khí%' OR LOWER(energy_source) LIKE '%gas%' THEN 'Khí'
                           WHEN LOWER(energy_source) LIKE '%thủy điện%' THEN 'Thủy điện'
                           WHEN LOWER(energy_source) LIKE '%mặt trời%' OR LOWER(energy_source) LIKE '%đmt%' THEN 'Mặt trời'
                           WHEN LOWER(energy_source) LIKE '%gió%' THEN 'Gió'
                           ELSE 'Khác'
                       END AS "Nhóm nguồn",
                       ROUND(SUM(generation_million_kwh), 1) AS "Sản lượng (GWh)"
                FROM iceberg.silver.generation
                CROSS JOIN target
                WHERE energy_source NOT LIKE '%thương phẩm%'
                  AND data_date >= date_add('month', -12, target.selected_date)
                  AND data_date <= target.selected_date
                GROUP BY 1, 2
                ORDER BY 1, 2
            """,
            "viz": {
                "graph.dimensions": ["Tháng", "Nhóm nguồn"],
                "graph.metrics": ["Sản lượng (GWh)"],
                "stackable.stack_type": "normalized"
            },
            "col": 0, "row": 13, "size_x": 10, "size_y": 8
        },
        {
            "name": "D1-8. Phụ tải 3 miền Bắc - Trung - Nam theo giờ",
            "desc": "So sánh hình dạng tiêu thụ và độ lệch pha giờ đỉnh giữa 3 miền Bắc, Trung, Nam.",
            "display": "area",
            "sql": f"""
                {TARGET_DATE_CTE}
                SELECT hour_of_day AS "Giờ",
                       region_name AS "Miền",
                       ROUND(load_mw, 1) AS "Phụ tải (MW)"
                FROM gold.v_load_region_hourly
                WHERE date_value = (SELECT selected_date FROM target)
                ORDER BY 1, 2
            """,
            "viz": {
                "graph.dimensions": ["Giờ", "Miền"],
                "graph.metrics": ["Phụ tải (MW)"],
                "series_settings": {
                    "Bắc": {"color": "#EF4444"},
                    "Trung": {"color": "#F59E0B"},
                    "Nam": {"color": "#3B82F6"}
                }
            },
            "col": 10, "row": 13, "size_x": 14, "size_y": 8
        },

        # ======================================================================
        # NHỊP 2: BẢNG KIỂM TOÁN ĐIỀU ĐỘ CHI TIẾT (Rows 21-28, Height 8, Width 24)
        # ======================================================================
        {
            "name": "D1-9. Bảng kiểm toán điều độ theo loại hình nguồn",
            "desc": "Số liệu kiểm toán cuối ngày: Sản lượng, Tỷ trọng, Pmax, Giờ đạt Pmax, Biên độ dao động, Hệ số sử dụng CS, Vai trò và Nhóm phát thải.",
            "display": "table",
            "sql": f"""
                {TARGET_DATE_CTE},
                g AS (
                  SELECT source_group, hour_of_day, generation_mw
                  FROM gold.v_gen_hourly WHERE date_value = (SELECT selected_date FROM target)
                ),
                a AS (
                  SELECT source_group,
                         SUM(generation_mw)                      AS mwh,
                         MAX(generation_mw)                      AS pmax_mw,
                         MAX_BY(hour_of_day, generation_mw)      AS pmax_hour,
                         MAX(generation_mw) - MIN(generation_mw) AS swing_mw
                  FROM g GROUP BY 1
                ),
                c AS (
                  SELECT source_group, SUM(capacity_mw) AS capacity_mw
                  FROM gold.v_capacity_daily WHERE date_value = (SELECT selected_date FROM target) GROUP BY 1
                )
                SELECT a.source_group AS "Nguồn phát",
                       ROUND(a.mwh) AS "Sản lượng (MWh)",
                       ROUND(100.0 * a.mwh / SUM(a.mwh) OVER (), 1) AS "Tỷ trọng (%)",
                       ROUND(a.pmax_mw) AS "Pmax (MW)",
                       a.pmax_hour AS "Giờ Pmax",
                       ROUND(a.swing_mw) AS "Biên độ ΔP (MW)",
                       ROUND(100.0 * a.mwh / NULLIF(c.capacity_mw * 24, 0), 1) AS "Hệ số sử dụng (%)",
                       CASE a.source_group
                         WHEN 'Than' THEN 'Nền' 
                         WHEN 'Khí' THEN 'Linh hoạt'
                         WHEN 'Thủy điện' THEN 'Điều tần / Đỉnh'
                         WHEN 'Mặt trời' THEN 'Biến thiên' 
                         WHEN 'Gió' THEN 'Biến thiên'
                         ELSE 'Bổ sung' 
                       END AS "Vai trò điều độ",
                       CASE 
                         WHEN a.source_group IN ('Than', 'Khí') THEN 'Hóa thạch'
                         WHEN a.source_group IN ('Thủy điện', 'Mặt trời', 'Gió') THEN 'Tái tạo'
                         ELSE 'Khác' 
                       END AS "Nhóm phát thải"
                FROM a LEFT JOIN c ON a.source_group = c.source_group
                ORDER BY a.mwh DESC
            """,
            "viz": {
                "table.pivot": False
            },
            "col": 0, "row": 21, "size_x": 24, "size_y": 8
        }
    ]

    # Create/update cards
    for card in cards_def:
        cid = upsert_card(
            url, token, col_id, db_id,
            card["name"], card["sql"], card["display"],
            card.get("viz"), COMMON_TAG_DEF, card.get("desc")
        )
        card["card_id"] = cid

    # Update dashboard layout and date filter
    set_dashboard_layout(url, token, dash_id, cards_def)
    print(f"\n[SUCCESS] Deployed {len(cards_def)} cards on {DASHBOARD_NAME} (Dashboard ID: {dash_id})!")
    print(f"URL: {url}/dashboard/{dash_id}\n")


if __name__ == "__main__":
    args = parse_common_args(DASHBOARD_NAME)
    deploy_dashboard_01(args.url, args.user, args.password)
