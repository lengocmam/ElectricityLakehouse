# -*- coding: utf-8 -*-
"""Dashboard 01: Cân Bằng Cung - Cầu & Bù Trừ Năng Lượng Tái Tạo.

Phân tích hiện tượng Duck Curve (Đường cong con vịt), tốc độ tăng tải chiều tối
và vai trò Thủy điện làm Pin tích năng quốc gia bù đắp khi điện mặt trời tắt.
"""
import sys
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

DASHBOARD_NAME = "01. Cân Bằng Cung - Cầu & Bù Trừ Năng Lượng Tái Tạo"
DASHBOARD_DESC = "Phân tích hiện tượng Duck Curve, độ dốc tăng tải khi mặt trời lặn và vai trò Thủy điện làm Pin tích năng dự phòng."
CTE = get_target_date_cte("iceberg.gold.fact_generation_daily")

def deploy_dashboard_01(base_url=None, user=None, password=None):
    url = get_metabase_url(base_url)
    print(f"\n" + "="*70)
    print(f"DEPLOYING {DASHBOARD_NAME}")
    print(f"Target Metabase: {url}")
    print("="*70)

    token = authenticate(url, user or "admin@lakehouse.vn", password or "Lakehouse@2026")
    db_id = get_database_id(url, token)
    col_id = get_or_create_collection(url, token)
    dash_id = get_or_create_dashboard(url, token, col_id, DASHBOARD_NAME, DASHBOARD_DESC)

    cards_def = [
        # Nhịp 1: 4 KPIs hàng trên (Rows 0-3)
        {
            "name": "Phụ tải Đỉnh Toàn Quốc Pmax (MW)",
            "display": "scalar",
            "sql": f"""
                {CTE}
                SELECT ROUND(MAX(hourly_load), 2) AS peak_mw 
                FROM (
                    SELECT l.time_key, SUM(l.load_mw) AS hourly_load 
                    FROM iceberg.gold.fact_load_hourly l
                    JOIN target_date td ON l.date_key = td.date_key
                    GROUP BY l.time_key
                )
            """,
            "viz": {}, "col": 0, "row": 0, "size_x": 6, "size_y": 4
        },
        {
            "name": "Tỷ trọng Năng lượng Tái tạo (RE%)",
            "display": "scalar",
            "sql": f"""
                {CTE}
                SELECT ROUND(SUM(CASE WHEN s.renewable_flag = true THEN g.generation_mwh ELSE 0 END) * 100.0 / NULLIF(SUM(g.generation_mwh), 0), 2) AS re_pct 
                FROM iceberg.gold.fact_generation_daily g 
                JOIN target_date td ON g.date_key = td.date_key
                JOIN iceberg.gold.dim_energy_source s ON g.energy_source_key = s.energy_source_key
            """,
            "viz": {}, "col": 6, "row": 0, "size_x": 6, "size_y": 4
        },
        {
            "name": "Tốc độ Tăng tải Chiều tối (Max Ramp Rate MW/h)",
            "display": "scalar",
            "sql": f"""
                {CTE}
                SELECT ROUND(MAX(next_load - curr_load), 0) AS max_ramp_mw_h 
                FROM (
                    SELECT 
                        l.time_key, 
                        SUM(l.load_mw) AS curr_load, 
                        LEAD(SUM(l.load_mw)) OVER (ORDER BY l.time_key) AS next_load 
                    FROM iceberg.gold.fact_load_hourly l
                    JOIN target_date td ON l.date_key = td.date_key
                    GROUP BY l.time_key
                )
            """,
            "viz": {}, "col": 12, "row": 0, "size_x": 6, "size_y": 4
        },
        {
            "name": "Thủy điện Xả Bù lúc Đỉnh tối (MW lúc 18h-20h)",
            "display": "scalar",
            "sql": f"""
                {CTE}
                SELECT ROUND(AVG(v.avg_hourly_hydro_mw), 0) AS hydro_peak_mw 
                FROM iceberg.gold.v_dashboard1_supply_demand_hourly v
                JOIN target_date td ON v.date_key = td.date_key
                WHERE v.hour BETWEEN 18 AND 20
            """,
            "viz": {}, "col": 18, "row": 0, "size_x": 6, "size_y": 4
        },

        # Nhịp 2: 2 Hero Charts (Rows 4-13)
        {
            "name": "Nhịp Thở 24 Giờ: Nắng Tắt - Thủy Điện Bung Công Suất",
            "display": "line",
            "sql": f"""
                {CTE}
                SELECT 
                    v.hour AS Gio, 
                    ROUND(AVG(v.national_load_mw), 1) AS Phu_Tai_Gross_MW, 
                    ROUND(AVG(v.net_load_mw), 1) AS Phu_Tai_Rong_Net_MW, 
                    ROUND(AVG(v.avg_hourly_solar_mw), 1) AS Solar_MW, 
                    ROUND(AVG(v.avg_hourly_hydro_mw), 1) AS Thuy_Dien_MW 
                FROM iceberg.gold.v_dashboard1_supply_demand_hourly v
                JOIN target_date td ON v.date_key = td.date_key
                GROUP BY v.hour 
                ORDER BY v.hour
            """,
            "viz": {
                "graph.dimensions": ["Gio"],
                "graph.metrics": ["Phu_Tai_Gross_MW", "Phu_Tai_Rong_Net_MW", "Solar_MW", "Thuy_Dien_MW"],
                "graph.x_axis.scale": "linear",
                "graph.y_axis.title_text": "Công suất điều độ (MW)",
                "graph.x_axis.title_text": "Giờ trong ngày (0-23h)",
                "column_settings": {
                    '["name","Gio"]': {"column_title": "Giờ"},
                    '["name","Phu_Tai_Gross_MW"]': {"column_title": "1. Tổng phụ tải (Gross Load)"},
                    '["name","Phu_Tai_Rong_Net_MW"]': {"column_title": "2. Phụ tải ròng (Net Load)"},
                    '["name","Solar_MW"]': {"column_title": "3. Điện mặt trời (Solar)"},
                    '["name","Thuy_Dien_MW"]': {"column_title": "4. Thủy điện xả bù (Hydro)"}
                }
            },
            "col": 0, "row": 4, "size_x": 12, "size_y": 10
        },
        {
            "name": "Đường Cong Con Vịt & Áp Lực Huy Động Nguồn Nhanh (Duck Curve)",
            "display": "line",
            "sql": f"""
                {CTE}
                SELECT 
                    v.hour AS Gio, 
                    ROUND(AVG(v.national_load_mw), 1) AS Gross_Load_MW, 
                    ROUND(AVG(v.net_load_mw), 1) AS Net_Load_MW 
                FROM iceberg.gold.v_dashboard1_supply_demand_hourly v
                JOIN target_date td ON v.date_key = td.date_key
                GROUP BY v.hour 
                ORDER BY v.hour
            """,
            "viz": {
                "graph.dimensions": ["Gio"],
                "graph.metrics": ["Gross_Load_MW", "Net_Load_MW"],
                "graph.x_axis.scale": "linear",
                "graph.y_axis.title_text": "Công suất (MW)",
                "graph.x_axis.title_text": "Giờ trong ngày",
                "column_settings": {
                    '["name","Gio"]': {"column_title": "Giờ"},
                    '["name","Gross_Load_MW"]': {"column_title": "Tổng phụ tải (Gross Load)"},
                    '["name","Net_Load_MW"]': {"column_title": "Phụ tải ròng (Net Load - Bụng con vịt)"}
                }
            },
            "col": 12, "row": 4, "size_x": 12, "size_y": 10
        },

        # Nhịp 3: Cơ cấu nguồn phát & Bảng điều độ (Rows 14-31)
        {
            "name": "Cơ Cấu Nguồn Phát 24 Giờ Theo Thời Gian Thực",
            "display": "area",
            "sql": f"""
                {CTE}
                SELECT 
                    v.hour AS Gio, 
                    ROUND(AVG(v.avg_hourly_coal_mw), 0) AS Than_BaseLoad, 
                    ROUND(AVG(v.avg_hourly_gas_mw), 0) AS Khi_FlexLoad, 
                    ROUND(AVG(v.avg_hourly_hydro_mw), 0) AS Thuy_Dien_PeakLoad, 
                    ROUND(AVG(v.avg_hourly_solar_mw), 0) AS Dien_Mat_Troi, 
                    ROUND(AVG(v.avg_hourly_wind_mw), 0) AS Dien_Gio 
                FROM iceberg.gold.v_dashboard1_supply_demand_hourly v
                JOIN target_date td ON v.date_key = td.date_key
                GROUP BY v.hour 
                ORDER BY v.hour
            """,
            "viz": {
                "graph.dimensions": ["Gio"],
                "graph.metrics": ["Than_BaseLoad", "Khi_FlexLoad", "Thuy_Dien_PeakLoad", "Dien_Mat_Troi", "Dien_Gio"],
                "graph.x_axis.scale": "linear",
                "stackable.stack_type": "stacked",
                "graph.y_axis.title_text": "Công suất phát (MW)",
                "graph.x_axis.title_text": "Giờ trong ngày",
                "column_settings": {
                    '["name","Gio"]': {"column_title": "Giờ"},
                    '["name","Than_BaseLoad"]': {"column_title": "Than (Chạy nền)"},
                    '["name","Khi_FlexLoad"]': {"column_title": "Khí (Linh hoạt)"},
                    '["name","Thuy_Dien_PeakLoad"]': {"column_title": "Thủy điện (Gọt đỉnh)"},
                    '["name","Dien_Mat_Troi"]': {"column_title": "Mặt trời (Tái tạo)"},
                    '["name","Dien_Gio"]': {"column_title": "Gió (Tái tạo)"}
                }
            },
            "col": 0, "row": 14, "size_x": 12, "size_y": 10
        },
        {
            "name": "Cơ Cấu Nguồn Phát 12 Tháng Trong Năm",
            "display": "bar",
            "sql": f"""
                {CTE}
                SELECT 
                    d.month AS Thang, 
                    ROUND(SUM(CASE WHEN LOWER(s.source_code) = 'hydro' THEN g.generation_mwh ELSE 0 END), 0) AS Thuy_Dien, 
                    ROUND(SUM(CASE WHEN LOWER(s.source_code) = 'coal' THEN g.generation_mwh ELSE 0 END), 0) AS Nhiet_Dien_Than, 
                    ROUND(SUM(CASE WHEN LOWER(s.source_code) = 'solar' THEN g.generation_mwh ELSE 0 END), 0) AS Mat_Troi, 
                    ROUND(SUM(CASE WHEN LOWER(s.source_code) = 'wind' THEN g.generation_mwh ELSE 0 END), 0) AS Gio 
                FROM iceberg.gold.fact_generation_daily g 
                JOIN iceberg.gold.dim_energy_source s ON g.energy_source_key = s.energy_source_key 
                JOIN iceberg.gold.dim_date d ON g.date_key = d.date_key 
                JOIN target_date td ON d.date_key / 10000 = td.date_key / 10000
                GROUP BY d.month 
                ORDER BY d.month
            """,
            "viz": {
                "graph.dimensions": ["Thang"],
                "graph.metrics": ["Thuy_Dien", "Nhiet_Dien_Than", "Mat_Troi", "Gio"],
                "stackable.stack_type": "stacked",
                "graph.y_axis.title_text": "Sản lượng (MWh)",
                "graph.x_axis.title_text": "Tháng",
                "column_settings": {
                    '["name","Thang"]': {"column_title": "Tháng"},
                    '["name","Thuy_Dien"]': {"column_title": "Thủy điện (MWh)"},
                    '["name","Nhiet_Dien_Than"]': {"column_title": "Nhiệt điện Than (MWh)"},
                    '["name","Mat_Troi"]': {"column_title": "Điện Mặt trời (MWh)"},
                    '["name","Gio"]': {"column_title": "Điện Gió (MWh)"}
                }
            },
            "col": 12, "row": 14, "size_x": 12, "size_y": 10
        },
        {
            "name": "Bảng Chi Tiết Điều Độ Nguồn Phát & Vai Trò Lưới Điện",
            "display": "table",
            "sql": f"""
                {CTE}
                SELECT 
                    s.source_name AS "Nguồn Năng Lượng", 
                    ROUND(g.generation_mwh, 1) AS "Sản Lượng (MWh)", 
                    ROUND(g.generation_share_pct, 2) AS "Tỷ Trọng (%)", 
                    ROUND(g.generation_growth_pct, 2) AS "Tăng Trưởng YoY (%)", 
                    CASE 
                        WHEN LOWER(s.source_code) = 'hydro' THEN '⚡ Siêu nhanh (2-5 phút)' 
                        WHEN LOWER(s.source_code) = 'gas' THEN '🚀 Nhanh (15-30 phút)' 
                        WHEN LOWER(s.source_code) = 'coal' THEN '⏳ Chậm (4-8 giờ)' 
                        ELSE '☀️ Phụ thuộc thời tiết' 
                    END AS "Khả Năng Linh Hoạt (Ramp Speed)", 
                    CASE 
                        WHEN s.renewable_flag = true THEN '✅ Năng Lượng Tái Tạo' 
                        ELSE '⚡ Nguồn Truyền Thống' 
                    END AS "Phân Loại" 
                FROM iceberg.gold.fact_generation_daily g 
                JOIN target_date td ON g.date_key = td.date_key
                JOIN iceberg.gold.dim_energy_source s ON g.energy_source_key = s.energy_source_key 
                ORDER BY g.source_rank_in_day
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
    print(f"[✓] Dashboard 01 ready at: {url}/dashboard/{dash_id}\n")
    return dash_id

if __name__ == "__main__":
    args = parse_common_args("Dashboard 01")
    deploy_dashboard_01(args.url, args.user, args.password)
