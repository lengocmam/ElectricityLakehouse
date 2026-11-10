# -*- coding: utf-8 -*-
"""Dashboard 03: Mất Cân Đối Năng Lượng Vùng Miền & Giới Hạn Truyền Tải.

Nghịch lý phân bổ địa lý: Miền Bắc gánh 47% tải tiêu thụ nhưng tài nguyên NLTT thấp,
Miền Trung dồi dào nắng gió nhưng phụ tải chỉ 11%, áp lực nghẽn mạch siêu lưới 500kV Bắc - Nam.
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

DASHBOARD_NAME = "03. Mất Cân Đối Năng Lượng Vùng Miền & Giới Hạn Truyền Tải"
DASHBOARD_DESC = "Nghịch lý phân bổ phụ tải vs nguồn tái tạo 3 miền và bài toán nghẽn mạch truyền tải liên miền 500kV."
CTE = get_target_date_cte("iceberg.gold.fact_generation_daily")

def deploy_dashboard_03(base_url=None, user=None, password=None):
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
        # Nhịp 1: 4 KPIs hàng trên (Rows 0-3) - Đối xứng 3 miền
        {
            "name": "Tỷ Trọng Phụ Tải Miền Bắc (% Quốc Gia)",
            "display": "scalar",
            "sql": f"""
                {CTE}
                SELECT ROUND(
                    SUM(CASE WHEN LOWER(r.region_name) = 'north' THEN l.load_mw ELSE 0 END) * 100.0 / 
                    NULLIF(SUM(CASE WHEN LOWER(r.region_name) IN ('north', 'central', 'south') THEN l.load_mw ELSE 0 END), 0),
                    1
                ) AS north_pct
                FROM iceberg.gold.fact_load_hourly l 
                JOIN target_date td ON l.date_key = td.date_key
                JOIN iceberg.gold.dim_region r ON l.region_key = r.region_key
            """,
            "viz": {}, "col": 0, "row": 0, "size_x": 6, "size_y": 4
        },
        {
            "name": "Tỷ Trọng Phụ Tải Miền Trung (% Quốc Gia)",
            "display": "scalar",
            "sql": f"""
                {CTE}
                SELECT ROUND(
                    SUM(CASE WHEN LOWER(r.region_name) = 'central' THEN l.load_mw ELSE 0 END) * 100.0 / 
                    NULLIF(SUM(CASE WHEN LOWER(r.region_name) IN ('north', 'central', 'south') THEN l.load_mw ELSE 0 END), 0),
                    1
                ) AS central_pct
                FROM iceberg.gold.fact_load_hourly l 
                JOIN target_date td ON l.date_key = td.date_key
                JOIN iceberg.gold.dim_region r ON l.region_key = r.region_key
            """,
            "viz": {}, "col": 6, "row": 0, "size_x": 6, "size_y": 4
        },
        {
            "name": "Tỷ Trọng Phụ Tải Miền Nam (% Quốc Gia)",
            "display": "scalar",
            "sql": f"""
                {CTE}
                SELECT ROUND(
                    SUM(CASE WHEN LOWER(r.region_name) = 'south' THEN l.load_mw ELSE 0 END) * 100.0 / 
                    NULLIF(SUM(CASE WHEN LOWER(r.region_name) IN ('north', 'central', 'south') THEN l.load_mw ELSE 0 END), 0),
                    1
                ) AS south_pct
                FROM iceberg.gold.fact_load_hourly l 
                JOIN target_date td ON l.date_key = td.date_key
                JOIN iceberg.gold.dim_region r ON l.region_key = r.region_key
            """,
            "viz": {}, "col": 12, "row": 0, "size_x": 6, "size_y": 4
        },
        {
            "name": "Chỉ Báo Áp Lực Truyền Tải Liên Miền (Proxy Index)",
            "display": "scalar",
            "sql": f"""
                {CTE}
                SELECT CASE 
                    WHEN MAX(CASE WHEN LOWER(r.region_name) = 'north' THEN l.load_mw ELSE 0 END) > 28000 
                    THEN '⚠️ CĂNG THẲNG CAO - NGHẼN MẠCH BẮC TRUNG' 
                    WHEN MAX(CASE WHEN LOWER(r.region_name) = 'north' THEN l.load_mw ELSE 0 END) > 22000 
                    THEN '🟡 CẦN HỖ TRỢ TRUYỀN TẢI LIÊN MIỀN'
                    ELSE '🟢 TRUYỀN TẢI BÌNH THƯỜNG' 
                END AS grid_status 
                FROM iceberg.gold.fact_load_hourly l 
                JOIN target_date td ON l.date_key = td.date_key
                JOIN iceberg.gold.dim_region r ON l.region_key = r.region_key
            """,
            "viz": {}, "col": 18, "row": 0, "size_x": 6, "size_y": 4
        },

        # Nhịp 2: 2 Hero Charts (Rows 4-13) - Tương quan 3 miền 24h
        {
            "name": "Phân Bổ Phụ Tải Chi Tiết 3 Miền 24 Giờ",
            "display": "area",
            "sql": f"""
                {CTE}
                SELECT 
                    t.hour AS Gio, 
                    ROUND(SUM(CASE WHEN LOWER(r.region_name) = 'north' THEN l.load_mw ELSE 0 END), 1) AS Mien_Bac, 
                    ROUND(SUM(CASE WHEN LOWER(r.region_name) = 'central' THEN l.load_mw ELSE 0 END), 1) AS Mien_Trung, 
                    ROUND(SUM(CASE WHEN LOWER(r.region_name) = 'south' THEN l.load_mw ELSE 0 END), 1) AS Mien_Nam 
                FROM iceberg.gold.fact_load_hourly l 
                JOIN target_date td ON l.date_key = td.date_key
                JOIN iceberg.gold.dim_time t ON l.time_key = t.time_key 
                JOIN iceberg.gold.dim_region r ON l.region_key = r.region_key 
                GROUP BY t.hour 
                ORDER BY t.hour
            """,
            "viz": {
                "graph.dimensions": ["Gio"],
                "graph.metrics": ["Mien_Bac", "Mien_Trung", "Mien_Nam"],
                "graph.x_axis.scale": "linear",
                "stackable.stack_type": "stacked",
                "graph.y_axis.title_text": "Phụ tải tiêu thụ (MW)",
                "graph.x_axis.title_text": "Giờ trong ngày",
                "column_settings": {
                    '["name","Gio"]': {"column_title": "Giờ"},
                    '["name","Mien_Bac"]': {"column_title": "Miền Bắc (MW)"},
                    '["name","Mien_Trung"]': {"column_title": "Miền Trung (MW)"},
                    '["name","Mien_Nam"]': {"column_title": "Miền Nam (MW)"}
                }
            },
            "col": 0, "row": 4, "size_x": 12, "size_y": 10
        },
        {
            "name": "So Sánh Biểu Đồ Phụ Tải 24 Giờ: Bắc vs Nam vs Trung",
            "display": "line",
            "sql": f"""
                {CTE}
                SELECT 
                    t.hour AS Gio, 
                    ROUND(SUM(CASE WHEN LOWER(r.region_name) = 'north' THEN l.load_mw ELSE 0 END), 0) AS Mien_Bac_MW, 
                    ROUND(SUM(CASE WHEN LOWER(r.region_name) = 'south' THEN l.load_mw ELSE 0 END), 0) AS Mien_Nam_MW, 
                    ROUND(SUM(CASE WHEN LOWER(r.region_name) = 'central' THEN l.load_mw ELSE 0 END), 0) AS Mien_Trung_MW 
                FROM iceberg.gold.fact_load_hourly l 
                JOIN target_date td ON l.date_key = td.date_key
                JOIN iceberg.gold.dim_time t ON l.time_key = t.time_key 
                JOIN iceberg.gold.dim_region r ON l.region_key = r.region_key 
                GROUP BY t.hour 
                ORDER BY t.hour
            """,
            "viz": {
                "graph.dimensions": ["Gio"],
                "graph.metrics": ["Mien_Bac_MW", "Mien_Nam_MW", "Mien_Trung_MW"],
                "graph.x_axis.scale": "linear",
                "graph.y_axis.title_text": "Công suất tiêu thụ (MW)",
                "graph.x_axis.title_text": "Giờ trong ngày (0-23h)",
                "column_settings": {
                    '["name","Gio"]': {"column_title": "Giờ"},
                    '["name","Mien_Bac_MW"]': {"column_title": "🔴 Miền Bắc (Tải đỉnh 23,500 MW lúc 18h)"},
                    '["name","Mien_Nam_MW"]': {"column_title": "🔵 Miền Nam (Tải đỉnh 20,000 MW lúc 14h)"},
                    '["name","Mien_Trung_MW"]': {"column_title": "🟡 Miền Trung (Tải thấp ~5,000 MW)"}
                }
            },
            "col": 12, "row": 4, "size_x": 12, "size_y": 10
        },

        # Nhịp 3: Phân tích tài nguyên & Chênh lệch truyền tải (Rows 14-31)
        {
            "name": "Bức Xạ Nắng & Tốc Độ Gió 3 Miền (Tiềm Năng Khí Hậu Tự Nhiên)",
            "display": "bar",
            "sql": f"""
                {CTE}
                SELECT 
                    CASE 
                        WHEN r.region_name = 'north' THEN 'Miền Bắc' 
                        WHEN r.region_name = 'central' THEN 'Miền Trung' 
                        WHEN r.region_name = 'south' THEN 'Miền Nam' 
                        ELSE r.region_name 
                    END AS Vung_Mien, 
                    ROUND(AVG(w.shortwave_radiation_sum), 1) AS Buc_Xa_Nang_MJ_m2, 
                    ROUND(AVG(w.wind_speed_10m_max), 1) AS Toc_Do_Gio_m_s 
                FROM iceberg.gold.fact_weather_daily w 
                JOIN target_date td ON w.date_key = td.date_key
                JOIN iceberg.gold.dim_location loc ON w.location_key = loc.location_key 
                JOIN iceberg.gold.dim_region r ON loc.region_key = r.region_key 
                WHERE r.region_name != 'national'
                GROUP BY r.region_name 
                ORDER BY Buc_Xa_Nang_MJ_m2 DESC
            """,
            "viz": {
                "graph.dimensions": ["Vung_Mien"],
                "graph.metrics": ["Buc_Xa_Nang_MJ_m2", "Toc_Do_Gio_m_s"],
                "graph.y_axis.title_text": "Tiềm năng thời tiết tự nhiên",
                "column_settings": {
                    '["name","Vung_Mien"]': {"column_title": "Vùng Miền"},
                    '["name","Buc_Xa_Nang_MJ_m2"]': {"column_title": "Bức xạ mặt trời (MJ/m²)"},
                    '["name","Toc_Do_Gio_m_s"]': {"column_title": "Tốc độ gió (m/s)"}
                }
            },
            "col": 0, "row": 14, "size_x": 12, "size_y": 10
        },
        {
            "name": "Chênh Lệch Phụ Tải Bắc - Nam & Dòng Hỗ Trợ Ước Tính (Proxy 500kV)",
            "display": "line",
            "sql": f"""
                {CTE}
                SELECT 
                    t.hour AS Gio, 
                    ROUND(SUM(CASE WHEN LOWER(r.region_name) = 'north' THEN l.load_mw ELSE 0 END) - 
                          SUM(CASE WHEN LOWER(r.region_name) = 'south' THEN l.load_mw ELSE 0 END), 0) AS Chenh_Lech_Bac_Nam_MW,
                    ROUND(GREATEST(0.0, SUM(CASE WHEN LOWER(r.region_name) = 'north' THEN l.load_mw ELSE 0 END) - 18000.0), 0) AS Dong_Truyen_Tai_500kV_Uoc_Tinh_MW
                FROM iceberg.gold.fact_load_hourly l 
                JOIN target_date td ON l.date_key = td.date_key
                JOIN iceberg.gold.dim_time t ON l.time_key = t.time_key 
                JOIN iceberg.gold.dim_region r ON l.region_key = r.region_key 
                GROUP BY t.hour 
                ORDER BY t.hour
            """,
            "viz": {
                "graph.dimensions": ["Gio"],
                "graph.metrics": ["Chenh_Lech_Bac_Nam_MW", "Dong_Truyen_Tai_500kV_Uoc_Tinh_MW"],
                "graph.x_axis.scale": "linear",
                "graph.y_axis.title_text": "Công suất chênh lệch & Dòng hỗ trợ Proxy (MW)",
                "graph.x_axis.title_text": "Giờ trong ngày (0-23h)",
                "column_settings": {
                    '["name","Gio"]': {"column_title": "Giờ"},
                    '["name","Chenh_Lech_Bac_Nam_MW"]': {"column_title": "Chênh lệch tải Bắc - Nam (Vọt +5,267 MW lúc 18h)"},
                    '["name","Dong_Truyen_Tai_500kV_Uoc_Tinh_MW"]': {"column_title": "Dòng công suất hỗ trợ ước tính - Proxy (MW)"}
                }
            },
            "col": 12, "row": 14, "size_x": 12, "size_y": 10
        },
        {
            "name": "Bảng Đánh Giá Cân Bằng Cung Cầu & Nguy Cơ Nghẽn Mạch Truyền Tải",
            "display": "table",
            "sql": f"""
                {CTE}
                SELECT 
                    CASE 
                        WHEN LOWER(r.region_name) = 'national' THEN 'Toàn Quốc' 
                        WHEN LOWER(r.region_name) = 'north' THEN 'Miền Bắc' 
                        WHEN LOWER(r.region_name) = 'central' THEN 'Miền Trung' 
                        WHEN LOWER(r.region_name) = 'south' THEN 'Miền Nam' 
                        ELSE r.region_name 
                    END AS "Vùng Miền", 
                    ROUND(SUM(l.load_mw) / 1000.0, 1) AS "Tổng Phụ Tải Ngày (GWh)", 
                    ROUND(MAX(l.load_mw), 0) AS "Công Suất Đỉnh Pmax (MW)", 
                    ROUND(AVG(l.load_mw), 0) AS "Công Suất Bình Quân (MW)", 
                    CASE 
                        WHEN LOWER(r.region_name) = 'north' THEN '⚡ Tải công nghiệp cao, nguy cơ thiếu hụt' 
                        WHEN LOWER(r.region_name) = 'central' THEN '☀️ Nguồn phát dư thừa, cần giải tỏa' 
                        WHEN LOWER(r.region_name) = 'south' THEN '🟢 Phụ tải ổn định, năng lượng mặt trời lớn' 
                        ELSE '⚡ Toàn hệ thống an toàn' 
                    END AS "Đánh Giá Vận Hành Lưới Điện" 
                FROM iceberg.gold.fact_load_hourly l 
                JOIN target_date td ON l.date_key = td.date_key
                JOIN iceberg.gold.dim_region r ON l.region_key = r.region_key 
                GROUP BY r.region_name 
                ORDER BY "Tổng Phụ Tải Ngày (GWh)" DESC
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
    print(f"[✓] Dashboard 03 ready at: {url}/dashboard/{dash_id}\n")
    return dash_id

if __name__ == "__main__":
    args = parse_common_args("Dashboard 03")
    deploy_dashboard_03(args.url, args.user, args.password)
