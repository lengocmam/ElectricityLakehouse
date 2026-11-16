# -*- coding: utf-8 -*-
"""Dashboard 03: Chu Kỳ Năng Lượng Theo Mùa (Seasonal Climate & Power Dynamics).

Phân tích chu kỳ 4 Mùa (Xuân, Hạ, Thu, Đông) và 12 Tháng của Thủy điện, Nắng, Gió, Nhiệt độ và Phụ tải 3 Miền.
Kế thừa trọn vẹn triết lý học thuật:
1. TUYỆT ĐỐI KHÔNG DÙNG AVG (No AVG): Chỉ dùng SUM, MAX hoặc đếm tần suất thực tế.
2. 5 BIỂU ĐỒ TRỰC QUAN ĐA DẠNG + CHỈ 1 BẢNG KHUYẾN NGHỊ TÁC NGHIỆP 4 MÙA.
3. KHAI THÁC TOÀN DIỆN KHO DỮ LIỆU LỊCH SỬ 3.5 NĂM (2023 - 2026):
   - Đỉnh phụ tải nắng nóng mùa hè (Tháng 5 - 8).
   - Lệch pha mùa lũ thủy điện Bắc (Hè) vs Trung (Thu - Đông).
   - Mùa bão và gió giật cực đoan (Tháng 9 - 11).
   - Chuyển dịch cơ cấu bù trừ Thủy điện - Mặt trời - Than theo 4 mùa.
"""

import os
import sys

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
)

DASHBOARD_NAME = "03. Chu Kỳ Năng Lượng Theo Mùa"
DASHBOARD_DESC = (
    "Trung tâm Phân tích Chiến lược Chu kỳ Mùa & Khí hậu: Đối chiếu quy luật 12 tháng và 4 mùa (Xuân, Hạ, Thu, Đông) "
    "của Thủy điện, Nắng, Gió, Nhiệt độ và Phụ tải 3 Miền; minh chứng tính cấp thiết của đường dây 500kV truyền tải bù trừ liên miền."
)

TAG_DEF_YEAR = {
    "selected_year": {
        "id": "tag_selected_year",
        "name": "selected_year",
        "display-name": "Chọn Năm Phân Tích (Để trống = Mặc định 2024)",
        "type": "number",
        "required": False
    }
}

YEAR_CTE = """
WITH target AS (
    SELECT COALESCE(NULL [[ , CAST({{selected_year}} AS INTEGER) ]], 2024) AS sel_year
)
""".strip()


def deploy_dashboard_seasonal(base_url=None, user=None, password=None):
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
        # HÀNG 1: CHU KỲ NHIỆT ĐỘ, PHỤ TẢI & NĂNG LƯỢNG TÁI TẠO 12 THÁNG
        # ======================================================================
        {
            "name": "S3-1. [Nhiệt Độ Cực Đại & Phụ Tải Đỉnh 12 Tháng] Đỉnh Nắng Nóng vs Đỉnh Điện",
            "desc": "Đối chiếu nhiệt độ cao nhất (Tmax °C) và công suất phụ tải đỉnh (Pmax MW) theo 12 tháng. Chứng minh đỉnh phụ tải nắng nóng mùa hè (Tháng 6-8).",
            "display": "combo",
            "sql": f"""
                {YEAR_CTE},
                l AS (
                    SELECT 
                        d.month AS m,
                        ROUND(MAX(f.load_mw)) AS peak_load_mw
                    FROM gold.fact_load_hourly f
                    JOIN gold.dim_date d ON f.date_key = d.date_key
                    CROSS JOIN target
                    WHERE d.date_key / 10000 = target.sel_year
                    GROUP BY d.month
                ),
                w AS (
                    SELECT 
                        d.month AS m,
                        ROUND(MAX(f.temperature_2m_max), 1) AS max_temp
                    FROM gold.fact_weather_daily f
                    JOIN gold.dim_date d ON f.date_key = d.date_key
                    CROSS JOIN target
                    WHERE d.date_key / 10000 = target.sel_year
                    GROUP BY d.month
                )
                SELECT 
                    'Tháng ' || LPAD(CAST(l.m AS VARCHAR), 2, '0') AS "Tháng",
                    l.peak_load_mw AS "Phụ Tải Đỉnh Pmax (MW)",
                    w.max_temp AS "Nhiệt Độ Cực Đại (°C)"
                FROM l
                JOIN w ON l.m = w.m
                ORDER BY l.m
            """,
            "viz": {
                "graph.dimensions": ["Tháng"],
                "graph.metrics": ["Phụ Tải Đỉnh Pmax (MW)", "Nhiệt Độ Cực Đại (°C)"],
                "graph.series_settings": {
                    "Phụ Tải Đỉnh Pmax (MW)": {
                        "type": "bar",
                        "y_axis": "left",
                        "title": "Phụ Tải Đỉnh Pmax (MW)"
                    },
                    "Nhiệt Độ Cực Đại (°C)": {
                        "type": "line",
                        "y_axis": "right",
                        "title": "Nhiệt Độ Cực Đại (°C)"
                    }
                },
                "graph.x_axis.title_text": "12 Tháng Trong Năm",
                "graph.y_axis.title_text": "Phụ Tải Đỉnh (MW) [Trục Trái]"
            },
            "col": 0, "row": 0, "size_x": 12, "size_y": 8,
            "has_year_filter": True
        },
        {
            "name": "S3-2. [Chu Kỳ Nắng & Gió 12 Tháng] Bức Xạ Tích Lũy vs Gió Cực Đại",
            "desc": "Theo dõi bức xạ mặt trời (MJ/m²) và tốc độ gió cực đại (m/s) qua 12 tháng. Làm rõ: Nắng gắt mùa hè (T3-T8), Gió cực đại mùa bão đông (T9-T2).",
            "display": "line",
            "sql": f"""
                {YEAR_CTE}
                SELECT 
                    'Tháng ' || LPAD(CAST(d.month AS VARCHAR), 2, '0') AS "Tháng",
                    ROUND(SUM(f.shortwave_radiation_sum), 1) AS "Tổng Bức Xạ (MJ/m²)",
                    ROUND(MAX(f.wind_speed_10m_max), 1) AS "Gió Cực Đại (m/s)"
                FROM gold.fact_weather_daily f
                JOIN gold.dim_date d ON f.date_key = d.date_key
                CROSS JOIN target
                WHERE d.date_key / 10000 = target.sel_year
                GROUP BY d.month
                ORDER BY d.month
            """,
            "viz": {
                "graph.dimensions": ["Tháng"],
                "graph.metrics": ["Tổng Bức Xạ (MJ/m²)", "Gió Cực Đại (m/s)"],
                "graph.x_axis.title_text": "12 Tháng Trong Năm",
                "graph.y_axis.axis_enabled": True
            },
            "col": 12, "row": 0, "size_x": 12, "size_y": 8,
            "has_year_filter": True
        },

        # ======================================================================
        # HÀNG 2: THỦY VĂN LỆCH PHA LŨ & CƠ CẤU 4 MÙA
        # ======================================================================
        {
            "name": "S3-3. [Lệch Pha Mùa Lũ 3 Miền] Lượng Mưa 12 Tháng Bắc - Trung - Nam",
            "desc": "So sánh lượng mưa tích lũy (mm) 3 miền. Minh chứng quy luật địa lý: Miền Bắc đỉnh lũ mùa Hè (T6-T8), Miền Trung đỉnh lũ bão muộn (T9-T11).",
            "display": "bar",
            "sql": f"""
                {YEAR_CTE}
                SELECT 
                    'Tháng ' || LPAD(CAST(d.month AS VARCHAR), 2, '0') AS "Tháng",
                    ROUND(SUM(CASE WHEN loc.region_key = 1 THEN f.precipitation_sum END)) AS "Lượng Mưa Miền Bắc (mm)",
                    ROUND(SUM(CASE WHEN loc.region_key = 2 THEN f.precipitation_sum END)) AS "Lượng Mưa Miền Trung (mm)",
                    ROUND(SUM(CASE WHEN loc.region_key = 3 THEN f.precipitation_sum END)) AS "Lượng Mưa Miền Nam (mm)"
                FROM gold.fact_weather_daily f
                JOIN gold.dim_date d ON f.date_key = d.date_key
                JOIN gold.dim_location loc ON f.location_key = loc.location_key
                CROSS JOIN target
                WHERE d.date_key / 10000 = target.sel_year
                GROUP BY d.month
                ORDER BY d.month
            """,
            "viz": {
                "graph.dimensions": ["Tháng"],
                "graph.metrics": [
                    "Lượng Mưa Miền Bắc (mm)",
                    "Lượng Mưa Miền Trung (mm)",
                    "Lượng Mưa Miền Nam (mm)"
                ],
                "graph.x_axis.title_text": "12 Tháng Trong Năm",
                "graph.y_axis.title_text": "Lượng Mưa (mm)"
            },
            "col": 0, "row": 8, "size_x": 12, "size_y": 8,
            "has_year_filter": True
        },
        {
            "name": "S3-4. [Cơ Cấu Nguồn Phát 4 Mùa] Chuyển Dịch Bù Trừ Thủy Điện - Nắng - Than",
            "desc": "Sản lượng phát điện (GWh) theo 4 mùa trong năm. Thể hiện sự bù trừ: Mùa Thu Thủy điện phát tràn ngập (giảm huy động than), Mùa Khô/Hạ Than và ĐMT gánh tải.",
            "display": "bar",
            "sql": f"""
                {YEAR_CTE}
                SELECT 
                    CASE 
                        WHEN d.month IN (1, 2, 3) THEN '1. Mùa Xuân (T1-T3)'
                        WHEN d.month IN (4, 5, 6) THEN '2. Mùa Hạ (T4-T6)'
                        WHEN d.month IN (7, 8, 9) THEN '3. Mùa Thu (T7-T9)'
                        ELSE '4. Mùa Đông (T10-T12)'
                    END AS "Mùa Vụ",
                    ROUND(SUM(CASE WHEN s.source_name LIKE '%Thủy điện%' THEN f.generation_mwh END) / 1000.0) AS "Thủy Điện (GWh)",
                    ROUND(SUM(CASE WHEN s.source_name LIKE '%mặt trời%' OR s.source_name LIKE '%ĐMT%' THEN f.generation_mwh END) / 1000.0) AS "Điện Mặt Trời (GWh)",
                    ROUND(SUM(CASE WHEN s.source_name LIKE '%than%' THEN f.generation_mwh END) / 1000.0) AS "Nhiệt Điện Than (GWh)"
                FROM gold.fact_generation_daily f
                JOIN gold.dim_date d ON f.date_key = d.date_key
                JOIN gold.dim_energy_source s ON f.energy_source_key = s.energy_source_key
                CROSS JOIN target
                WHERE d.date_key / 10000 = target.sel_year
                GROUP BY 
                    CASE 
                        WHEN d.month IN (1, 2, 3) THEN '1. Mùa Xuân (T1-T3)'
                        WHEN d.month IN (4, 5, 6) THEN '2. Mùa Hạ (T4-T6)'
                        WHEN d.month IN (7, 8, 9) THEN '3. Mùa Thu (T7-T9)'
                        ELSE '4. Mùa Đông (T10-T12)'
                    END
                ORDER BY "Mùa Vụ"
            """,
            "viz": {
                "graph.dimensions": ["Mùa Vụ"],
                "graph.metrics": ["Thủy Điện (GWh)", "Điện Mặt Trời (GWh)", "Nhiệt Điện Than (GWh)"],
                "graph.x_axis.title_text": "4 Mùa Trong Năm",
                "graph.y_axis.title_text": "Sản Lượng Phát (GWh)"
            },
            "col": 12, "row": 8, "size_x": 12, "size_y": 8,
            "has_year_filter": True
        },

        # ======================================================================
        # HÀNG 3: BẢN ĐỒ RỦI RO THIÊN TAI & BẢNG KHUYẾN NGHỊ ĐIỀU ĐỘ CHIẾN LƯỢC
        # ======================================================================
        {
            "name": "S3-5. [Tần Suất Thiên Tai Cực Đoan] Số Ngày Gió Giật & Mưa To Theo Tháng",
            "desc": "Đếm số ngày có gió giật mạnh (≥15 m/s) và số ngày mưa lớn (≥50 mm) qua 12 tháng. Cảnh báo cao điểm thiên tai bão lũ (Tháng 9-11).",
            "display": "bar",
            "sql": f"""
                {YEAR_CTE}
                SELECT 
                    'Tháng ' || LPAD(CAST(d.month AS VARCHAR), 2, '0') AS "Tháng",
                    COUNT_IF(f.wind_speed_10m_max >= 15.0) AS "Số Ngày Gió Giật ≥15m/s",
                    COUNT_IF(f.precipitation_sum >= 50.0) AS "Số Ngày Mưa To ≥50mm"
                FROM gold.fact_weather_daily f
                JOIN gold.dim_date d ON f.date_key = d.date_key
                CROSS JOIN target
                WHERE d.date_key / 10000 = target.sel_year
                GROUP BY d.month
                ORDER BY d.month
            """,
            "viz": {
                "graph.dimensions": ["Tháng"],
                "graph.metrics": ["Số Ngày Gió Giật ≥15m/s", "Số Ngày Mưa To ≥50mm"],
                "graph.x_axis.title_text": "12 Tháng Trong Năm",
                "graph.y_axis.title_text": "Số Ngày Ghi Nhận"
            },
            "col": 0, "row": 16, "size_x": 10, "size_y": 8,
            "has_year_filter": True
        },
        {
            "name": "S3-6. [Bảng Chỉ Huy] Khuyến Nghị Chiến Lược Điều Độ Hệ Thống 4 Mùa",
            "desc": "Bảng tổng hợp khuyến nghị vận hành và điều độ hệ thống điện - thủy văn chuẩn NSMO/EVN theo 4 mùa trong năm.",
            "display": "table",
            "sql": f"""
                SELECT 
                    '1. Mùa Xuân (T1 - T3)' AS "Mùa Vụ",
                    'Mùa Khô Bắc Bộ / Gió Mùa Đông Bắc' AS "Đặc Trưng Khí Hậu",
                    'Tích nước hồ chứa miền Bắc; Huy động tối đa điện gió duyên hải miền Trung; Điều tiết xả nước hồ Thác Bà/Hòa Bình phục vụ 2-3 đợt đổ ải nông nghiệp vụ Đông Xuân.' AS "Chiến Lược Điều Độ Cấp Nước & Phát Điện",
                    'Mực nước hồ về sát mực nước chết nếu khô hạn kéo dài; Nguy cơ nghẽn đường dây 500kV truyền tải Bắc - Nam.' AS "Rủi Ro Trọng Tâm"
                UNION ALL
                SELECT 
                    '2. Mùa Hạ (T4 - T6)',
                    'Nắng Nóng Gay Gắt / Đỉnh Phụ Tải Pmax',
                    'Khai thác tối đa điện mặt trời áp mái và farm vào khung giờ trưa (11h-13h) để giữ nước cho thủy điện; Huy động toàn bộ nhiệt điện than/khí chạy đáy nền; Giữ nước phát tối đa giờ cao điểm tối (18h-21h).',
                    'Quá tải lưới điện cục bộ miền Bắc; Nguy cơ sự cố tổ máy nhiệt điện than do vận hành liên tục ở nhiệt độ cao.'
                UNION ALL
                SELECT 
                    '3. Mùa Thu (T7 - T9)',
                    'Mùa Lũ Chính Vụ Sông Đà / Bão Biển Đông',
                    'Vận hành liên hồ chứa sông Đà (Sơn La, Hòa Bình, Lai Châu); Mở cửa xả đáy đón lũ an toàn đập; Tận dụng nước phát điện tối đa công suất tuabin Pmax (giảm huy động nhiệt điện than để tiết kiệm nhiên liệu hóa thạch).',
                    'Lũ quét, sạt lở và lưu lượng nước về đột biến vượt tần suất thiết kế đập; Bão gió giật rã lưới điện gió.'
                UNION ALL
                SELECT 
                    '4. Mùa Đông (T10 - T12)',
                    'Mưa Lũ Muộn Miền Trung / Không Khí Lạnh',
                    'Chủ động xả hạ mực nước đón bão tại các hồ miền Trung (A Vương, Sông Tranh, Ba Hạ); Sau bão tiến hành tích nước đầy hồ lên mực nước dâng bình thường chuẩn bị cho mùa cạn năm sau; Truyền tải điện miền Trung ra miền Bắc.',
                    'Lũ chồng lũ tại miền Trung; Xả tràn gây ngập lụt vùng hạ du; Gió mùa Đông Bắc gây dao động tần số lưới điện gió.'
                ORDER BY "Mùa Vụ"
            """,
            "viz": {},
            "col": 10, "row": 16, "size_x": 14, "size_y": 8,
            "has_year_filter": False
        }
    ]

    card_specs = []
    for c in cards_def:
        cid = upsert_card(
            url, token, col_id, db_id,
            c["name"], c["sql"], c["display"],
            viz=c.get("viz"),
            tag_def=TAG_DEF_YEAR if c.get("has_year_filter", True) else None,
            description=c.get("desc")
        )
        card_specs.append({
            "card_id": cid,
            "col": c["col"],
            "row": c["row"],
            "size_x": c["size_x"],
            "size_y": c["size_y"],
            "has_year_filter": c.get("has_year_filter", True)
        })

    year_param = [
        {
            "id": "year_filter",
            "name": "Chọn Năm Phân Tích (Để trống = Mặc định 2024)",
            "slug": "selected_year",
            "type": "number",
            "default": 2024
        }
    ]

    set_dashboard_layout(url, token, dash_id, card_specs, parameters=year_param, param_id="year_filter")

    print(f"\n[SUCCESS] Deployed {len(cards_def)} cards on {DASHBOARD_NAME} (Dashboard ID: {dash_id})!")
    print(f"URL: {url}/dashboard/{dash_id}\n")
    return dash_id


if __name__ == "__main__":
    deploy_dashboard_seasonal()
