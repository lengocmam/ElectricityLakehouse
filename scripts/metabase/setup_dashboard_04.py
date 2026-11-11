# -*- coding: utf-8 -*-
"""Dashboard 04: Kế Hoạch Năng Lượng & Phân Tích Xu Hướng Theo Tháng (Monthly Strategic Planning).

Hỗ trợ phân tích chiến lược trung - dài hạn:
- Xu hướng điện năng tiêu thụ (TWh) qua 4 năm liên tiếp (2023, 2024, 2025, 2026).
- Chu kỳ mùa vụ nguồn phát (Mùa khô vs Mùa mưa: Than, Thủy điện, Tua bin khí, Năng lượng tái tạo).
- Diễn biến đỉnh phụ tải Pmax và độ nhạy nhiệt độ theo tháng.
- Kế hoạch tích nước và an ninh hồ thủy điện theo tháng.
- Hỗ trợ bộ lọc động {{selected_year}} (Mặc định: 2026).
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
)

DASHBOARD_NAME = "04. Kế Hoạch Năng Lượng & Phân Tích Xu Hướng Theo Tháng"
DASHBOARD_DESC = (
    "Công cụ hỗ trợ lãnh đạo và ban kế hoạch: Phân tích xu hướng điện năng tiêu thụ (TWh), "
    "tăng trưởng phụ tải qua các năm 2023 - 2026, chu kỳ mùa vụ cơ cấu nguồn phát điện (Than, Khí, Thủy điện, Tái tạo) "
    "và an ninh dung tích hồ chứa nước."
)

TAG_DEF_YEAR = {
    "selected_year": {
        "id": "tag_selected_year",
        "name": "selected_year",
        "display-name": "Chọn Năm Phân Tích (Để trống = Mặc định 2026)",
        "type": "number",
        "required": False
    }
}


def deploy_dashboard_04(base_url=None, user=None, password=None):
    url = get_metabase_url(base_url)
    print("\n" + "=" * 70)
    print(f"DEPLOYING {DASHBOARD_NAME}")
    print(f"Target Metabase: {url}")
    print("=" * 70)

    token = authenticate(url, user or "admin@lakehouse.vn", password or "Lakehouse@2026")
    db_id = get_database_id(url, token)
    col_id = get_or_create_collection(url, token)
    dash_id = get_or_create_dashboard(url, token, col_id, DASHBOARD_NAME, DASHBOARD_DESC)

    cards_to_place = []

    # --------------------------------------------------------------------------
    # HÀNG 1: THẺ KPI THÁNG / NĂM (Row 0 - 3, Height: 4)
    # --------------------------------------------------------------------------
    # Card 4.1: Tổng Điện Năng Tiêu Thụ Toàn Quốc (TWh)
    sql_c1 = """
    WITH target AS (
        SELECT COALESCE(NULL [[ , CAST({{selected_year}} AS INTEGER) ]], 2026) AS sel_year
    )
    SELECT 
        ROUND(SUM(v.total_energy_twh), 2) AS "Tổng Điện Năng Tiêu Thụ (TWh)"
    FROM iceberg.gold.v_dashboard4_monthly_energy_planning v
    JOIN target t ON v.yr = t.sel_year
    """
    c1_id = upsert_card(
        url, token, col_id, db_id,
        "D4-1. Tổng Điện Năng Tiêu Thụ Trong Năm (TWh)",
        sql_c1, "scalar",
        viz={"scalar.title": "Tổng Điện Năng Tiêu Thụ (TWh)", "scalar.subtitle": "Tích lũy từ hệ thống phụ tải toàn quốc"},
        tag_def=TAG_DEF_YEAR
    )
    cards_to_place.append({
        "card_id": c1_id,
        "col": 0, "row": 0, "size_x": 6, "size_y": 4,
        "parameter_mappings": [{
            "parameter_id": "year_filter",
            "card_id": c1_id,
            "target": ["variable", ["template-tag", "selected_year"]]
        }]
    })

    # Card 4.2: Đỉnh Công Suất Pmax Kỷ Lục Năm (MW)
    sql_c2 = """
    WITH target AS (
        SELECT COALESCE(NULL [[ , CAST({{selected_year}} AS INTEGER) ]], 2026) AS sel_year
    )
    SELECT 
        ROUND(MAX(v.peak_load_pmax_mw), 0) AS "Đỉnh Công Suất Pmax (MW)"
    FROM iceberg.gold.v_dashboard4_monthly_energy_planning v
    JOIN target t ON v.yr = t.sel_year
    """
    c2_id = upsert_card(
        url, token, col_id, db_id,
        "D4-2. Đỉnh Công Suất Pmax Kỷ Lục Năm (MW)",
        sql_c2, "scalar",
        viz={"scalar.title": "Đỉnh Công Suất Pmax (MW)", "scalar.subtitle": "Công suất đỉnh cao nhất ghi nhận trong năm"},
        tag_def=TAG_DEF_YEAR
    )
    cards_to_place.append({
        "card_id": c2_id,
        "col": 6, "row": 0, "size_x": 6, "size_y": 4,
        "parameter_mappings": [{
            "parameter_id": "year_filter",
            "card_id": c2_id,
            "target": ["variable", ["template-tag", "selected_year"]]
        }]
    })

    # Card 4.3: Tỷ Trọng Thủy Điện Bình Quân Năm (%)
    sql_c3 = """
    WITH target AS (
        SELECT COALESCE(NULL [[ , CAST({{selected_year}} AS INTEGER) ]], 2026) AS sel_year
    )
    SELECT 
        ROUND(SUM(v.gen_hydro_gwh) * 100.0 / NULLIF(SUM(v.gen_total_gwh), 0), 1) AS "Tỷ Trọng Thủy Điện (%)"
    FROM iceberg.gold.v_dashboard4_monthly_energy_planning v
    JOIN target t ON v.yr = t.sel_year
    """
    c3_id = upsert_card(
        url, token, col_id, db_id,
        "D4-3. Tỷ Trọng Thủy Điện Bình Quân Năm (%)",
        sql_c3, "scalar",
        viz={"scalar.title": "Tỷ Trọng Thủy Điện (%)", "scalar.subtitle": "Đóng góp của nguồn thủy điện trong tổng sản lượng"},
        tag_def=TAG_DEF_YEAR
    )
    cards_to_place.append({
        "card_id": c3_id,
        "col": 12, "row": 0, "size_x": 6, "size_y": 4,
        "parameter_mappings": [{
            "parameter_id": "year_filter",
            "card_id": c3_id,
            "target": ["variable", ["template-tag", "selected_year"]]
        }]
    })

    # Card 4.4: Nhiệt Độ Tháng Nóng Nhất (°C)
    sql_c4 = """
    WITH target AS (
        SELECT COALESCE(NULL [[ , CAST({{selected_year}} AS INTEGER) ]], 2026) AS sel_year
    )
    SELECT 
        ROUND(MAX(v.avg_temp_c), 1) AS "Nhiệt Độ TB Tháng Cao Nhất (°C)"
    FROM iceberg.gold.v_dashboard4_monthly_energy_planning v
    JOIN target t ON v.yr = t.sel_year
    """
    c4_id = upsert_card(
        url, token, col_id, db_id,
        "D4-4. Nhiệt Độ Tháng Nóng Nhất (°C)",
        sql_c4, "scalar",
        viz={"scalar.title": "Nhiệt Độ Tháng Nóng Nhất (°C)", "scalar.subtitle": "Nhiệt độ trung bình của tháng cao điểm nắng nóng"},
        tag_def=TAG_DEF_YEAR
    )
    cards_to_place.append({
        "card_id": c4_id,
        "col": 18, "row": 0, "size_x": 6, "size_y": 4,
        "parameter_mappings": [{
            "parameter_id": "year_filter",
            "card_id": c4_id,
            "target": ["variable", ["template-tag", "selected_year"]]
        }]
    })

    # --------------------------------------------------------------------------
    # HÀNG 2: BIỂU ĐỒ HERO CHIẾN LƯỢC (Row 4 - 11, Height: 8)
    # --------------------------------------------------------------------------
    # Card 4.5: So Sánh Điện Năng Tiêu Thụ Theo Tháng Qua Các Năm 2023 - 2026 (TWh)
    sql_c5 = """
    SELECT 
        month_label AS "Tháng",
        ROUND(SUM(CASE WHEN yr = 2023 THEN total_energy_twh END), 2) AS "Năm 2023 (TWh)",
        ROUND(SUM(CASE WHEN yr = 2024 THEN total_energy_twh END), 2) AS "Năm 2024 (TWh)",
        ROUND(SUM(CASE WHEN yr = 2025 THEN total_energy_twh END), 2) AS "Năm 2025 (TWh)",
        ROUND(SUM(CASE WHEN yr = 2026 THEN total_energy_twh END), 2) AS "Năm 2026 (TWh)"
    FROM iceberg.gold.v_dashboard4_monthly_energy_planning
    GROUP BY month, month_label
    ORDER BY month
    """
    c5_id = upsert_card(
        url, token, col_id, db_id,
        "D4-5. [Xu Hướng] Điện Năng Tiêu Thụ Theo Tháng Qua Các Năm 2023 - 2026 (TWh)",
        sql_c5, "bar",
        viz={
            "graph.dimensions": ["Tháng"],
            "graph.metrics": ["Năm 2023 (TWh)", "Năm 2024 (TWh)", "Năm 2025 (TWh)", "Năm 2026 (TWh)"],
            "graph.x_axis.title_text": "Tháng Trong Năm",
            "graph.y_axis.title_text": "Điện Năng Tiêu Thụ (TWh)"
        }
    )
    cards_to_place.append({
        "card_id": c5_id,
        "col": 0, "row": 4, "size_x": 12, "size_y": 8,
        "parameter_mappings": []  # Luôn hiển thị so sánh cả 4 năm
    })

    # Card 4.6: Chuyển Dịch Cơ Cấu Nguồn Phát Theo Tháng (GWh)
    sql_c6 = """
    WITH target AS (
        SELECT COALESCE(NULL [[ , CAST({{selected_year}} AS INTEGER) ]], 2026) AS sel_year
    )
    SELECT 
        v.month_label AS "Tháng",
        v.gen_coal_gwh AS "Nhiệt Điện Than",
        v.gen_hydro_gwh AS "Thủy Điện",
        v.gen_gas_gwh AS "Tua Bin Khí",
        v.gen_solar_gwh AS "Điện Mặt Trời",
        v.gen_wind_gwh AS "Điện Gió"
    FROM iceberg.gold.v_dashboard4_monthly_energy_planning v
    JOIN target t ON v.yr = t.sel_year
    ORDER BY v.month
    """
    c6_id = upsert_card(
        url, token, col_id, db_id,
        "D4-6. [Cơ Cấu Mùa Vụ] Sản Lượng Phát Điện Theo Nguồn Từng Tháng (GWh)",
        sql_c6, "area",
        viz={
            "graph.dimensions": ["Tháng"],
            "graph.metrics": ["Nhiệt Điện Than", "Thủy Điện", "Tua Bin Khí", "Điện Mặt Trời", "Điện Gió"],
            "graph.x_axis.title_text": "Tháng",
            "graph.y_axis.title_text": "Sản Lượng (GWh)"
        },
        tag_def=TAG_DEF_YEAR
    )
    cards_to_place.append({
        "card_id": c6_id,
        "col": 12, "row": 4, "size_x": 12, "size_y": 8,
        "parameter_mappings": [{
            "parameter_id": "year_filter",
            "card_id": c6_id,
            "target": ["variable", ["template-tag", "selected_year"]]
        }]
    })

    # --------------------------------------------------------------------------
    # HÀNG 3: TƯƠNG QUAN ĐỈNH TẢI - NHIỆT ĐỘ & HỒ CHỨA (Row 12 - 19, Height: 8)
    # --------------------------------------------------------------------------
    # Card 4.7: Đỉnh Phụ Tải Pmax vs Nhiệt Độ Bình Quân Theo Tháng
    sql_c7 = """
    WITH target AS (
        SELECT COALESCE(NULL [[ , CAST({{selected_year}} AS INTEGER) ]], 2026) AS sel_year
    )
    SELECT 
        v.month_label AS "Tháng",
        v.peak_load_pmax_mw AS "Đỉnh Công Suất Pmax (MW)",
        v.avg_load_mw AS "Phụ Tải Bình Quân (MW)",
        v.avg_temp_c AS "Nhiệt Độ TB (°C)"
    FROM iceberg.gold.v_dashboard4_monthly_energy_planning v
    JOIN target t ON v.yr = t.sel_year
    ORDER BY v.month
    """
    c7_id = upsert_card(
        url, token, col_id, db_id,
        "D4-7. [Tương Quan Mùa Vụ] Đỉnh Phụ Tải Pmax vs Nhiệt Độ Bình Quân",
        sql_c7, "line",
        viz={
            "graph.dimensions": ["Tháng"],
            "graph.metrics": ["Đỉnh Công Suất Pmax (MW)", "Phụ Tải Bình Quân (MW)", "Nhiệt Độ TB (°C)"],
            "graph.x_axis.title_text": "Tháng",
            "graph.y_axis.title_text": "Công Suất (MW) / Nhiệt Độ (°C)"
        },
        tag_def=TAG_DEF_YEAR
    )
    cards_to_place.append({
        "card_id": c7_id,
        "col": 0, "row": 12, "size_x": 10, "size_y": 8,
        "parameter_mappings": [{
            "parameter_id": "year_filter",
            "card_id": c7_id,
            "target": ["variable", ["template-tag", "selected_year"]]
        }]
    })

    # Card 4.8: An Ninh Hồ Chứa & Lưu Lượng Nước Về Bình Quân Tháng
    sql_c8 = """
    WITH target AS (
        SELECT COALESCE(NULL [[ , CAST({{selected_year}} AS INTEGER) ]], 2026) AS sel_year
    )
    SELECT 
        v.month_label AS "Tháng",
        v.avg_useful_capacity_pct AS "Dung Tích Hữu Ích TB (%)",
        v.avg_inflow_m3s AS "Lưu Lượng Nước Về TB (m³/s)",
        v.avg_discharge_m3s AS "Tổng Xả TB (m³/s)"
    FROM iceberg.gold.v_dashboard4_monthly_energy_planning v
    JOIN target t ON v.yr = t.sel_year
    ORDER BY v.month
    """
    c8_id = upsert_card(
        url, token, col_id, db_id,
        "D4-8. [An Ninh Nước] Dung Tích Hồ Chứa & Thủy Văn Theo Tháng",
        sql_c8, "bar",
        viz={
            "graph.dimensions": ["Tháng"],
            "graph.metrics": ["Dung Tích Hữu Ích TB (%)", "Lưu Lượng Nước Về TB (m³/s)", "Tổng Xả TB (m³/s)"],
            "graph.x_axis.title_text": "Tháng",
            "graph.y_axis.title_text": "Tỷ Lệ (%) / Lưu Lượng (m³/s)"
        },
        tag_def=TAG_DEF_YEAR
    )
    cards_to_place.append({
        "card_id": c8_id,
        "col": 10, "row": 12, "size_x": 14, "size_y": 8,
        "parameter_mappings": [{
            "parameter_id": "year_filter",
            "card_id": c8_id,
            "target": ["variable", ["template-tag", "selected_year"]]
        }]
    })

    # --------------------------------------------------------------------------
    # HÀNG 4: BẢNG TỔNG HỢP CHI TIẾT 12 THÁNG (Row 20 - 27, Height: 8)
    # --------------------------------------------------------------------------
    # Card 4.9: Bảng Tổng Hợp Chi Tiết Chỉ Tiêu Năng Lượng 12 Tháng
    sql_c9 = """
    WITH target AS (
        SELECT COALESCE(NULL [[ , CAST({{selected_year}} AS INTEGER) ]], 2026) AS sel_year
    )
    SELECT 
        v.month_label AS "Tháng",
        v.total_energy_twh AS "Điện Năng (TWh)",
        v.peak_load_pmax_mw AS "Đỉnh Pmax (MW)",
        v.avg_load_mw AS "Phụ Tải TB (MW)",
        v.pct_coal AS "Tỷ Trọng Than (%)",
        v.pct_hydro AS "Tỷ Trọng Thủy Điện (%)",
        v.pct_renewables AS "Tỷ Trọng Tái Tạo (%)",
        v.avg_temp_c AS "Nhiệt Độ TB (°C)",
        v.avg_precip_mm AS "Lượng Mưa TB (mm)"
    FROM iceberg.gold.v_dashboard4_monthly_energy_planning v
    JOIN target t ON v.yr = t.sel_year
    ORDER BY v.month
    """
    c9_id = upsert_card(
        url, token, col_id, db_id,
        "D4-9. [Bảng Kế Hoạch] Tổng Hợp Chi Tiết Chỉ Tiêu Năng Lượng 12 Tháng",
        sql_c9, "table",
        viz={},
        tag_def=TAG_DEF_YEAR
    )
    cards_to_place.append({
        "card_id": c9_id,
        "col": 0, "row": 20, "size_x": 24, "size_y": 8,
        "parameter_mappings": [{
            "parameter_id": "year_filter",
            "card_id": c9_id,
            "target": ["variable", ["template-tag", "selected_year"]]
        }]
    })

    # Cấu hình bố cục Dashboard và Filter Parameter
    year_param = [
        {
            "id": "year_filter",
            "name": "Chọn Năm Phân Tích (Để trống = Mặc định 2026)",
            "slug": "selected_year",
            "type": "number",
            "default": 2026
        }
    ]
    set_dashboard_layout(url, token, dash_id, cards_to_place, parameters=year_param)

    print("\n" + "=" * 70)
    print("DEPLOYMENT DASHBOARD 04 COMPLETED!")
    print(f"URL: {url}/dashboard/{dash_id}")
    print("=" * 70 + "\n")
    return dash_id


if __name__ == "__main__":
    deploy_dashboard_04()
