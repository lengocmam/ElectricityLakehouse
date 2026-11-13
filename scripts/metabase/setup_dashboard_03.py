# -*- coding: utf-8 -*-
"""Dashboard 03: Hỗ Trợ Điều Độ Viên: Dự Báo Phụ Tải Ngày Mai & Giám Sát Vận Hành (Chế Độ Chọn Mỗi Ngày).

Hỗ trợ bộ lọc {{selected_date}} tương tác:
- Người dùng / Điều độ viên có thể chọn BẤT KỲ NGÀY NÀO trong tập kiểm thử (284 ngày năm 2026).
- Khi chọn một ngày, toàn bộ 4 khu vực áp dụng học máy và kiểm toán cho ngày đó:
  1. Dự báo ngày được chọn & 7 ngày tiếp theo (Ensemble + dải bất định).
  2. Cảnh báo ngày nóng tại ngày đó (Nhiệt độ, so sánh đỉnh lịch sử 43,609.5 MW).
  3. What-if thời tiết & Stress-test 4 kịch bản NWP.
  4. Giám sát 30 ngày tính ngược từ ngày đó (Thực tế vs Dự báo, Rolling MAPE tại thời điểm đó).
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

DASHBOARD_NAME = "03. Hỗ Trợ Điều Độ Viên: Dự Báo Phụ Tải Ngày Mai & Giám Sát Vận Hành"
DASHBOARD_DESC = (
    "Công cụ hỗ trợ điều độ viên với chế độ chọn mỗi ngày: Dự báo phụ tải ngày được chọn & 7 ngày tiếp theo, "
    "cảnh báo ngày nóng chạm đỉnh phụ tải, mô phỏng what-if thời tiết và giám sát trôi dạt mô hình 30 ngày."
)

TAG_DEF_DATE = COMMON_TAG_DEF


def deploy_dashboard_03(base_url=None, user=None, password=None):
    url = get_metabase_url(base_url)
    print("\n" + "=" * 70)
    print(f"DEPLOYING {DASHBOARD_NAME} (CHẾ ĐỘ CHỌN MỖI NGÀY)")
    print(f"Target Metabase: {url}")
    print("=" * 70)

    token = authenticate(url, user or "admin@lakehouse.vn", password or "Lakehouse@2026")
    db_id = get_database_id(url, token)
    col_id = get_or_create_collection(url, token)
    dash_id = get_or_create_dashboard(url, token, col_id, DASHBOARD_NAME, DASHBOARD_DESC)

    cards_to_place = []

    # --------------------------------------------------------------------------
    # HÀNG 1: THẺ KPI ĐIỀU ĐỘ (Row 0 - 3, Height: 4)
    # --------------------------------------------------------------------------
    # Card 3.1: Dự báo Phụ tải Ngày Được Chọn (Ensemble)
    sql_c1 = """
    WITH target AS (
        SELECT COALESCE(
            NULL [[ , CAST({{selected_date}} AS DATE) ]],
            (SELECT MAX(record_date) FROM iceberg.gold.fact_dispatcher_daily_predictions)
        ) AS sel_date
    )
    SELECT 
        ROUND(f.pred_ensemble_mw, 0) AS "Phụ tải Dự báo Ngày Được Chọn (MW)"
    FROM iceberg.gold.fact_dispatcher_daily_predictions f
    JOIN target t ON f.record_date = t.sel_date
    """
    c1_id = upsert_card(
        url, token, col_id, db_id,
        "D3-1. Phụ tải Dự báo Ngày Được Chọn (Ensemble H=1)",
        sql_c1, "scalar",
        viz={"scalar.title": "Phụ tải Dự báo Ngày Được Chọn (MW)", "scalar.subtitle": "Ensemble SSM + Tree (MAPE kỳ vọng: 2.11%)"},
        tag_def=TAG_DEF_DATE
    )
    cards_to_place.append({
        "card_id": c1_id,
        "col": 0, "row": 0, "size_x": 6, "size_y": 4,
        "has_date_filter": True
    })

    # Card 3.2: Dải Bất Định 95% Ngày Được Chọn [Min - Max]
    sql_c2 = """
    WITH target AS (
        SELECT COALESCE(
            NULL [[ , CAST({{selected_date}} AS DATE) ]],
            (SELECT MAX(record_date) FROM iceberg.gold.fact_dispatcher_daily_predictions)
        ) AS sel_date
    )
    SELECT 
        '[' || CAST(ROUND(f.ci_lower_mw, 0) AS VARCHAR) || ' - ' || CAST(ROUND(f.ci_upper_mw, 0) AS VARCHAR) || '] MW (±' || CAST(ROUND(f.uncertainty_band_mw, 0) AS VARCHAR) || ' MW)' AS "Dải Bất Định 95%"
    FROM iceberg.gold.fact_dispatcher_daily_predictions f
    JOIN target t ON f.record_date = t.sel_date
    """
    c2_id = upsert_card(
        url, token, col_id, db_id,
        "D3-2. Dải Bất Định 95% Ngày Được Chọn (Độ Tin Cậy)",
        sql_c2, "scalar",
        viz={"scalar.title": "Dải Bất Định 95%", "scalar.subtitle": "Khoảng tin cậy dựa trên sai số kiểm thử (RMSE 1,177 MW)"},
        tag_def=TAG_DEF_DATE
    )
    cards_to_place.append({
        "card_id": c2_id,
        "col": 6, "row": 0, "size_x": 6, "size_y": 4,
        "has_date_filter": True
    })

    # Card 3.3: Tình trạng Cảnh báo Ngày Nóng (Nhiệt độ +0.44 & So sánh Đỉnh LS)
    sql_c3 = """
    WITH target AS (
        SELECT COALESCE(
            NULL [[ , CAST({{selected_date}} AS DATE) ]],
            (SELECT MAX(record_date) FROM iceberg.gold.fact_dispatcher_daily_predictions)
        ) AS sel_date
    )
    SELECT 
        f.heat_risk_status || ' (T=' || CAST(ROUND(f.temp_c, 1) AS VARCHAR) || '°C)' AS "Cảnh Báo Đỉnh Tải"
    FROM iceberg.gold.fact_dispatcher_daily_predictions f
    JOIN target t ON f.record_date = t.sel_date
    """
    c3_id = upsert_card(
        url, token, col_id, db_id,
        "D3-3. Cảnh Báo Ngày Nóng & Nguy Cơ Chạm Đỉnh",
        sql_c3, "scalar",
        viz={"scalar.title": "Cảnh Báo Vận Hành", "scalar.subtitle": "Hệ số nhiệt r = +0.44 | Đỉnh lịch sử: 43,609.5 MW"},
        tag_def=TAG_DEF_DATE
    )
    cards_to_place.append({
        "card_id": c3_id,
        "col": 12, "row": 0, "size_x": 6, "size_y": 4,
        "has_date_filter": True
    })

    # Card 3.4: Sức Khỏe Mô Hình & Rolling MAPE 30 ngày tính đến ngày được chọn
    sql_c4 = """
    WITH target AS (
        SELECT COALESCE(
            NULL [[ , CAST({{selected_date}} AS DATE) ]],
            (SELECT MAX(record_date) FROM iceberg.gold.fact_dispatcher_daily_predictions)
        ) AS sel_date
    )
    SELECT 
        'MAPE 30d: ' || CAST(ROUND(f.rolling_30d_mape, 2) AS VARCHAR) || '% (Thực tế: ' || CAST(ROUND(f.actual_load_mw, 0) AS VARCHAR) || ' MW)' AS "Sức Khỏe Mô Hình"
    FROM iceberg.gold.fact_dispatcher_daily_predictions f
    JOIN target t ON f.record_date = t.sel_date
    """
    c4_id = upsert_card(
        url, token, col_id, db_id,
        "D3-4. Giám Sát Sức Khỏe Mô Hình (Rolling 30 Ngày)",
        sql_c4, "scalar",
        viz={"scalar.title": "Giám Sát Trôi Dạt Mô Hình", "scalar.subtitle": "Ngưỡng cảnh báo: 3.5% | Ngưỡng re-train: 4.5%"},
        tag_def=TAG_DEF_DATE
    )
    cards_to_place.append({
        "card_id": c4_id,
        "col": 18, "row": 0, "size_x": 6, "size_y": 4,
        "has_date_filter": True
    })

    # --------------------------------------------------------------------------
    # HÀNG 2: HERO CHARTS - DỰ BÁO 7 NGÀY & ĐỐI CHIẾU ĐỈNH LỊCH SỬ (Row 4 - 13, Height: 10)
    # --------------------------------------------------------------------------
    # Card 3.5: [Hero 1 - Khu vực 1]: Đường Dự Báo 7 Ngày Tiếp Theo
    sql_c5 = """
    WITH target AS (
        SELECT COALESCE(
            NULL [[ , CAST({{selected_date}} AS DATE) ]],
            (SELECT MAX(record_date) - INTERVAL '7' DAY FROM iceberg.gold.fact_dispatcher_daily_predictions)
        ) AS sel_date
    )
    SELECT 
        f.record_date AS "Ngày",
        f.pred_ensemble_mw AS "Dự báo Ensemble (MW)",
        f.actual_load_mw AS "Thực tế Đối chiếu (MW)",
        f.ci_lower_mw AS "Cận Dưới 95% (MW)",
        f.ci_upper_mw AS "Cận Trên 95% (MW)"
    FROM iceberg.gold.fact_dispatcher_daily_predictions f
    JOIN target t ON f.record_date >= t.sel_date AND f.record_date <= t.sel_date + INTERVAL '7' DAY
    ORDER BY f.record_date
    """
    c5_id = upsert_card(
        url, token, col_id, db_id,
        "D3-5. [Hero 1] Dự Báo Phụ Tải 7 Ngày Tới & Dải Bất Định (Ensemble)",
        sql_c5, "line",
        viz={
            "graph.dimensions": ["Ngày"],
            "graph.metrics": ["Dự báo Ensemble (MW)", "Thực tế Đối chiếu (MW)", "Cận Dưới 95% (MW)", "Cận Trên 95% (MW)"],
            "graph.x_axis.axis_enabled": True,
            "graph.y_axis.axis_enabled": True
        },
        tag_def=TAG_DEF_DATE
    )
    cards_to_place.append({
        "card_id": c5_id,
        "col": 0, "row": 4, "size_x": 14, "size_y": 10,
        "has_date_filter": True
    })

    # Card 3.6: [Khu vực 2]: Bảng Đối Chiếu 7 Ngày Kế Tiếp & Tỷ Lệ Đỉnh Lịch Sử
    sql_c6 = """
    WITH target AS (
        SELECT COALESCE(
            NULL [[ , CAST({{selected_date}} AS DATE) ]],
            (SELECT MAX(record_date) - INTERVAL '7' DAY FROM iceberg.gold.fact_dispatcher_daily_predictions)
        ) AS sel_date
    )
    SELECT 
        f.day_of_week AS "Thứ",
        f.record_date AS "Ngày",
        ROUND(f.pred_ensemble_mw, 0) AS "Dự báo (MW)",
        ROUND(f.actual_load_mw, 0) AS "Thực tế (MW)",
        f.temp_c AS "Nhiệt độ (°C)",
        f.pct_of_historical_peak AS "Tỷ lệ so với Đỉnh LS (%)",
        f.heat_risk_status AS "Mức độ rủi ro"
    FROM iceberg.gold.fact_dispatcher_daily_predictions f
    JOIN target t ON f.record_date >= t.sel_date AND f.record_date <= t.sel_date + INTERVAL '7' DAY
    ORDER BY f.record_date
    """
    c6_id = upsert_card(
        url, token, col_id, db_id,
        "D3-6. [Khu Vực 2] Ma Trận Đối Chiếu Đỉnh Lịch Sử & Rủi Ro 7 Ngày",
        sql_c6, "table",
        viz={},
        tag_def=TAG_DEF_DATE
    )
    cards_to_place.append({
        "card_id": c6_id,
        "col": 14, "row": 4, "size_x": 10, "size_y": 10,
        "has_date_filter": True
    })

    # --------------------------------------------------------------------------
    # HÀNG 3: WHAT-IF THỜI TIẾT & GIÁM SÁT 30 NGÀY (Row 14 - 23, Height: 10)
    # --------------------------------------------------------------------------
    # Card 3.7: [Khu vực 3]: Stress-Test 4 Kịch Bản Dự Báo Thời Tiết NWP
    sql_c7 = """
    SELECT 
        scenario_name AS "Kịch bản NWP Stress-Test",
        temp_error AS "Sai số Nhiệt độ",
        radiation_error AS "Sai số Bức xạ",
        ROUND(mae_mw, 1) AS "MAE (MW)",
        ROUND(mape_pct, 2) AS "MAPE (%)",
        ROUND(delta_mape_pt, 2) AS "Biến động MAPE (pt)",
        operational_status AS "Trạng thái vận hành"
    FROM iceberg.gold.fact_dispatcher_nwp_stress_test
    ORDER BY mape_pct
    """
    c7_id = upsert_card(
        url, token, col_id, db_id,
        "D3-7. [Khu Vực 3] Bảng Stress-Test 4 Kịch Bản Dự Báo Thời Tiết NWP",
        sql_c7, "table",
        viz={},
        tag_def=TAG_DEF_DATE
    )
    cards_to_place.append({
        "card_id": c7_id,
        "col": 0, "row": 14, "size_x": 12, "size_y": 10,
        "has_date_filter": False
    })

    # Card 3.8: [Khu vực 4]: Giám Sát Sai Số Thực Tế vs Dự Báo 30 Ngày Tính Đến Ngày Được Chọn
    sql_c8 = """
    WITH target AS (
        SELECT COALESCE(
            NULL [[ , CAST({{selected_date}} AS DATE) ]],
            (SELECT MAX(record_date) FROM iceberg.gold.fact_dispatcher_daily_predictions)
        ) AS sel_date
    )
    SELECT 
        f.record_date AS "Ngày",
        f.actual_load_mw AS "Phụ tải Thực tế (MW)",
        f.pred_ensemble_mw AS "Dự báo Ensemble (MW)",
        f.error_mw AS "Sai số Phần dư (MW)",
        f.rolling_30d_mape AS "Rolling MAPE 30 ngày (%)"
    FROM iceberg.gold.fact_dispatcher_daily_predictions f
    JOIN target t ON f.record_date <= t.sel_date AND f.record_date >= t.sel_date - INTERVAL '30' DAY
    ORDER BY f.record_date
    """
    c8_id = upsert_card(
        url, token, col_id, db_id,
        "D3-8. [Khu Vực 4] Giám Sát Sai Số Thực Tế vs Dự Báo 30 Ngày",
        sql_c8, "line",
        viz={
            "graph.dimensions": ["Ngày"],
            "graph.metrics": ["Phụ tải Thực tế (MW)", "Dự báo Ensemble (MW)"]
        },
        tag_def=TAG_DEF_DATE
    )
    cards_to_place.append({
        "card_id": c8_id,
        "col": 12, "row": 14, "size_x": 12, "size_y": 10,
        "has_date_filter": True
    })

    # --------------------------------------------------------------------------
    # THIẾT LẬP BỐ CỤC DASHBOARD & BỘ LỌC NGÀY TRÊN METABASE
    # --------------------------------------------------------------------------
    print("\n[+] Đang cấu hình bố cục lưới 24 cột và Bộ lọc Date Picker cho Dashboard 03...")
    for c in cards_to_place:
        if c.get("has_date_filter", False):
            c["parameter_mappings"] = [{
                "parameter_id": "date_filter",
                "card_id": c["card_id"],
                "target": ["variable", ["template-tag", "selected_date"]]
            }]
        else:
            c["parameter_mappings"] = []

    date_param = [
        {
            "id": "date_filter",
            "name": "Chọn Ngày Điều Độ (Để trống = Mặc định ngày mới nhất)",
            "slug": "selected_date",
            "type": "date/single",
            "default": None
        }
    ]
    set_dashboard_layout(url, token, dash_id, cards_to_place, parameters=date_param)

    print("\n" + "=" * 70)
    print(f"DEPLOYMENT COMPLETED WITH DATE PICKER!")
    print(f"URL: {url}/dashboard/{dash_id}")
    print("=" * 70)
    return dash_id


if __name__ == "__main__":
    deploy_dashboard_03()
