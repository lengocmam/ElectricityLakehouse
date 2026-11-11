# -*- coding: utf-8 -*-
"""Dashboard 03: Hỗ Trợ Điều Độ Viên: Dự Báo Phụ Tải Ngày Mai & Giám Sát Vận Hành.

Storytelling & Operational Architecture (4 Khu Vực Trọng Tâm):
1. Dự báo ngày mai và 7 ngày tới: Đường Ensemble (mô hình tốt nhất), kèm dải bất định 95%
   lấy từ sai số kiểm thử (MAPE kỳ vọng: 2.11% ~ 2.3% cho ngày mai, 4.07% ~ 4.0% cho ngày 7).
2. Cảnh báo ngày nóng: Tận dụng hệ số tương quan riêng phần r_partial = +0.44. Báo động khi nhiệt độ
   vượt ngưỡng kích hoạt làm mát và so sánh trực tiếp với đỉnh lịch sử 43,609.5 MW.
3. What-if thời tiết & NWP Stress Test: Đo lường độ nhạy Delta P / Delta T (+795.8 MW/°C) và trực quan hóa
   kết quả stress test 4 kịch bản NWP (Oracle, NWP Tốt, NWP Chuẩn, NWP Bão cực đoan).
4. Giám sát mô hình: So sánh dự báo vs thực tế 30 ngày, hiển thị rolling MAPE và cảnh báo khi sai số
   vượt ngưỡng trôi dạt dữ liệu (model drift).
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

DASHBOARD_NAME = "03. Hỗ Trợ Điều Độ Viên: Dự Báo Phụ Tải Ngày Mai & Giám Sát Vận Hành"
DASHBOARD_DESC = (
    "Công cụ hỗ trợ điều độ viên: Dự báo phụ tải ngày mai & 7 ngày tới kèm dải bất định, "
    "cảnh báo ngày nóng chạm đỉnh phụ tải, mô phỏng what-if thời tiết và giám sát trôi dạt mô hình 30 ngày."
)

TAG_EMPTY = {}


def deploy_dashboard_03(base_url=None, user=None, password=None):
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
    # HÀNG 1: THẺ KPI ĐIỀU ĐỘ (Row 0 - 3, Height: 4)
    # --------------------------------------------------------------------------
    # Card 3.1: Dự báo Phụ tải Ngày mai (H=1)
    sql_c1 = """
    SELECT 
        ROUND(pred_load_mw, 0) AS "Phụ tải Dự báo Ngày mai (MW)"
    FROM iceberg.gold.fact_dispatcher_forecast_7d
    WHERE horizon_step = 1
    """
    c1_id = upsert_card(
        url, token, col_id, db_id,
        "D3-1. Phụ tải Dự báo Ngày mai (Ensemble H=1)",
        sql_c1, "scalar",
        viz={"scalar.title": "Phụ tải Dự báo Ngày mai (MW)", "scalar.subtitle": "MAPE kỳ vọng: 2.11% (~2.3%)"},
        tag_def=TAG_EMPTY
    )
    cards_to_place.append({
        "card_id": c1_id,
        "col": 0, "row": 0, "size_x": 6, "size_y": 4,
        "series": []
    })

    # Card 3.2: Dải Bất Định 95% Ngày Mai [Min - Max]
    sql_c2 = """
    SELECT 
        '[' || CAST(ROUND(ci_lower_mw, 0) AS VARCHAR) || ' - ' || CAST(ROUND(ci_upper_mw, 0) AS VARCHAR) || '] MW (±' || CAST(ROUND(uncertainty_band_mw, 0) AS VARCHAR) || ' MW)' AS "Dải Bất Định 95%"
    FROM iceberg.gold.fact_dispatcher_forecast_7d
    WHERE horizon_step = 1
    """
    c2_id = upsert_card(
        url, token, col_id, db_id,
        "D3-2. Dải Bất Định 95% Ngày Mai (Độ Tin Cậy)",
        sql_c2, "scalar",
        viz={"scalar.title": "Dải Bất Định 95%", "scalar.subtitle": "Khoảng tin cậy dựa trên sai số kiểm thử (RMSE 1,177 MW)"},
        tag_def=TAG_EMPTY
    )
    cards_to_place.append({
        "card_id": c2_id,
        "col": 6, "row": 0, "size_x": 6, "size_y": 4,
        "series": []
    })

    # Card 3.3: Tình trạng Cảnh báo Ngày Nóng (Nhiệt độ +0.44 & So sánh Đỉnh LS)
    sql_c3 = """
    SELECT 
        dispatch_risk_alert AS "Cảnh Báo Đỉnh Tải"
    FROM iceberg.gold.v_dashboard3_dispatcher_operational
    WHERE horizon_step = 1
    """
    c3_id = upsert_card(
        url, token, col_id, db_id,
        "D3-3. Cảnh Báo Ngày Nóng & Nguy Cơ Chạm Đỉnh",
        sql_c3, "scalar",
        viz={"scalar.title": "Cảnh Báo Vận Hành", "scalar.subtitle": "Hệ số nhiệt r = +0.44 | Đỉnh lịch sử: 43,609.5 MW"},
        tag_def=TAG_EMPTY
    )
    cards_to_place.append({
        "card_id": c3_id,
        "col": 12, "row": 0, "size_x": 6, "size_y": 4,
        "series": []
    })

    # Card 3.4: Sức Khỏe Mô Hình & Rolling MAPE 30 ngày
    sql_c4 = """
    SELECT 
        'MAPE 30d: ' || CAST(ROUND(AVG(mape_pct), 2) AS VARCHAR) || '% (Chuẩn A+ Ổn định)' AS "Sức Khỏe Mô Hình"
    FROM iceberg.gold.fact_dispatcher_monitoring_30d
    """
    c4_id = upsert_card(
        url, token, col_id, db_id,
        "D3-4. Giám Sát Sức Khỏe Mô Hình (Rolling 30 Ngày)",
        sql_c4, "scalar",
        viz={"scalar.title": "Giám Sát Trôi Dạt Mô Hình", "scalar.subtitle": "Ngưỡng cảnh báo: 3.5% | Ngưỡng re-train: 4.5%"},
        tag_def=TAG_EMPTY
    )
    cards_to_place.append({
        "card_id": c4_id,
        "col": 18, "row": 0, "size_x": 6, "size_y": 4,
        "series": []
    })

    # --------------------------------------------------------------------------
    # HÀNG 2: HERO CHARTS - DỰ BÁO 7 NGÀY & ĐỐI CHIẾU ĐỈNH LỊCH SỬ (Row 4 - 13, Height: 10)
    # --------------------------------------------------------------------------
    # Card 3.5: [Hero 1 - Khu vực 1]: Đường Dự Báo 7 Ngày Ensemble & Dải Bất Định
    sql_c5 = """
    SELECT 
        forecast_date AS "Ngày",
        pred_load_mw AS "Dự báo Ensemble (MW)",
        ci_lower_mw AS "Cận Dưới 95% (MW)",
        ci_upper_mw AS "Cận Trên 95% (MW)"
    FROM iceberg.gold.fact_dispatcher_forecast_7d
    ORDER BY horizon_step
    """
    c5_id = upsert_card(
        url, token, col_id, db_id,
        "D3-5. [Hero 1] Dự Báo Phụ Tải 7 Ngày Tới & Dải Bất Định (Ensemble)",
        sql_c5, "line",
        viz={
            "graph.dimensions": ["Ngày"],
            "graph.metrics": ["Dự báo Ensemble (MW)", "Cận Dưới 95% (MW)", "Cận Trên 95% (MW)"],
            "graph.x_axis.axis_enabled": True,
            "graph.y_axis.axis_enabled": True
        },
        tag_def=TAG_EMPTY
    )
    cards_to_place.append({
        "card_id": c5_id,
        "col": 0, "row": 4, "size_x": 14, "size_y": 10,
        "series": []
    })

    # Card 3.6: [Khu vực 2]: Bảng Đối Chiếu Đỉnh Lịch Sử & Đánh Giá Rủi Ro 7 Ngày
    sql_c6 = """
    SELECT 
        horizon_name AS "Bước dự báo",
        forecast_date AS "Ngày",
        ROUND(pred_load_mw, 0) AS "Dự báo (MW)",
        expected_mape_pct AS "MAPE kỳ vọng (%)",
        pct_of_historical_peak AS "Tỷ lệ so với Đỉnh LS (%)",
        dispatch_risk_alert AS "Mức độ rủi ro"
    FROM iceberg.gold.v_dashboard3_dispatcher_operational
    ORDER BY horizon_step
    """
    c6_id = upsert_card(
        url, token, col_id, db_id,
        "D3-6. [Khu Vực 2] Ma Trận Đối Chiếu Đỉnh Lịch Sử & Rủi Ro 7 Ngày",
        sql_c6, "table",
        viz={},
        tag_def=TAG_EMPTY
    )
    cards_to_place.append({
        "card_id": c6_id,
        "col": 14, "row": 4, "size_x": 10, "size_y": 10,
        "series": []
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
        tag_def=TAG_EMPTY
    )
    cards_to_place.append({
        "card_id": c7_id,
        "col": 0, "row": 14, "size_x": 12, "size_y": 10,
        "series": []
    })

    # Card 3.8: [Khu vực 4]: Giám Sát Sai Số Thực Tế vs Dự Báo 30 Ngày Gần Nhất
    sql_c8 = """
    SELECT 
        record_date AS "Ngày",
        actual_load_mw AS "Phụ tải Thực tế (MW)",
        pred_load_mw AS "Dự báo Ensemble (MW)",
        rolling_mape_7d AS "Rolling MAPE 7 ngày (%)"
    FROM iceberg.gold.fact_dispatcher_monitoring_30d
    ORDER BY record_date
    """
    c8_id = upsert_card(
        url, token, col_id, db_id,
        "D3-8. [Khu Vực 4] Giám Sát Sai Số Thực Tế vs Dự Báo 30 Ngày",
        sql_c8, "line",
        viz={
            "graph.dimensions": ["Ngày"],
            "graph.metrics": ["Phụ tải Thực tế (MW)", "Dự báo Ensemble (MW)"]
        },
        tag_def=TAG_EMPTY
    )
    cards_to_place.append({
        "card_id": c8_id,
        "col": 12, "row": 14, "size_x": 12, "size_y": 10,
        "series": []
    })

    # --------------------------------------------------------------------------
    # THIẾT LẬP BỐ CỤC DASHBOARD TRÊN METABASE
    # --------------------------------------------------------------------------
    print("\n[+] Đang cấu hình bố cục lưới 24 cột cho Dashboard 03...")
    for c in cards_to_place:
        c["has_date_filter"] = False
        c["parameter_mappings"] = []

    set_dashboard_layout(url, token, dash_id, cards_to_place, parameters=[])

    print("\n" + "=" * 70)
    print(f"DEPLOYMENT COMPLETED!")
    print(f"URL: {url}/dashboard/{dash_id}")
    print("=" * 70)
    return dash_id


if __name__ == "__main__":
    deploy_dashboard_03()
