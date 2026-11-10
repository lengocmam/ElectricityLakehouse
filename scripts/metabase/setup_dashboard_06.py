#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Metabase Dashboard 06 Provisioner: Dự Báo AI & Đánh Giá Mô Hình Học Máy.

Dashboard: 06. Dự Báo AI & Đánh Giá Mô Hình Học Máy (Machine Learning & Deep Learning)
Bao gồm đầy đủ 4 Hồi nghiệp vụ khoa học:
- Hồi 1: 4 KPIs Độ Chính Xác Mô Hình Tốt Nhất (Phụ tải & Thủy điện, H1 & H7)
- Hồi 2: Ma Trận Tương Quan Tuyển Chọn Đặc Trưng (Pearson, Spearman, Trễ Lags, VIF)
- Hồi 3: Đấu Trường Thuật Toán (LightGBM vs S4D vs TimeXer vs S-Mamba vs Ensemble) & Phân tích Horizon MAE
- Hồi 4: Trực Quan Hóa Vận Hành 24 Giờ Thực Tế (Intraday Hourly Load Forecast)
"""

import sys
import os

if hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

try:
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
except ImportError:
    from scripts.metabase.common import (
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

DASHBOARD_NAME = "06. Dự Báo AI & Đánh Giá Mô Hình Học Máy (Machine Learning & Deep Learning)"
DASHBOARD_DESC = (
    "Nghiên cứu dự báo phụ tải điện và thủy điện bằng mô hình Không gian trạng thái S4D, "
    "TimeXer (NeurIPS 2024), S-Mamba (2024) và LightGBM. Đi từ Ma trận tương quan tuyển chọn "
    "đặc trưng khoa học đến kiểm định mô hình thực tế trên lưới điện EVN."
)

CTE = """
WITH target_date AS (
    SELECT COALESCE(
        NULL [[ , (YEAR(CAST({{selected_date}} AS DATE)) * 10000 + MONTH(CAST({{selected_date}} AS DATE)) * 100 + DAY(CAST({{selected_date}} AS DATE))) ]],
        (SELECT MAX(date_key) FROM iceberg.gold.fact_forecast_hourly)
    ) AS date_key
)
""".strip()


def deploy_dashboard_06(base_url=None, user=None, password=None):
    url = get_metabase_url(base_url)
    print("\n" + "=" * 70)
    print(f"DEPLOYING {DASHBOARD_NAME}")
    print(f"Target Metabase: {url}")
    print("=" * 70)

    token = authenticate(url, user, password)
    db_id = get_database_id(url, token)
    col_id = get_or_create_collection(url, token)
    dash_id = get_or_create_dashboard(url, token, col_id, DASHBOARD_NAME, DASHBOARD_DESC)

    cards_def = [
        # ======================================================================
        # NHỊP 1: 4 KPIs ĐỘ CHÍNH XÁC MÔ HÌNH TỐT NHẤT (Rows 0-3, Height 4)
        # ======================================================================
        {
            "name": "1. Độ Chính Xác Dự Báo Phụ Tải 1 Ngày (%)",
            "desc": "Độ chính xác cao nhất đạt 97.91% (MAE = 858 MW) thuộc về LightGBM cho bài toán Day-Ahead (H=1).",
            "display": "scalar",
            "sql": """
                SELECT accuracy_pct AS "accuracy"
                FROM iceberg.gold.fact_forecast_benchmark 
                WHERE task_code = 'load_H1' AND rank_order = 1
            """,
            "viz": {
                "column_settings": {
                    '["name","accuracy"]': {"column_title": "Độ Chính Xác Phụ Tải 1 Ngày (%)"}
                }
            },
            "col": 0, "row": 0, "size_x": 6, "size_y": 4,
            "has_date_filter": False
        },
        {
            "name": "2. Độ Chính Xác Dự Báo Phụ Tải 7 Ngày (%)",
            "desc": "Độ chính xác cao nhất đạt 96.24% (MAE = 1,545 MW) thuộc về Tổ Hợp Ensemble 3 Mô Hình Bài Báo (S4D + TimeXer + S-Mamba) cho bài toán Week-Ahead (H=7).",
            "display": "scalar",
            "sql": """
                SELECT accuracy_pct AS "accuracy"
                FROM iceberg.gold.fact_forecast_benchmark 
                WHERE task_code = 'load_H7' AND rank_order = 1
            """,
            "viz": {
                "column_settings": {
                    '["name","accuracy"]': {"column_title": "Độ Chính Xác Phụ Tải 7 Ngày (%)"}
                }
            },
            "col": 6, "row": 0, "size_x": 6, "size_y": 4,
            "has_date_filter": False
        },
        {
            "name": "3. Độ Chính Xác Dự Báo Thủy Điện 1 Ngày (%)",
            "desc": "Độ chính xác cao nhất đạt 93.99% (MAE = 14,598 MWh) thuộc về LightGBM cho dự báo sản lượng thủy điện khả dụng 1 ngày tới.",
            "display": "scalar",
            "sql": """
                SELECT accuracy_pct AS "accuracy"
                FROM iceberg.gold.fact_forecast_benchmark 
                WHERE task_code = 'hydro_H1' AND rank_order = 1
            """,
            "viz": {
                "column_settings": {
                    '["name","accuracy"]': {"column_title": "Độ Chính Xác Thủy Điện 1 Ngày (%)"}
                }
            },
            "col": 12, "row": 0, "size_x": 6, "size_y": 4,
            "has_date_filter": False
        },
        {
            "name": "4. Độ Chính Xác Dự Báo Thủy Điện 7 Ngày (%)",
            "desc": "Độ chính xác cao nhất đạt 90.14% (MAE = 23,994 MWh) thuộc về Tổ Hợp Deep Learning (S-Mamba + TimeXer + S4D) cho dự báo thủy điện tuần tới.",
            "display": "scalar",
            "sql": """
                SELECT accuracy_pct AS "accuracy"
                FROM iceberg.gold.fact_forecast_benchmark 
                WHERE task_code = 'hydro_H7' AND rank_order = 1
            """,
            "viz": {
                "column_settings": {
                    '["name","accuracy"]': {"column_title": "Độ Chính Xác Thủy Điện 7 Ngày (%)"}
                }
            },
            "col": 18, "row": 0, "size_x": 6, "size_y": 4,
            "has_date_filter": False
        },

        # ======================================================================
        # NHỊP 2: CƠ SỞ KHOA HỌC - DANH SÁCH ĐẶC TRƯNG TƯƠNG QUAN CAO (Rows 4-13)
        # ======================================================================
        {
            "name": "5. Danh Sách Đặc Trưng Tương Quan Cao Được Tuyển Chọn - Bài Toán Phụ Tải (|r| >= 0.35)",
            "desc": "Chỉ lấy các đặc trưng có hệ số tương quan cao (|r| >= 0.35) và loại bỏ hoàn toàn các biến rác, biến yếu hoặc biến đa cộng tuyến.",
            "display": "table",
            "sql": """
                SELECT 
                    rank_order AS "Hang",
                    feature_label AS "Dac_Trung",
                    feature_group AS "Nhom",
                    pearson_r AS "Pearson_r",
                    spearman_rho AS "Spearman_rho",
                    '✅ ĐƯỢC CHỌN' AS "Trang_Thai",
                    selection_reason AS "Co_So_Khoa_Hoc"
                FROM iceberg.gold.fact_feature_correlation
                WHERE task_code = 'load_forecasting' 
                  AND selection_status LIKE '%ĐƯỢC CHỌN%'
                  AND pearson_abs >= 0.35
                ORDER BY rank_order
            """,
            "viz": {
                "column_settings": {
                    '["name","Hang"]': {"column_title": "Hạng"},
                    '["name","Dac_Trung"]': {"column_title": "Đặc Trưng Tuyển Chọn"},
                    '["name","Nhom"]': {"column_title": "Nhóm Biến"},
                    '["name","Pearson_r"]': {"column_title": "Pearson (r)"},
                    '["name","Spearman_rho"]': {"column_title": "Spearman (ρ)"},
                    '["name","Trang_Thai"]': {"column_title": "Trạng Thái"},
                    '["name","Co_So_Khoa_Hoc"]': {"column_title": "Cơ Sở Khoa Học Điều Độ"}
                }
            },
            "col": 0, "row": 4, "size_x": 12, "size_y": 10,
            "has_date_filter": False
        },
        {
            "name": "6. Danh Sách Đặc Trưng Tương Quan Cao Được Tuyển Chọn - Bài Toán Thủy Điện (|r| >= 0.40)",
            "desc": "Chỉ lấy các đặc trưng thủy văn - khí tượng có tương quan cao (|r| >= 0.40) và tương quan trễ lũ 7 ngày phục vụ dự báo thủy điện.",
            "display": "table",
            "sql": """
                SELECT 
                    rank_order AS "Hang",
                    feature_label AS "Dac_Trung",
                    feature_group AS "Nhom",
                    pearson_r AS "Pearson_r",
                    spearman_rho AS "Spearman_rho",
                    lag_7_r AS "Tre_7_Ngay",
                    '✅ ĐƯỢC CHỌN' AS "Trang_Thai",
                    selection_reason AS "Co_So_Khoa_Hoc"
                FROM iceberg.gold.fact_feature_correlation
                WHERE task_code = 'hydro_forecasting' 
                  AND selection_status LIKE '%ĐƯỢC CHỌN%'
                  AND pearson_abs >= 0.40
                ORDER BY rank_order
            """,
            "viz": {
                "column_settings": {
                    '["name","Hang"]': {"column_title": "Hạng"},
                    '["name","Dac_Trung"]': {"column_title": "Đặc Trưng Tuyển Chọn"},
                    '["name","Nhom"]': {"column_title": "Nhóm Biến"},
                    '["name","Pearson_r"]': {"column_title": "Pearson (r)"},
                    '["name","Spearman_rho"]': {"column_title": "Spearman (ρ)"},
                    '["name","Tre_7_Ngay"]': {"column_title": "Trễ 7 Ngày (r)"},
                    '["name","Trang_Thai"]': {"column_title": "Trạng Thái"},
                    '["name","Co_So_Khoa_Hoc"]': {"column_title": "Cơ Sở Khoa Học Điều Độ"}
                }
            },
            "col": 12, "row": 4, "size_x": 12, "size_y": 10,
            "has_date_filter": False
        },

        # ======================================================================
        # NHỊP 3: ĐẤU TRƯỜNG THUẬT TOÁN & PHÂN TÍCH HORIZON (Rows 14-23)
        # ======================================================================
        {
            "name": "7. Bảng Đánh Giá Xếp Hạng 4 Thuật Toán Dự Báo & Baselines",
            "desc": "So sánh định lượng toàn diện giữa LightGBM, S4D, TimeXer, S-Mamba, Tổ Hợp Ensemble và các Baselines theo MAE, MAPE, RMSE và R².",
            "display": "table",
            "sql": """
                SELECT 
                    task_name AS "Bai_Toan",
                    model_name AS "Mo_Hinh",
                    ROUND(mae, 1) AS "MAE",
                    unit AS "Don_Vi",
                    ROUND(mape_pct, 2) AS "MAPE_Pct",
                    ROUND(accuracy_pct, 2) AS "Do_Chinh_Xac_Pct",
                    ROUND(r2_score, 4) AS "He_So_R2",
                    rank_order AS "Hang"
                FROM iceberg.gold.fact_forecast_benchmark
                WHERE task_code IN ('load_H1', 'load_H7', 'hydro_H1', 'hydro_H7')
                ORDER BY task_code, rank_order
            """,
            "viz": {
                "column_settings": {
                    '["name","Bai_Toan"]': {"column_title": "Bài Toán Dự Báo"},
                    '["name","Mo_Hinh"]': {"column_title": "Thuật Toán / Mô Hình"},
                    '["name","MAE"]': {"column_title": "Sai Số MAE"},
                    '["name","Don_Vi"]': {"column_title": "Đơn Vị"},
                    '["name","MAPE_Pct"]': {"column_title": "MAPE (%)"},
                    '["name","Do_Chinh_Xac_Pct"]': {"column_title": "Độ Chính Xác (%)"},
                    '["name","He_So_R2"]': {"column_title": "Hệ Số Giải Thích R²"},
                    '["name","Hang"]': {"column_title": "Thứ Hạng"}
                }
            },
            "col": 0, "row": 14, "size_x": 12, "size_y": 10,
            "has_date_filter": False
        },
        {
            "name": "8. So Sánh Sai Số MAE Theo Từng Bước Dự Báo Từ Ngày 1 Đến Ngày 7 (MW)",
            "desc": "Minh chứng khoa học: LightGBM tốt nhất ở Ngày +1, nhưng từ Ngày +3 đến Ngày +7, S4D Deep Learning và Tổ Hợp AI vượt trội hơn hẳn mô hình cây.",
            "display": "line",
            "sql": """
                SELECT 
                    horizon_name AS "Buoc_Ngay",
                    ROUND(lightgbm_mae, 1) AS "LightGBM_MW",
                    ROUND(s4d_mae, 1) AS "S4D_MW",
                    ROUND(timexer_mae, 1) AS "TimeXer_MW",
                    ROUND(smamba_mae, 1) AS "S_Mamba_MW",
                    ROUND(ensemble_mae, 1) AS "To_Hop_AI_MW"
                FROM iceberg.gold.fact_forecast_horizon_mae
                WHERE task_code = 'load_H7'
                ORDER BY horizon_step
            """,
            "viz": {
                "graph.dimensions": ["Buoc_Ngay"],
                "graph.metrics": [
                    "LightGBM_MW",
                    "S4D_MW",
                    "TimeXer_MW",
                    "S_Mamba_MW",
                    "To_Hop_AI_MW"
                ],
                "graph.x_axis.title_text": "Tầm dự báo từng ngày (Horizon Day +1 -> Day +7)",
                "graph.y_axis.title_text": "Sai số tuyệt đối trung bình MAE (MW)",
                "series_settings": {
                    "LightGBM_MW": {"color": "#475569", "title": "LightGBM (Baseline)", "line.width": 2},
                    "S4D_MW": {"color": "#2563EB", "title": "S4D Deep Learning", "line.width": 2},
                    "TimeXer_MW": {"color": "#10B981", "title": "TimeXer (NeurIPS 2024)", "line.width": 2},
                    "S_Mamba_MW": {"color": "#F59E0B", "title": "S-Mamba (2024)", "line.width": 2},
                    "To_Hop_AI_MW": {"color": "#DC2626", "title": "Tổ Hợp AI (Tốt nhất H=7)", "line.width": 3}
                },
                "column_settings": {
                    '["name","Buoc_Ngay"]': {"column_title": "Bước Ngày"},
                    '["name","LightGBM_MW"]': {"column_title": "LightGBM (MW)"},
                    '["name","S4D_MW"]': {"column_title": "S4D Deep Learning (MW)"},
                    '["name","TimeXer_MW"]': {"column_title": "TimeXer (MW)"},
                    '["name","S_Mamba_MW"]': {"column_title": "S-Mamba (MW)"},
                    '["name","To_Hop_AI_MW"]': {"column_title": "Tổ Hợp AI (MW)"}
                }
            },
            "col": 12, "row": 14, "size_x": 12, "size_y": 10,
            "has_date_filter": False
        },

        # ======================================================================
        # NHỊP 4: VẬN HÀNH THỰC TẾ 24 GIỜ (Rows 24-34, Height 10)
        # ======================================================================
        {
            "name": "9. Dự Báo Phụ Tải Từng Giờ Tiếp Theo: Thực Tế vs S4D Deep Learning vs Tổ Hợp AI vs Baseline Hôm Qua (MW)",
            "desc": "Trực quan hóa năng lực dự báo phụ tải từng giờ (Intraday Forecast): Đối chiếu đường cong thực tế EVN với S4D, Tổ Hợp AI và Baseline hôm qua cùng giờ.",
            "display": "line",
            "sql": f"""
                {CTE}
                SELECT 
                    f.hour_label AS "Gio",
                    f.actual_load_mw AS "Thuc_Te_EVN_MW",
                    f.ensemble_forecast_mw AS "Du_Bao_To_Hop_AI_MW",
                    f.s4d_forecast_mw AS "Du_Bao_S4D_MW",
                    f.baseline_lag24h_mw AS "Baseline_Hom_Qua_MW"
                FROM iceberg.gold.v_dashboard6_hourly_forecast f
                JOIN target_date td ON f.date_key = td.date_key
                ORDER BY f.hour
            """,
            "viz": {
                "graph.dimensions": ["Gio"],
                "graph.metrics": [
                    "Thuc_Te_EVN_MW",
                    "Du_Bao_To_Hop_AI_MW",
                    "Du_Bao_S4D_MW",
                    "Baseline_Hom_Qua_MW"
                ],
                "graph.show_values": False,
                "graph.x_axis.title_text": "Giờ trong ngày (01:00 - 23:00)",
                "graph.y_axis.title_text": "Công suất phụ tải toàn quốc (MW)",
                "graph.y_axis.auto_split": False,
                "series_settings": {
                    "Thuc_Te_EVN_MW": {
                        "title": "Thực Tế EVN (MW)",
                        "color": "#0F172A",
                        "line.style": "solid",
                        "line.width": 3
                    },
                    "Du_Bao_To_Hop_AI_MW": {
                        "title": "Dự Báo Tổ Hợp AI (MW) [MAE: 948 MW, WAPE: 2.3%]",
                        "color": "#DC2626",
                        "line.style": "solid",
                        "line.width": 3
                    },
                    "Du_Bao_S4D_MW": {
                        "title": "Dự Báo S4D Deep Learning (MW)",
                        "color": "#2563EB",
                        "line.style": "solid",
                        "line.width": 2
                    },
                    "Baseline_Hom_Qua_MW": {
                        "title": "Baseline Hôm Qua Cùng Giờ (MW) [Lag 24h, MAE: 3.007 MW]",
                        "color": "#94A3B8",
                        "line.style": "dashed",
                        "line.width": 2
                    }
                },
                "column_settings": {
                    '["name","Gio"]': {"column_title": "Giờ trong ngày"},
                    '["name","Thuc_Te_EVN_MW"]': {"column_title": "Thực Tế EVN (MW)"},
                    '["name","Du_Bao_To_Hop_AI_MW"]': {"column_title": "Dự Báo Tổ Hợp AI (MW)"},
                    '["name","Du_Bao_S4D_MW"]': {"column_title": "Dự Báo S4D Deep Learning (MW)"},
                    '["name","Baseline_Hom_Qua_MW"]': {"column_title": "Baseline Hôm Qua Cùng Giờ (MW)"}
                }
            },
            "col": 0, "row": 24, "size_x": 24, "size_y": 10,
            "has_date_filter": True
        }
    ]

    card_specs = []
    for c in cards_def:
        cid = upsert_card(
            url, token, col_id, db_id, c["name"], c["sql"], c["display"],
            c["viz"], tag_def=COMMON_TAG_DEF, description=c.get("desc")
        )
        card_specs.append({
            "card_id": cid, "col": c["col"], "row": c["row"],
            "size_x": c["size_x"], "size_y": c["size_y"], "viz": c["viz"],
            "has_date_filter": c.get("has_date_filter", True)
        })

    set_dashboard_layout(url, token, dash_id, card_specs)
    print(f"\n[✓] Dashboard 06 successfully deployed with 9 CARDS at:")
    print(f"    {url}/dashboard/{dash_id}\n")
    return dash_id


if __name__ == "__main__":
    args = parse_common_args("Dashboard 06")
    deploy_dashboard_06(args.url, args.user, args.password)
