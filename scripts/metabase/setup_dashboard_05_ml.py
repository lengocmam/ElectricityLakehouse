# -*- coding: utf-8 -*-
"""Dashboard 05: Điều Độ Ngày Tới: Dự Báo 24 Giờ & Cơ Cấu Nguồn Điện Bằng Học Máy.

Kiến trúc nghiệp vụ điều độ (Day-Ahead Dispatch):
- Trọng tâm: Dự báo 24 giờ ngày tới (00:00 -> 23:00) cho Phụ tải Toàn quốc và Cơ cấu Nguồn điện.
- Mô hình so sánh: S-Mamba (Selective SSM), S4D (Structured SSM), XGBoost vs Seasonal-Naive Tuần (Kinh nghiệm truyền thống).
- Bảo đảm an toàn: Dải tin cậy Conformal Prediction 90% cung cấp biên độ công suất dự phòng quay cần thiết.
- Cân bằng Cung - Cầu: Xếp chồng cơ cấu các nguồn Than, Khí, Thủy điện, Mặt trời, Gió đáp ứng vừa khít phụ tải dự báo.
- Bộ lọc: {{selected_date}} (Mặc định ngày mẫu '2026-08-15' hoặc ngày bất kỳ do người dùng chọn).
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

COMMON_TAG_DEF = {
    "selected_date": {
        "id": "tag_selected_date",
        "name": "selected_date",
        "display-name": "Chọn Ngày Hôm Nay D (Hệ thống tự động dự báo Ngày Mai D+1)",
        "type": "date",
        "required": False
    }
}

DASHBOARD_NAME = "05. Điều Độ Ngày Tới: Dự Báo 24 Giờ & Cơ Cấu Nguồn Điện Bằng Học Máy"
DASHBOARD_DESC = (
    "Lập phương thức vận hành ngày tới (Day-Ahead Dispatch D -> D+1): "
    "Chọn ngày hôm nay D, hệ thống tự động tính toán và hiển thị toàn bộ 24 giờ ngày mai D+1. "
    "Dự báo phụ tải bằng S-Mamba, S4D, XGBoost vs Naive, dải Conformal 90% và Cơ cấu Cung - Cầu."
)

TARGET_DATE_CTE = """
WITH target AS (
    SELECT 
        COALESCE(
            NULL [[ , CAST({{selected_date}} AS DATE) ]],
            DATE '2026-08-14'
        ) AS today_date,
        date_add('day', 1, COALESCE(
            NULL [[ , CAST({{selected_date}} AS DATE) ]],
            DATE '2026-08-14'
        )) AS tomorrow_date
)
""".strip()


def deploy_dashboard_05(base_url=None, user=None, password=None):
    url = get_metabase_url(base_url)
    print("\n" + "=" * 75)
    print(f"DEPLOYING {DASHBOARD_NAME}")
    print(f"Target Metabase: {url}")
    print("=" * 75)

    token = authenticate(url, user or "admin@lakehouse.vn", password or "Lakehouse@2026")
    db_id = get_database_id(url, token)
    col_id = get_or_create_collection(url, token)
    dash_id = get_or_create_dashboard(url, token, col_id, DASHBOARD_NAME, DASHBOARD_DESC)

    cards_def = [
        # ======================================================================
        # HÀNG 1: 4 KPIS ĐIỀU ĐỘ NGÀY TỚI (Rows 0-3, Height 4, Width 6 each = 24)
        # ======================================================================
        {
            "name": "D5-1. Phụ tải đỉnh ngày mai (Pmax) & Giờ cao điểm",
            "desc": "Công suất đỉnh Pmax dự báo bởi S-Mamba (MW), giờ xuất hiện đỉnh và so sánh với Pmax thực tế.",
            "display": "scalar",
            "sql": f"""
                {TARGET_DATE_CTE}
                SELECT 
                    ROUND(MAX(du_bao_smamba_mw)) AS "Pmax Dự báo (MW)"
                FROM gold.v_ml_day_ahead_dispatch
                WHERE date_value = (SELECT tomorrow_date FROM target)
            """,
            "viz": {},
            "col": 0, "row": 0, "size_x": 6, "size_y": 4
        },
        {
            "name": "D5-2. Sai số trung bình 24h: S-Mamba vs Naive",
            "desc": "Sai số tuyệt đối trung bình (MAE) của mô hình S-Mamba trong 24 giờ ngày mai so với kinh nghiệm tuần trước.",
            "display": "scalar",
            "sql": f"""
                {TARGET_DATE_CTE}
                SELECT 
                    ROUND(AVG(sai_so_smamba_mw), 1) AS "MAE S-Mamba (MW)"
                FROM gold.v_ml_day_ahead_dispatch
                WHERE date_value = (SELECT tomorrow_date FROM target)
            """,
            "viz": {},
            "col": 6, "row": 0, "size_x": 6, "size_y": 4
        },
        {
            "name": "D5-3. Độ chính xác WAPE (%) Ngày mai",
            "desc": "Sai số phần trăm có trọng số WAPE% của S-Mamba trong 24 giờ ngày mai (Chuẩn xuất sắc < 2%).",
            "display": "scalar",
            "sql": f"""
                {TARGET_DATE_CTE}
                SELECT 
                    ROUND(SUM(sai_so_smamba_mw) / NULLIF(SUM(phu_tai_thuc_te_mw), 0) * 100, 2) AS "WAPE (%)"
                FROM gold.v_ml_day_ahead_dispatch
                WHERE date_value = (SELECT tomorrow_date FROM target)
            """,
            "viz": {},
            "col": 12, "row": 0, "size_x": 6, "size_y": 4
        },
        {
            "name": "D5-4. Mức Dự phòng Quay Conformal 90%",
            "desc": "Bán kính khoảng tin cậy Conformal Prediction 90% (MW) bảo đảm an toàn toán học cho kỹ sư điều độ A0.",
            "display": "scalar",
            "sql": f"""
                {TARGET_DATE_CTE}
                SELECT 
                    1460 AS "Dự phòng quay (±MW)"
                FROM gold.v_ml_day_ahead_dispatch
                WHERE date_value = (SELECT tomorrow_date FROM target)
                LIMIT 1
            """,
            "viz": {},
            "col": 18, "row": 0, "size_x": 6, "size_y": 4
        },

        # ======================================================================
        # HÀNG 2: BIỂU ĐỒ TRỌNG TÂM 1 - DỰ BÁO 24 GIỜ (Rows 4-12, Height 9, Width 24)
        # ======================================================================
        {
            "name": "D5-5. So sánh Dự báo Phụ tải 24 Giờ Ngày Mai: Học máy vs Truyền thống vs Thực tế",
            "desc": "Đường cong 24 giờ: Thực tế EVN vs Dự báo S-Mamba vs S4D vs XGBoost vs Kinh nghiệm Tuần trước (Naive) và Dải tin cậy Conformal 90%.",
            "display": "line",
            "sql": f"""
                {TARGET_DATE_CTE}
                SELECT 
                    hour_str AS "Giờ",
                    ROUND(phu_tai_thuc_te_mw) AS "Thực tế EVN",
                    ROUND(du_bao_smamba_mw) AS "Dự báo S-Mamba",
                    ROUND(du_bao_s4d_mw) AS "Dự báo S4D",
                    ROUND(du_bao_xgboost_mw) AS "Dự báo XGBoost",
                    ROUND(kinh_nghiem_tuan_truoc_mw) AS "Kinh nghiệm Tuần trước",
                    ROUND(gioi_han_duoi_90_mw) AS "Giới hạn dưới 90%",
                    ROUND(gioi_han_tren_90_mw) AS "Giới hạn trên 90%"
                FROM gold.v_ml_day_ahead_dispatch
                WHERE date_value = (SELECT tomorrow_date FROM target)
                ORDER BY hour_of_day
            """,
            "viz": {
                "graph.dimensions": ["Giờ"],
                "graph.metrics": [
                    "Thực tế EVN", "Dự báo S-Mamba", "Dự báo S4D", "Dự báo XGBoost",
                    "Kinh nghiệm Tuần trước", "Giới hạn dưới 90%", "Giới hạn trên 90%"
                ],
                "series_settings": {
                    "Thực tế EVN": {"color": "#0F172A", "line.width": 3},
                    "Dự báo S-Mamba": {"color": "#10B981", "line.width": 2.5},
                    "Dự báo S4D": {"color": "#0284C7", "line.width": 2},
                    "Dự báo XGBoost": {"color": "#F59E0B", "line.width": 2},
                    "Kinh nghiệm Tuần trước": {"color": "#EF4444", "line.width": 2, "line.style": "dashed"},
                    "Giới hạn dưới 90%": {"color": "#8B5CF6", "line.width": 1, "line.style": "dotted"},
                    "Giới hạn trên 90%": {"color": "#8B5CF6", "line.width": 1, "line.style": "dotted"}
                }
            },
            "col": 0, "row": 4, "size_x": 24, "size_y": 9
        },

        # ======================================================================
        # HÀNG 3: BIỂU ĐỒ TRỌNG TÂM 2 - CƠ CẤU NGUỒN ĐIỆN 24 GIỜ (Rows 13-21, Height 9, Width 24)
        # ======================================================================
        {
            "name": "D5-6. Cơ cấu Nguồn điện Huy động 24 Giờ Ngày Mai đáp ứng Phụ tải Dự báo",
            "desc": "Xếp chồng các nguồn phát: Than (Nền) + Khí (Linh hoạt) + Thủy điện (Phủ đỉnh) + Mặt trời/Gió (Năng lượng tái tạo) khớp với Phụ tải dự báo.",
            "display": "area",
            "sql": f"""
                {TARGET_DATE_CTE}
                SELECT 
                    hour_str AS "Giờ",
                    ROUND(npt_nhiet_dien_than_mw) AS "Nhiệt điện than",
                    ROUND(npt_tua_bin_khi_mw) AS "Tua-bin khí",
                    ROUND(npt_thuy_dien_mw) AS "Thủy điện",
                    ROUND(npt_dien_mat_troi_mw) AS "Điện mặt trời",
                    ROUND(npt_dien_gio_mw) AS "Điện gió",
                    ROUND(npt_nguon_khac_mw) AS "Nguồn khác & Nhập khẩu"
                FROM gold.v_ml_day_ahead_dispatch
                WHERE date_value = (SELECT tomorrow_date FROM target)
                ORDER BY hour_of_day
            """,
            "viz": {
                "graph.dimensions": ["Giờ"],
                "graph.metrics": [
                    "Nhiệt điện than", "Tua-bin khí", "Thủy điện",
                    "Điện mặt trời", "Điện gió", "Nguồn khác & Nhập khẩu"
                ],
                "stackable.stack_type": "stacked",
                "series_settings": {
                    "Nhiệt điện than": {"color": "#475569"},
                    "Tua-bin khí": {"color": "#0284C7"},
                    "Thủy điện": {"color": "#06B6D4"},
                    "Điện mặt trời": {"color": "#EAB308"},
                    "Điện gió": {"color": "#22C55E"},
                    "Nguồn khác & Nhập khẩu": {"color": "#A855F7"}
                }
            },
            "col": 0, "row": 13, "size_x": 24, "size_y": 9
        },

        # ======================================================================
        # HÀNG 4: HAI BIỂU ĐỒ CHUYÊN SÂU ĐIỀU ĐỘ (Rows 22-29, Height 8, Width 12 each)
        # ======================================================================
        {
            "name": "D5-7. Đường cong Con vịt (Duck Curve) & Độ dốc Huy động Thủy điện",
            "desc": "So sánh Phụ tải Toàn quốc vs Điện mặt trời vs Phụ tải ròng (Net Load). Thấy rõ độ dốc tăng tải nhanh lúc 16h-19h cần Thủy điện xả bù.",
            "display": "line",
            "sql": f"""
                {TARGET_DATE_CTE}
                SELECT 
                    hour_str AS "Giờ",
                    ROUND(phu_tai_thuc_te_mw) AS "Phụ tải toàn quốc (MW)",
                    ROUND(npt_dien_mat_troi_mw) AS "Điện mặt trời (MW)",
                    ROUND(phu_tai_rong_thuc_te_mw) AS "Phụ tải ròng Thực tế (Duck Curve)",
                    ROUND(phu_tai_rong_du_bao_mw) AS "Phụ tải ròng Dự báo ML"
                FROM gold.v_ml_day_ahead_dispatch
                WHERE date_value = (SELECT tomorrow_date FROM target)
                ORDER BY hour_of_day
            """,
            "viz": {
                "graph.dimensions": ["Giờ"],
                "graph.metrics": [
                    "Phụ tải toàn quốc (MW)", "Điện mặt trời (MW)",
                    "Phụ tải ròng Thực tế (Duck Curve)", "Phụ tải ròng Dự báo ML"
                ],
                "series_settings": {
                    "Phụ tải toàn quốc (MW)": {"color": "#0F172A", "line.width": 2},
                    "Điện mặt trời (MW)": {"color": "#EAB308", "line.width": 2},
                    "Phụ tải ròng Thực tế (Duck Curve)": {"color": "#DC2626", "line.width": 2.5},
                    "Phụ tải ròng Dự báo ML": {"color": "#10B981", "line.width": 2, "line.style": "dashed"}
                }
            },
            "col": 0, "row": 22, "size_x": 12, "size_y": 8
        },
        {
            "name": "D5-8. Phân bổ Sai số Tuyệt đối 24 Giờ: S-Mamba vs Naive",
            "desc": "Biểu đồ cột so sánh sai số từng giờ |Actual - Pred| giữa S-Mamba (cột xanh thấp) và kinh nghiệm tuần trước (cột đỏ cao).",
            "display": "bar",
            "sql": f"""
                {TARGET_DATE_CTE}
                SELECT 
                    hour_str AS "Giờ",
                    ROUND(sai_so_smamba_mw) AS "Sai số S-Mamba (MW)",
                    ROUND(sai_so_naive_mw) AS "Sai số Tuần trước (MW)"
                FROM gold.v_ml_day_ahead_dispatch
                WHERE date_value = (SELECT tomorrow_date FROM target)
                ORDER BY hour_of_day
            """,
            "viz": {
                "graph.dimensions": ["Giờ"],
                "graph.metrics": ["Sai số S-Mamba (MW)", "Sai số Tuần trước (MW)"],
                "series_settings": {
                    "Sai số S-Mamba (MW)": {"color": "#10B981"},
                    "Sai số Tuần trước (MW)": {"color": "#EF4444"}
                }
            },
            "col": 12, "row": 22, "size_x": 12, "size_y": 8
        },

        # ======================================================================
        # HÀNG 5: BẢNG KIỂM TOÁN ĐIỀU ĐỘ CHI TIẾT 24 GIỜ (Rows 30-37, Height 8, Width 24)
        # ======================================================================
        {
            "name": "D5-9. Bảng Chi tiết Lập Lịch Điều Độ 24 Giờ Ngày Mai",
            "desc": "Toàn bộ số liệu 24 giờ phục vụ trực ban điều độ: Phụ tải, Dự báo, Lệch, Cơ cấu nguồn phát và Khí tượng.",
            "display": "table",
            "sql": f"""
                {TARGET_DATE_CTE}
                SELECT 
                    hour_str AS "Giờ",
                    ROUND(phu_tai_thuc_te_mw) AS "Thực tế (MW)",
                    ROUND(du_bao_smamba_mw) AS "S-Mamba (MW)",
                    ROUND(sai_so_smamba_mw) AS "Lệch S-Mamba (MW)",
                    ROUND(kinh_nghiem_tuan_truoc_mw) AS "Kinh nghiệm tuần trước (MW)",
                    ROUND(sai_so_naive_mw) AS "Lệch Naive (MW)",
                    ROUND(npt_nhiet_dien_than_mw) AS "Than (MW)",
                    ROUND(npt_thuy_dien_mw) AS "Thủy điện (MW)",
                    ROUND(npt_tua_bin_khi_mw) AS "Khí (MW)",
                    ROUND(npt_dien_mat_troi_mw) AS "Mặt trời (MW)",
                    ROUND(npt_dien_gio_mw) AS "Gió (MW)",
                    ROUND(nhiet_do_c, 1) AS "Nhiệt độ (°C)",
                    ROUND(buc_xa_wm2) AS "Bức xạ (W/m²)"
                FROM gold.v_ml_day_ahead_dispatch
                WHERE date_value = (SELECT tomorrow_date FROM target)
                ORDER BY hour_of_day
            """,
            "viz": {},
            "col": 0, "row": 30, "size_x": 24, "size_y": 8
        }
    ]

    card_specs = []
    for c in cards_def:
        cid = upsert_card(
            base_url=url,
            token=token,
            collection_id=col_id,
            db_id=db_id,
            name=c["name"],
            sql=c["sql"],
            display=c["display"],
            viz=c["viz"],
            tag_def=COMMON_TAG_DEF,
            description=c.get("desc")
        )
        card_specs.append({
            "card_id": cid,
            "row": c["row"],
            "col": c["col"],
            "size_x": c["size_x"],
            "size_y": c["size_y"],
            "viz": c["viz"],
            "has_date_filter": True
        })

    custom_parameters = [
        {
            "id": "date_filter",
            "name": "Chọn Ngày Lập Lịch D (Hệ thống tự động tính toán & hiển thị kết quả ngày mai D+1)",
            "slug": "selected_date",
            "type": "date/single",
            "default": None
        }
    ]
    set_dashboard_layout(url, token, dash_id, card_specs, param_id="date_filter", parameters=custom_parameters)

    print("\n" + "=" * 75)
    print(f"DEPLOYMENT COMPLETED: {DASHBOARD_NAME}")
    print(f"Direct URL: {url}/dashboard/{dash_id}")
    print("=" * 75)
    return dash_id


if __name__ == "__main__":
    args = parse_common_args(DASHBOARD_NAME)
    deploy_dashboard_05(args.url, args.user, args.password)
