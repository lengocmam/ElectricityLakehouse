# -*- coding: utf-8 -*-
"""Dashboard 03: Giám Sát Cảnh Báo Vận Hành Pipeline & Chất Lượng Dữ Liệu Lakehouse (DataOps & SRE Center).

Kiến trúc giám sát 5 trụ cột vững chắc theo quy chuẩn DataOps:
1. pipeline_dag_audit: Giám sát vòng đời, trạng thái và SLA % của toàn bộ DAGs.
2. alert_event: Bảng quản lý và phân loại sự cố cảnh báo thời gian thực (🔴 CRITICAL, 🟡 WARNING, 🔵 INFO).
3. Đọc / Ghi / Cách ly từ chối: Lưu lượng throughput và số dòng dữ liệu vi phạm chất lượng bị loại trừ.
4. pipeline_task_audit: Phát hiện bất thường thời lượng (% lố thời gian so với Trung vị P50 & Phân vị P75).
5. data_lineage: Bản đồ phả hệ dòng chảy dữ liệu minh bạch từ External API -> Bronze -> Silver -> Gold.
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
    COMMON_TAG_DEF,
)

DASHBOARD_NAME = "04. Giám Sát Vận Hành Pipeline & Chất Lượng Dữ Liệu DataOps"
DASHBOARD_DESC = (
    "Trung tâm Điều hành Dữ liệu (DataOps Center): Giám sát SLA vòng đời DAGs, cảnh báo sự cố thời gian thực, "
    "phát hiện bất thường thời lượng (% lố vs Trung vị & P75), lưu lượng Đọc/Ghi/Loại trừ và Phả hệ Lineage."
)

TAG_DEF_DATE = COMMON_TAG_DEF


def deploy_dashboard_03(base_url=None, user=None, password=None):
    url = get_metabase_url(base_url)
    print("\n" + "=" * 70)
    print(f"DEPLOYING {DASHBOARD_NAME}")
    print(f"Target Metabase: {url}")
    print("=" * 70)

    token = authenticate(url, user or "admin@lakehouse.vn", password or "Lakehouse@2026")
    db_id = get_database_id(url, token, engine_type="postgres")
    col_id = get_or_create_collection(url, token)
    dash_id = get_or_create_dashboard(url, token, col_id, DASHBOARD_NAME, DASHBOARD_DESC)

    cards_def = [
        # ----------------------------------------------------------------------
        # NHỊP 1: 4 KPIs DATAOPS THỜI GIAN THỰC (Rows 0-3, Height 4)
        # ----------------------------------------------------------------------
        {
            "name": "D4-1. Tỷ lệ hoàn thành tác vụ (SLA)",
            "desc": "Tỷ lệ DAG hoàn thành thành công theo tiêu chuẩn SLA DataOps.",
            "display": "scalar",
            "sql": """
                SELECT 
                    ROUND(COUNT(CASE WHEN state = 'SUCCESS' THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), 1) AS "Tỷ Lệ SLA (%)"
                FROM pipeline_dag_audit
                WHERE 1=1
                [[ AND CAST(start_time AS DATE) = CAST({{selected_date}} AS DATE) ]]
            """,
            "viz": {
                "scalar.title": "TỶ LỆ SLA HOÀN THÀNH DAG",
                "scalar.subtitle": "Chuẩn DataOps: 100% SUCCESS | Ngưỡng cảnh báo < 99%"
            },
            "col": 0, "row": 0, "size_x": 6, "size_y": 4,
            "has_date_filter": True
        },
        {
            "name": "D4-2. Trạng thái cảnh báo pipeline",
            "desc": "Trạng thái cảnh báo và số lượng sự cố phát sinh theo mức độ nghiêm trọng.",
            "display": "scalar",
            "sql": """
                SELECT 
                    CASE 
                        WHEN COUNT(CASE WHEN severity = 'CRITICAL' THEN 1 END) > 0 
                            THEN '🔴 ' || COUNT(CASE WHEN severity = 'CRITICAL' THEN 1 END) || ' CRITICAL'
                        WHEN COUNT(CASE WHEN severity = 'WARNING' THEN 1 END) > 0 
                            THEN '🟡 ' || COUNT(CASE WHEN severity = 'WARNING' THEN 1 END) || ' WARNING'
                        ELSE '🟢 HỆ THỐNG AN TOÀN'
                    END AS "Trạng Thái Cảnh Báo"
                FROM alert_event
                WHERE 1=1
                [[ AND CAST(created_at AS DATE) = CAST({{selected_date}} AS DATE) ]]
            """,
            "viz": {
                "scalar.title": "TRẠNG THÁI CẢNH BÁO PIPELINE",
                "scalar.subtitle": "Phân loại: 🔴 CRITICAL | 🟡 WARNING | 🟢 AN TOÀN"
            },
            "col": 6, "row": 0, "size_x": 6, "size_y": 4,
            "has_date_filter": True
        },
        {
            "name": "D4-3. Tổng lưu lượng xử lý dữ liệu",
            "desc": "Tổng khối lượng bản ghi dữ liệu đã được đọc và nạp thành công qua các tầng.",
            "display": "scalar",
            "sql": """
                SELECT 
                    TO_CHAR(COALESCE(SUM(records_read), 0) + COALESCE(SUM(records_written), 0), 'FM999,999,999') || ' dòng' AS "Tổng Lưu Lượng"
                FROM pipeline_task_audit
                WHERE task_id != 'start'
                [[ AND CAST(start_time AS DATE) = CAST({{selected_date}} AS DATE) ]]
            """,
            "viz": {
                "scalar.title": "TỔNG DỮ LIỆU ĐÃ XỬ LÝ (THROUGHPUT)",
                "scalar.subtitle": "Tổng số bản ghi Đọc (Read) và Ghi (Written)"
            },
            "col": 12, "row": 0, "size_x": 6, "size_y": 4,
            "has_date_filter": True
        },
        {
            "name": "D4-4. Tỷ lệ dữ liệu bị cách ly",
            "desc": "Số lượng bản ghi vi phạm kiểm tra chất lượng (Data Quality Rule) bị cách ly.",
            "display": "scalar",
            "sql": """
                SELECT 
                    TO_CHAR(COALESCE(SUM(records_rejected), 0), 'FM999,999,999') || ' dòng' AS "Dữ Liệu Loại Trừ"
                FROM pipeline_task_audit
                WHERE task_id != 'start'
                [[ AND CAST(start_time AS DATE) = CAST({{selected_date}} AS DATE) ]]
            """,
            "viz": {
                "scalar.title": "DỮ LIỆU BỊ CÁCH LY / TỪ CHỐI",
                "scalar.subtitle": "Bản ghi vi phạm chất lượng bị cách ly ở bảng reject"
            },
            "col": 18, "row": 0, "size_x": 6, "size_y": 4,
            "has_date_filter": True
        },

        # ----------------------------------------------------------------------
        # NHỊP 2: 2 HERO CHARTS - LƯU LƯỢNG DATA & PHÁT HIỆN LỐ THỜI GIAN (Rows 4-13)
        # ----------------------------------------------------------------------
        {
            "name": "D4-5. Cân bằng dòng dữ liệu Bronze - Silver",
            "desc": "So sánh trực quan số dòng đọc, ghi và từ chối theo từng tác vụ Bronze, Silver, Gold.",
            "display": "bar",
            "sql": """
                SELECT 
                    CASE 
                        WHEN task_id LIKE 'bronze%' THEN '1. Bronze: ' || split_part(task_id, '.', 2)
                        WHEN task_id LIKE 'silver%' THEN '2. Silver: ' || split_part(task_id, '.', 2)
                        WHEN task_id LIKE 'gold%' THEN '3. Gold: ' || split_part(task_id, '.', 2)
                        ELSE task_id 
                    END AS "Tác Vụ Pipeline",
                    COALESCE(SUM(records_read), 0) AS "Đọc (Read)",
                    COALESCE(SUM(records_written), 0) AS "Ghi (Written)",
                    COALESCE(SUM(records_rejected), 0) AS "Từ Chối (Rejected)"
                FROM pipeline_task_audit
                WHERE task_id != 'start'
                [[ AND CAST(start_time AS DATE) = CAST({{selected_date}} AS DATE) ]]
                GROUP BY task_id
                ORDER BY "Đọc (Read)" DESC
                LIMIT 10
            """,
            "viz": {
                "graph.dimensions": ["Tác Vụ Pipeline"],
                "graph.metrics": ["Đọc (Read)", "Ghi (Written)", "Từ Chối (Rejected)"],
                "graph.y_axis.title_text": "Số lượng bản ghi (records)",
                "graph.x_axis.title_text": "Tác Vụ Pipeline"
            },
            "col": 0, "row": 4, "size_x": 12, "size_y": 10,
            "has_date_filter": True
        },
        {
            "name": "D4-6. Tác vụ vượt ngưỡng thời lượng",
            "desc": "Phát hiện tắc nghẽn: So sánh thời lượng thực tế với Trung vị (P50) và Phân vị P75.",
            "display": "bar",
            "sql": """
                WITH task_stats AS (
                    SELECT 
                        task_id,
                        ROUND(AVG(duration_seconds)::numeric, 1) AS avg_duration,
                        ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY duration_seconds)::numeric, 1) AS median_duration,
                        ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY duration_seconds)::numeric, 1) AS p75_duration
                    FROM pipeline_task_audit
                    WHERE task_id != 'start' AND duration_seconds IS NOT NULL
                    GROUP BY task_id
                ),
                latest_task_run AS (
                    SELECT DISTINCT ON (task_id)
                        task_id,
                        duration_seconds AS actual_duration,
                        start_time,
                        run_id
                    FROM pipeline_task_audit
                    WHERE task_id != 'start' AND duration_seconds IS NOT NULL
                    [[ AND CAST(start_time AS DATE) = CAST({{selected_date}} AS DATE) ]]
                    ORDER BY task_id, start_time DESC
                )
                SELECT 
                    CASE 
                        WHEN l.task_id LIKE 'bronze%' THEN '1. Bronze: ' || split_part(l.task_id, '.', 2)
                        WHEN l.task_id LIKE 'silver%' THEN '2. Silver: ' || split_part(l.task_id, '.', 2)
                        WHEN l.task_id LIKE 'gold%' THEN '3. Gold: ' || split_part(l.task_id, '.', 2)
                        ELSE l.task_id 
                    END AS "Tác Vụ Pipeline",
                    ROUND(((l.actual_duration - s.median_duration) / NULLIF(s.median_duration, 0) * 100.0)::numeric, 1) AS "% Lố vs Trung Vị P50",
                    ROUND(((l.actual_duration - s.p75_duration) / NULLIF(s.p75_duration, 0) * 100.0)::numeric, 1) AS "% Lố vs Tứ Phân Vị P75"
                FROM latest_task_run l
                JOIN task_stats s ON l.task_id = s.task_id
                ORDER BY "% Lố vs Trung Vị P50" DESC
                LIMIT 12
            """,
            "viz": {
                "graph.dimensions": ["Tác Vụ Pipeline"],
                "graph.metrics": ["% Lố vs Trung Vị P50", "% Lố vs Tứ Phân Vị P75"],
                "graph.y_axis.title_text": "Tỷ lệ vượt ngưỡng (%)",
                "graph.x_axis.title_text": "Tác Vụ Pipeline (Sắp xếp theo độ lố giảm dần)"
            },
            "col": 12, "row": 4, "size_x": 12, "size_y": 10,
            "has_date_filter": True
        },

        # ----------------------------------------------------------------------
        # NHỊP 3: BẢNG CHỈ HUY SỰ CỐ & VÒNG ĐỜI DAG (Rows 14-23, Height 10)
        # ----------------------------------------------------------------------
        {
            "name": "D4-7. Quản lý sự cố DataOps",
            "desc": "Danh mục sự cố thời gian thực: Mã lỗi, Phân loại, DAG, Task, Thông báo Telegram và nội dung lỗi.",
            "display": "table",
            "sql": """
                SELECT 
                    alert_id AS "Mã Cảnh Báo",
                    CASE 
                        WHEN severity = 'CRITICAL' THEN '🔴 CRITICAL'
                        WHEN severity = 'WARNING' THEN '🟡 WARNING'
                        ELSE '🔵 INFO' 
                    END AS "Mức Độ",
                    alert_type AS "Phân Loại Sự Cố",
                    dag_id AS "DAG Gặp Lỗi",
                    task_id AS "Task Phát Sinh",
                    status AS "Trạng Thái",
                    notification_status AS "Thông Báo Telegram",
                    message AS "Nội Dung Chi Tiết",
                    TO_CHAR(created_at, 'YYYY-MM-DD HH24:MI:SS') AS "Thời Điểm Ghi Nhận"
                FROM alert_event
                WHERE 1=1
                [[ AND CAST(created_at AS DATE) = CAST({{selected_date}} AS DATE) ]]
                ORDER BY created_at DESC
                LIMIT 20
            """,
            "viz": {},
            "col": 0, "row": 14, "size_x": 12, "size_y": 10,
            "has_date_filter": True
        },
        {
            "name": "D4-8. Vòng đời thực thi DAG Airflow",
            "desc": "Kiểm toán vòng đời DAG: Thời lượng chạy, thời điểm bắt đầu/kết thúc và trạng thái hoàn thành.",
            "display": "table",
            "sql": """
                SELECT 
                    dag_id AS "DAG Pipeline",
                    run_id AS "Mã Lần Chạy (Run ID)",
                    CASE 
                        WHEN state = 'SUCCESS' THEN '🟢 SUCCESS'
                        WHEN state = 'FAILED' THEN '🔴 FAILED'
                        ELSE state 
                    END AS "Trạng Thái",
                    ROUND(duration_seconds::numeric, 1) AS "Thời Lượng (s)",
                    TO_CHAR(start_time, 'YYYY-MM-DD HH24:MI:SS') AS "Bắt Đầu",
                    TO_CHAR(end_time, 'YYYY-MM-DD HH24:MI:SS') AS "Kết Thúc"
                FROM pipeline_dag_audit
                WHERE 1=1
                [[ AND CAST(start_time AS DATE) = CAST({{selected_date}} AS DATE) ]]
                ORDER BY start_time DESC
                LIMIT 15
            """,
            "viz": {},
            "col": 12, "row": 14, "size_x": 12, "size_y": 10,
            "has_date_filter": True
        },

        # ----------------------------------------------------------------------
        # NHỊP 4: PHẢ HỆ DÒNG CHẢY DỮ LIỆU & KIỂM TOÁN TÁC VỤ (Rows 24-33, Height 10)
        # ----------------------------------------------------------------------
        {
            "name": "D4-9. Phả hệ dòng chảy dữ liệu",
            "desc": "Minh bạch hóa dòng chảy: External Source -> Bronze -> Silver -> Gold kèm tác vụ Spark/ETL phụ trách.",
            "display": "table",
            "sql": """
                SELECT 
                    source_name AS "Nguồn (Source)",
                    source_layer AS "Tầng Nguồn",
                    '➡️' AS "Dòng Chảy",
                    target_name AS "Đích Đến (Target)",
                    target_layer AS "Tầng Đích",
                    COALESCE(transformation_name, task_id) AS "Tác Vụ Xử Lý (Spark/ETL)",
                    TO_CHAR(created_at, 'YYYY-MM-DD HH24:MI:SS') AS "Thời Điểm Ghi Nhận"
                FROM data_lineage
                ORDER BY 
                    CASE source_layer 
                        WHEN 'external' THEN 1 
                        WHEN 'bronze' THEN 2 
                        WHEN 'silver' THEN 3 
                        ELSE 4 
                    END, source_name
            """,
            "viz": {},
            "col": 0, "row": 24, "size_x": 12, "size_y": 10,
            "has_date_filter": False
        },
        {
            "name": "D4-10. Bảng đối soát tác vụ pipeline",
            "desc": "Chi tiết từng lần chạy task: Số lần thử lại, thời gian thực thi, cờ bất thường, dòng đọc/ghi/loại trừ.",
            "display": "table",
            "sql": """
                SELECT 
                    task_id AS "Tác Vụ (Task ID)",
                    try_number AS "Lần Thử",
                    CASE 
                        WHEN state = 'SUCCESS' THEN '🟢 SUCCESS'
                        WHEN state = 'UP_FOR_RETRY' THEN '🟡 RETRY'
                        WHEN state = 'FAILED' THEN '🔴 FAILED'
                        ELSE state 
                    END AS "Trạng Thái",
                    ROUND(duration_seconds::numeric, 1) AS "Thời Gian (s)",
                    CASE WHEN is_anomaly THEN '⚠️ Bất thường' ELSE '🟢 Bình thường' END AS "Cảnh Báo Thời Lượng",
                    COALESCE(records_read, 0) AS "Đọc (Read)",
                    COALESCE(records_written, 0) AS "Ghi (Written)",
                    COALESCE(records_rejected, 0) AS "Loại Trừ (Rejected)",
                    TO_CHAR(start_time, 'YYYY-MM-DD HH24:MI:SS') AS "Bắt Đầu",
                    COALESCE(anomaly_reason, error_message, 'Hoàn thành bình thường') AS "Ghi Chú Vận Hành"
                FROM pipeline_task_audit
                WHERE task_id != 'start'
                [[ AND CAST(start_time AS DATE) = CAST({{selected_date}} AS DATE) ]]
                ORDER BY start_time DESC NULLS LAST
                LIMIT 50
            """,
            "viz": {},
            "col": 12, "row": 24, "size_x": 12, "size_y": 10,
            "has_date_filter": True
        }
    ]

    card_specs = []
    for c in cards_def:
        cid = upsert_card(
            url, token, col_id, db_id,
            c["name"], c["sql"], c["display"],
            viz=c["viz"], tag_def=TAG_DEF_DATE if c.get("has_date_filter", True) else None,
            description=c.get("desc")
        )
        card_specs.append({
            "card_id": cid,
            "col": c["col"],
            "row": c["row"],
            "size_x": c["size_x"],
            "size_y": c["size_y"],
            "has_date_filter": c.get("has_date_filter", True)
        })

    set_dashboard_layout(url, token, dash_id, card_specs, param_id="date_filter")

    print(f"\n[SUCCESS] Deployed 10 cards on {DASHBOARD_NAME} (Dashboard ID: {dash_id})!")
    print(f"URL: {url}/dashboard/{dash_id}\n")
    return dash_id


if __name__ == "__main__":
    deploy_dashboard_03()
