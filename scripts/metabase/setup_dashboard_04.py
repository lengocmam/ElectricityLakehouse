# -*- coding: utf-8 -*-
"""Dashboard 04: Giám Sát Vận Hành Pipeline & Chất Lượng Dữ Liệu Lakehouse (DataOps & SRE).

Kiến trúc giám sát 5 trụ cột vững chắc:
1. pipeline_dag_audit: Kiểm tra vòng đời và SLA của từng DAG.
2. pipeline_task_audit: Giám sát từng tác vụ, phát hiện bất thường thời lượng (% lố vs Median/P75).
3. Đọc/Ghi/Từ chối: Lưu lượng throughput và số dòng dữ liệu bị loại trừ do vi phạm chất lượng.
4. alert_event: Bảng quản lý và phân loại sự cố (CRITICAL, WARNING, INFO).
5. data_lineage: Bản đồ phả hệ dòng chảy dữ liệu từ External API -> Bronze -> Silver -> Gold.
"""
import os
import sys

# Ensure local imports work regardless of execution directory
sys.stdout.reconfigure(encoding="utf-8")
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
)

DASHBOARD_NAME = "04. Giám Sát Vận Hành Pipeline & Chất Lượng Dữ Liệu Lakehouse"
DASHBOARD_DESC = (
    "Trung tâm Điều hành Dữ liệu (DataOps Center): Giám sát SLA vòng đời DAGs, phát hiện "
    "bất thường thời lượng (% lố vs Trung vị & P75), lưu lượng Đọc/Ghi/Loại trừ, sự cố cảnh báo và Phả hệ Lineage."
)


def deploy_dashboard_04(base_url=None, user=None, password=None):
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
            "name": "Tình Trạng Vận Hành Toàn Bộ DAGs (DAG SLA %)",
            "display": "scalar",
            "sql": """
                SELECT 
                    ROUND(COUNT(CASE WHEN state = 'SUCCESS' THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), 1) AS dag_success_pct
                FROM pipeline_dag_audit
                WHERE 1=1
                [[ AND CAST(start_time AS DATE) = CAST({{selected_date}} AS DATE) ]]
            """,
            "viz": {}, "col": 0, "row": 0, "size_x": 6, "size_y": 4
        },
        {
            "name": "Tổng Số Dòng Dữ Liệu Đã Đọc & Ghi (Total Throughput)",
            "display": "scalar",
            "sql": """
                SELECT 
                    TO_CHAR(COALESCE(SUM(records_read), 0) + COALESCE(SUM(records_written), 0), 'FM999,999,999') AS total_records_processed
                FROM pipeline_task_audit
                WHERE task_id != 'start'
                [[ AND CAST(start_time AS DATE) = CAST({{selected_date}} AS DATE) ]]
            """,
            "viz": {}, "col": 6, "row": 0, "size_x": 6, "size_y": 4
        },
        {
            "name": "Dữ Liệu Bị Loại Trừ / Làm Sạch (Total Records Rejected)",
            "display": "scalar",
            "sql": """
                SELECT 
                    TO_CHAR(COALESCE(SUM(records_rejected), 0), 'FM999,999,999') AS total_records_rejected
                FROM pipeline_task_audit
                WHERE task_id != 'start'
                [[ AND CAST(start_time AS DATE) = CAST({{selected_date}} AS DATE) ]]
            """,
            "viz": {}, "col": 12, "row": 0, "size_x": 6, "size_y": 4
        },
        {
            "name": "Cảnh Báo & Sự Cố Hệ Thống (Alerts Logged)",
            "display": "scalar",
            "sql": """
                SELECT 
                    CASE 
                        WHEN COUNT(CASE WHEN severity = 'CRITICAL' THEN 1 END) > 0 
                            THEN '🔴 ' || COUNT(CASE WHEN severity = 'CRITICAL' THEN 1 END) || ' CRITICAL'
                        WHEN COUNT(CASE WHEN severity = 'WARNING' THEN 1 END) > 0 
                            THEN '🟡 ' || COUNT(CASE WHEN severity = 'WARNING' THEN 1 END) || ' WARNING'
                        ELSE '🟢 HỆ THỐNG AN TOÀN'
                    END AS system_alert_status
                FROM alert_event
                WHERE 1=1
                [[ AND CAST(created_at AS DATE) = CAST({{selected_date}} AS DATE) ]]
            """,
            "viz": {}, "col": 18, "row": 0, "size_x": 6, "size_y": 4
        },

        # ----------------------------------------------------------------------
        # NHỊP 2: 2 HERO CHARTS - LƯU LƯỢNG DATA & PHÁT HIỆN LỐ THỜI GIAN (Rows 4-13)
        # ----------------------------------------------------------------------
        {
            "name": "Lưu Lượng Dòng Dữ Liệu: Đọc (Read) vs Ghi (Written) vs Từ Chối (Rejected)",
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
            "col": 0, "row": 4, "size_x": 12, "size_y": 10
        },
        {
            "name": "Cảnh Báo Bất Thường: % Lố Thời Lượng Tác Vụ (vs Trung Vị & P75)",
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
            "col": 12, "row": 4, "size_x": 12, "size_y": 10
        },

        # ----------------------------------------------------------------------
        # NHỊP 3: QUẢN LÝ VÒNG ĐỜI DAG & SỰ CỐ CẢNH BÁO (Rows 14-23)
        # ----------------------------------------------------------------------
        {
            "name": "Giám Sát Vòng Đời Từng DAG (DAG Lifecycle Audit)",
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
            "viz": {}, "col": 0, "row": 14, "size_x": 12, "size_y": 10
        },
        {
            "name": "Bảng Quản Lý Sự Cố & Cảnh Báo (Alert Events Log)",
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
            "viz": {}, "col": 12, "row": 14, "size_x": 12, "size_y": 10
        },

        # ----------------------------------------------------------------------
        # NHỊP 4: PHẢ HỆ DÒNG CHẢY DỮ LIỆU & KIỂM TOÁN TÁC VỤ (Rows 24-33)
        # ----------------------------------------------------------------------
        {
            "name": "Bản Đồ Phả Hệ Dòng Chảy Dữ Liệu (Data Lineage: Source -> Bronze -> Silver -> Gold)",
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
            "viz": {}, "col": 0, "row": 24, "size_x": 12, "size_y": 10,
            "has_date_filter": False
        },
        {
            "name": "Bảng Nhật Ký Kiểm Toán Tác Vụ Chi Tiết (Task Execution Audit Matrix)",
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
            "viz": {}, "col": 12, "row": 24, "size_x": 12, "size_y": 10
        }
    ]

    card_specs = []
    for c in cards_def:
        cid = upsert_card(url, token, col_id, db_id, c["name"], c["sql"], c["display"], c["viz"])
        card_specs.append({
            "card_id": cid, "col": c["col"], "row": c["row"],
            "size_x": c["size_x"], "size_y": c["size_y"], "viz": c["viz"],
            "has_date_filter": c.get("has_date_filter", True)
        })

    set_dashboard_layout(url, token, dash_id, card_specs)
    print(f"[✓] Dashboard 04 ready at: {url}/dashboard/{dash_id}\n")
    return dash_id


if __name__ == "__main__":
    args = parse_common_args("Dashboard 04")
    deploy_dashboard_04(args.url, args.user, args.password)
