#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
cleanup_dashboards.py
Dọn dẹp và tinh gọn hệ thống Dashboard:
- Giữ lại duy nhất 2 Dashboard chính thức trên Metabase:
    + Dashboard 01: Cân Bằng Cung - Cầu & Chuyển Dịch Năng Lượng (id: 12)
    + Dashboard 02: Thời Tiết Cực Đoan, An Ninh Nước & Thủy Điện (id: 13)
- Xóa toàn bộ các dashboard khác trên Metabase (Dashboard 1, 4, 5, 6, 7, 8, 10, 11...).
- Xóa dashboard_app.html và các file cấu hình cho dashboard thừa.
"""

import os
import sys
import json
import urllib.request
import urllib.error

METABASE_URL = os.environ.get("METABASE_URL", "http://localhost:3000")
ADMIN_USER = os.environ.get("METABASE_USER", "admin@lakehouse.vn")
ADMIN_PASS = os.environ.get("METABASE_PASS", "Lakehouse@2026")

KEEP_DASHBOARD_IDS = {12, 13}


def get_session_token():
    req = urllib.request.Request(
        f"{METABASE_URL}/api/session",
        data=json.dumps({"username": ADMIN_USER, "password": ADMIN_PASS}).encode("utf-8"),
        headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read().decode("utf-8"))["id"]


def metabase_api(token, endpoint, method="GET", data=None):
    url = f"{METABASE_URL}{endpoint}"
    payload = json.dumps(data).encode("utf-8") if data is not None else None
    req = urllib.request.Request(
        url,
        data=payload,
        method=method,
        headers={"X-Metabase-Session": token, "Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(req) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw) if raw.strip() else {}
    except urllib.error.HTTPError as e:
        err_msg = e.read().decode("utf-8")
        print(f"    [API Error] {method} {endpoint} -> HTTP {e.code}: {err_msg}")
        return None
    except Exception as ex:
        print(f"    [Request Error] {method} {endpoint} -> {ex}")
        return None


def main():
    print("=" * 80)
    print("TIẾN TRÌNH DỌN DẸP HỆ THỐNG DASHBOARD METABASE & MÃ NGUỒN")
    print(f"Mục tiêu: GIỮ LẠI DUY NHẤT Dashboard 12 & 13. Xóa toàn bộ phần còn lại.")
    print("=" * 80)

    # 1. Đăng nhập Metabase
    try:
        token = get_session_token()
        print(f"[✓] Đăng nhập Metabase thành công tại {METABASE_URL}!")
    except Exception as e:
        print(f"[!] Không thể kết nối Metabase: {e}")
        return

    # 2. Thu thập danh sách thẻ (cards) của 2 dashboard cần giữ lại
    keep_cards = set()
    for did in KEEP_DASHBOARD_IDS:
        dash_data = metabase_api(token, f"/api/dashboard/{did}")
        if dash_data:
            cids = [dc["card_id"] for dc in dash_data.get("dashcards", []) if dc.get("card_id")]
            keep_cards.update(cids)
            print(f"[✓] Dashboard {did} ('{dash_data.get('name')}') đang hoạt động với {len(cids)} cards (BẢO VỆ).")
        else:
            print(f"[!] CẢNH BÁO: Không tìm thấy Dashboard {did}!")

    # 3. Lấy toàn bộ danh sách Dashboard hiện có trên Metabase
    all_dashboards = metabase_api(token, "/api/dashboard") or []
    print(f"\n[+] Tổng số Dashboard hiện có trên Metabase: {len(all_dashboards)}")

    deleted_dash_count = 0
    for d in all_dashboards:
        did = d["id"]
        dname = d["name"]
        if did in KEEP_DASHBOARD_IDS:
            print(f"  --> GIỮ LẠI: Dashboard {did} - '{dname}'")
            continue

        print(f"  [X] Đang xóa Dashboard {did} - '{dname}'...")
        # Thử xóa trực tiếp hoặc archive trước rồi xóa
        res_del = metabase_api(token, f"/api/dashboard/{did}", method="DELETE")
        if res_del is None:
            # Nếu DELETE không hỗ trợ trực tiếp, gọi PUT archive
            metabase_api(token, f"/api/dashboard/{did}", method="PUT", data={"archived": True})
            print(f"      -> Đã archive Dashboard {did}.")
        else:
            print(f"      -> Đã xóa vĩnh viễn Dashboard {did}.")
        deleted_dash_count += 1

    print(f"[✓] Đã dọn dẹp {deleted_dash_count} dashboard thừa trên Metabase!")

    # 4. Dọn dẹp các thẻ (cards) mồ côi không thuộc Dashboard 12 & 13
    all_cards = metabase_api(token, "/api/card") or []
    deleted_card_count = 0
    for c in all_cards:
        cid = c["id"]
        cname = c["name"]
        if cid not in keep_cards:
            # Archive card mồ côi
            metabase_api(token, f"/api/card/{cid}", method="PUT", data={"archived": True})
            deleted_card_count += 1

    print(f"[✓] Đã archive {deleted_card_count} thẻ mồ côi (giữ lại nguyên vẹn {len(keep_cards)} thẻ của Dash 12 & 13).")

    # 5. Xóa các tệp file cấu hình và dashboard html cục bộ
    files_to_delete = [
        "dashboard_app.html",
        "scripts/dashboard_data.json",
        "scripts/extract_dashboard_data.py",
        "scripts/metabase/setup_dashboard_03.py",
        "scripts/metabase/setup_dashboard_04.py",
        "scripts/metabase/setup_dashboard_05.py",
        "scripts/metabase/setup_dashboard_06.py",
        "config/trino/views/05_dashboard5_cross_source.sql",
        "config/trino/views/06_dashboard6_ml_forecasting.sql",
    ]

    print("\n[+] Đang xóa các tệp cục bộ không dùng đến...")
    for fpath in files_to_delete:
        abs_p = os.path.join(os.getcwd(), fpath)
        if os.path.exists(abs_p):
            os.remove(abs_p)
            print(f"  [✓] Đã xóa: {fpath}")
        else:
            print(f"  [-] Không tồn tại: {fpath}")

    # 6. Kiểm tra lại danh sách dashboard còn lại trên Metabase
    remaining_dashboards = metabase_api(token, "/api/dashboard") or []
    active_dashboards = [d for d in remaining_dashboards if not d.get("archived")]
    print("\n" + "=" * 80)
    print("DANH SÁCH DASHBOARD CHÍNH THỨC CÒN LẠI TRÊN METABASE:")
    print("=" * 80)
    for d in active_dashboards:
        print(f"  ★ Dashboard {d['id']:02d}: {d['name']} -> {METABASE_URL}/dashboard/{d['id']}")
    print("=" * 80)


if __name__ == "__main__":
    main()
