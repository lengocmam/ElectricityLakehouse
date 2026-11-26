# -*- coding: utf-8 -*-
"""Shared Utilities for Metabase Lakehouse Dashboard Provisioning.

Provides reusable functions to:
- Connect & authenticate with Metabase (auto-detect host or docker container URL).
- Auto-discover Trino/Iceberg database ID.
- Get or create collections & dashboards idempotently.
- Upsert cards (create if new, update query/settings if exists).
- Configure interactive date filters and 2-screen grid layouts.
"""
import argparse
import json
import os
import sys
import urllib.error
import urllib.request

DEFAULT_USER = os.getenv("METABASE_USER", "admin@lakehouse.vn")
DEFAULT_PASS = os.getenv("METABASE_PASSWORD", "Lakehouse@2026")
DEFAULT_COLLECTION = "Vietnam Electricity Lakehouse BI"

COMMON_TAG_DEF = {
    "selected_date": {
        "id": "tag_selected_date",
        "name": "selected_date",
        "display-name": "Chọn Ngày (Để trống = Tự động lấy ngày mới nhất)",
        "type": "date",
        "required": False
    }
}

TAGS_DATE_AND_PLANT = {
    "selected_date": {
        "id": "tag_selected_date",
        "name": "selected_date",
        "display-name": "Chọn Ngày (Để trống = Tự động lấy ngày mới nhất)",
        "type": "date",
        "required": False
    },
    "selected_plant": {
        "id": "tag_selected_plant",
        "name": "selected_plant",
        "display-name": "Chọn Hồ Thủy Điện",
        "type": "text",
        "required": False
    }
}

def get_target_date_cte(fallback_table="iceberg.gold.fact_generation_daily"):
    """Generate dynamic target_date CTE that defaults to latest date in Lakehouse."""
    return f"""
    WITH target_date AS (
        SELECT COALESCE(
            NULL [[ , (YEAR(CAST({{{{selected_date}}}} AS DATE)) * 10000 + MONTH(CAST({{{{selected_date}}}} AS DATE)) * 100 + DAY(CAST({{{{selected_date}}}} AS DATE))) ]],
            (SELECT MAX(date_key) FROM {fallback_table})
        ) AS date_key
    )
    """.strip()


def get_metabase_url(cli_url=None):
    """Determine reachability of Metabase URL (tries CLI url, localhost:3000, then metabase:3000)."""
    candidates = []
    if cli_url:
        candidates.append(cli_url.rstrip("/"))
    candidates.extend(["http://127.0.0.1:3000", "http://localhost:3000", "http://metabase:3000"])

    for url in candidates:
        try:
            req = urllib.request.Request(f"{url}/api/health", headers={"User-Agent": "LakehouseClient/1.0"})
            with urllib.request.urlopen(req, timeout=3) as resp:
                if resp.status == 200:
                    return url
        except Exception:
            continue

    # Fallback to localhost:3000
    return cli_url or "http://localhost:3000"


def authenticate(base_url, user=DEFAULT_USER, password=DEFAULT_PASS):
    """Obtain session token from Metabase."""
    url = f"{base_url}/api/session"
    payload = json.dumps({"username": user, "password": password}).encode("utf-8")
    req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            return data["id"]
    except urllib.error.HTTPError as e:
        err = e.read().decode("utf-8", errors="ignore")
        print(f"[!] Authentication failed on {base_url} ({e.code}): {err}")
        sys.exit(1)
    except Exception as e:
        print(f"[!] Connection error to {base_url}: {e}")
        sys.exit(1)


def get_database_id(base_url, token, engine_type="trino"):
    """Auto-detect Trino/Iceberg or Postgres database ID."""
    req = urllib.request.Request(f"{base_url}/api/database", headers={"X-Metabase-Session": token})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            databases = json.loads(resp.read().decode("utf-8")).get("data", [])
            for db in databases:
                engine = db.get("engine", "").lower()
                name = db.get("name", "").lower()
                if engine_type == "postgres":
                    if "postgres" in engine or "control" in name or "monitoring" in name:
                        return db["id"]
                else:
                    if "trino" in engine or "starburst" in engine or "iceberg" in name or "trino" in name:
                        return db["id"]
            if databases:
                return databases[0]["id"]
    except Exception as e:
        print(f"[*] Warning: Could not auto-detect database ID ({e}), defaulting to 2")
    return 2


def get_or_create_collection(base_url, token, name=DEFAULT_COLLECTION):
    """Retrieve existing collection ID or create a new one."""
    req = urllib.request.Request(f"{base_url}/api/collection", headers={"X-Metabase-Session": token})
    with urllib.request.urlopen(req, timeout=10) as resp:
        collections = json.loads(resp.read().decode("utf-8"))
        for c in collections:
            if c.get("name") == name:
                return c["id"]

    # Create collection
    create_req = urllib.request.Request(
        f"{base_url}/api/collection",
        data=json.dumps({"name": name, "color": "#509EE3"}).encode("utf-8"),
        headers={"X-Metabase-Session": token, "Content-Type": "application/json"}
    )
    with urllib.request.urlopen(create_req, timeout=10) as resp:
        new_col = json.loads(resp.read().decode("utf-8"))
        print(f"[+] Created Collection '{name}' (ID: {new_col['id']})")
        return new_col["id"]


def get_or_create_dashboard(base_url, token, collection_id, name, description):
    """Retrieve existing dashboard ID (by exact name or matching scope) or create a new one."""
    req = urllib.request.Request(f"{base_url}/api/dashboard", headers={"X-Metabase-Session": token})
    dash_num = name[:2] if len(name) >= 2 and name[:2].isdigit() else ""
    with urllib.request.urlopen(req, timeout=10) as resp:
        dashboards = json.loads(resp.read().decode("utf-8"))
        for d in dashboards:
            d_name = d.get("name", "")
            d_col = d.get("collection_id")
            matched = False
            if d_name == name and d_col == collection_id:
                matched = True
            elif d_col == collection_id and dash_num:
                if dash_num in ("01", "02", "05") and d_name.startswith(f"{dash_num}."):
                    matched = True
                elif dash_num == "03" and ("03. Chu Kỳ" in d_name or "03. Phân Tích Chu Kỳ" in d_name):
                    matched = True
                elif dash_num == "04" and ("03. Giám Sát Cảnh Báo" in d_name or "04." in d_name):
                    matched = True

            if matched:
                did = d["id"]
                if d_name != name or d.get("description") != description:
                    update_d_req = urllib.request.Request(
                        f"{base_url}/api/dashboard/{did}",
                        data=json.dumps({"name": name, "description": description}).encode("utf-8"),
                        headers={"X-Metabase-Session": token, "Content-Type": "application/json"},
                        method="PUT"
                    )
                    urllib.request.urlopen(update_d_req, timeout=10)
                    print(f"[✓] Updated Dashboard {did}: '{name}'")
                return did

    # Create dashboard
    create_req = urllib.request.Request(
        f"{base_url}/api/dashboard",
        data=json.dumps({"name": name, "description": description, "collection_id": collection_id}).encode("utf-8"),
        headers={"X-Metabase-Session": token, "Content-Type": "application/json"}
    )
    with urllib.request.urlopen(create_req, timeout=10) as resp:
        new_dash = json.loads(resp.read().decode("utf-8"))
        print(f"[+] Created Dashboard '{name}' (ID: {new_dash['id']})")
        return new_dash["id"]


def find_matching_card(cards, collection_id, name):
    """Find existing card by exact name or by unique card code prefix."""
    # 1. Exact match
    for c in cards:
        if c.get("collection_id") == collection_id and c.get("name") == name:
            return c
    # 2. Match by card code prefix (e.g. D1-1, D2-1, D3-1, D4-1, D5-1)
    if "." in name:
        code = name.split(".")[0].strip()
        for c in cards:
            if c.get("collection_id") != collection_id:
                continue
            c_name = c.get("name", "").strip()

            # Special case for Dashboard 03 (Seasonal): must match S3- or D3- in seasonal cards (ID 58-63)
            if code.startswith("D3-"):
                sub_idx = code[3:]
                if c.get("id") in range(58, 64):
                    if c_name.startswith(f"S3-{sub_idx}.") or c_name.startswith(f"D3-{sub_idx}."):
                        return c
                elif c_name.startswith(f"S3-{sub_idx}."):
                    return c

            # Special case for Dashboard 04 (DataOps): must match D4- or D3- in DataOps cards (ID 64-73)
            elif code.startswith("D4-"):
                sub_idx = code[3:]
                if c.get("id") in range(64, 74):
                    if c_name.startswith(f"D4-{sub_idx}.") or c_name.startswith(f"D3-{sub_idx}."):
                        return c
                elif c_name.startswith(f"D4-{sub_idx}."):
                    return c

            elif c_name.startswith(f"{code}."):
                return c
    return None


def upsert_card(base_url, token, collection_id, db_id, name, sql, display, viz=None, tag_def=None, description=None):
    """Create card if not existing, or update query & visualization if already exists."""
    req = urllib.request.Request(f"{base_url}/api/card", headers={"X-Metabase-Session": token})
    with urllib.request.urlopen(req, timeout=60) as resp:
        cards = json.loads(resp.read().decode("utf-8"))
        existing = find_matching_card(cards, collection_id, name)

    payload = {
        "name": name,
        "collection_id": collection_id,
        "dataset_query": {
            "database": db_id,
            "type": "native",
            "native": {
                "query": sql.strip(),
                "template-tags": tag_def or COMMON_TAG_DEF
            }
        },
        "display": display,
        "visualization_settings": viz or {}
    }
    if description:
        payload["description"] = description

    if existing:
        cid = existing["id"]
        update_req = urllib.request.Request(
            f"{base_url}/api/card/{cid}",
            data=json.dumps(payload).encode("utf-8"),
            headers={"X-Metabase-Session": token, "Content-Type": "application/json"},
            method="PUT"
        )
        with urllib.request.urlopen(update_req, timeout=60) as resp:
            print(f"  [✓] Updated Card {cid}: '{name}' ({display})")
            return cid
    else:
        create_req = urllib.request.Request(
            f"{base_url}/api/card",
            data=json.dumps(payload).encode("utf-8"),
            headers={"X-Metabase-Session": token, "Content-Type": "application/json"}
        )
        with urllib.request.urlopen(create_req, timeout=60) as resp:
            new_card = json.loads(resp.read().decode("utf-8"))
            cid = new_card["id"]
            print(f"  [+] Created Card {cid}: '{name}' ({display})")
            return cid


def set_dashboard_layout(base_url, token, dashboard_id, card_specs, param_id="date_filter", parameters=None):
    """Position cards on dashboard and configure dynamic interactive filters."""
    # 1. Configure dashboard parameter(s)
    if parameters is None:
        parameters = [
            {
                "id": param_id,
                "name": "Chọn Ngày (Để trống = Tự động cập nhật ngày mới nhất)",
                "slug": "selected_date",
                "type": "date/single",
                "default": None
            }
        ]
    d_param_payload = {"parameters": parameters}
    req_p = urllib.request.Request(
        f"{base_url}/api/dashboard/{dashboard_id}",
        data=json.dumps(d_param_payload).encode("utf-8"),
        headers={"X-Metabase-Session": token, "Content-Type": "application/json"},
        method="PUT"
    )
    urllib.request.urlopen(req_p, timeout=10)

    # 2. Configure dashcards with parameter mappings
    dashcards = []
    for i, spec in enumerate(card_specs, start=1):
        if "parameter_mappings" in spec:
            mapping = spec["parameter_mappings"]
        else:
            mapping = []
            if spec.get("has_date_filter", True):
                mapping.append({
                    "parameter_id": "date_filter",
                    "card_id": spec["card_id"],
                    "target": ["variable", ["template-tag", "selected_date"]]
                })
            if spec.get("has_plant_filter", False):
                mapping.append({
                    "parameter_id": "plant_filter",
                    "card_id": spec["card_id"],
                    "target": ["variable", ["template-tag", "selected_plant"]]
                })

        dashcards.append({
            "id": -i,
            "card_id": spec["card_id"],
            "row": spec["row"],
            "col": spec["col"],
            "size_x": spec["size_x"],
            "size_y": spec["size_y"],
            "parameter_mappings": mapping,
            "visualization_settings": spec.get("viz", {})
        })

    req_c = urllib.request.Request(
        f"{base_url}/api/dashboard/{dashboard_id}/cards",
        data=json.dumps({"cards": dashcards}).encode("utf-8"),
        headers={"X-Metabase-Session": token, "Content-Type": "application/json"},
        method="PUT"
    )
    urllib.request.urlopen(req_c, timeout=10)
    print(f"[✓] Dashboard {dashboard_id}: Configured {len(dashcards)} cards with layout and filters!\n")


def parse_common_args(dashboard_name):
    """Standard CLI argument parser for dashboard scripts."""
    parser = argparse.ArgumentParser(description=f"Provision Metabase {dashboard_name}")
    parser.add_argument("--url", default=None, help="Metabase base URL (e.g. http://localhost:3000)")
    parser.add_argument("--user", default=DEFAULT_USER, help="Admin username/email")
    parser.add_argument("--password", default=DEFAULT_PASS, help="Admin password")
    return parser.parse_args()
