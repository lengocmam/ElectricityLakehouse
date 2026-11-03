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
    candidates.extend(["http://localhost:3000", "http://metabase:3000", "http://127.0.0.1:3000"])

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
        with urllib.request.urlopen(req, timeout=10) as resp:
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
    """Retrieve existing dashboard ID or create a new one."""
    req = urllib.request.Request(f"{base_url}/api/dashboard", headers={"X-Metabase-Session": token})
    with urllib.request.urlopen(req, timeout=10) as resp:
        dashboards = json.loads(resp.read().decode("utf-8"))
        for d in dashboards:
            if d.get("name") == name and d.get("collection_id") == collection_id:
                return d["id"]

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


def upsert_card(base_url, token, collection_id, db_id, name, sql, display, viz=None, tag_def=None):
    """Create card if not existing, or update query & visualization if already exists."""
    req = urllib.request.Request(f"{base_url}/api/card", headers={"X-Metabase-Session": token})
    with urllib.request.urlopen(req, timeout=10) as resp:
        cards = json.loads(resp.read().decode("utf-8"))
        existing = next((c for c in cards if c.get("name") == name and c.get("collection_id") == collection_id), None)

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

    if existing:
        cid = existing["id"]
        update_req = urllib.request.Request(
            f"{base_url}/api/card/{cid}",
            data=json.dumps(payload).encode("utf-8"),
            headers={"X-Metabase-Session": token, "Content-Type": "application/json"},
            method="PUT"
        )
        with urllib.request.urlopen(update_req, timeout=10) as resp:
            print(f"  [✓] Updated Card {cid}: '{name}' ({display})")
            return cid
    else:
        create_req = urllib.request.Request(
            f"{base_url}/api/card",
            data=json.dumps(payload).encode("utf-8"),
            headers={"X-Metabase-Session": token, "Content-Type": "application/json"}
        )
        with urllib.request.urlopen(create_req, timeout=10) as resp:
            new_card = json.loads(resp.read().decode("utf-8"))
            cid = new_card["id"]
            print(f"  [+] Created Card {cid}: '{name}' ({display})")
            return cid


def set_dashboard_layout(base_url, token, dashboard_id, card_specs, param_id="date_filter"):
    """Position cards on dashboard and configure dynamic interactive date filter."""
    # 1. Configure dashboard parameter
    d_param_payload = {
        "parameters": [
            {
                "id": param_id,
                "name": "Chọn Ngày (Để trống = Tự động cập nhật ngày mới nhất)",
                "slug": "selected_date",
                "type": "date/single",
                "default": None
            }
        ]
    }
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
        mapping = []
        if spec.get("has_date_filter", True):
            mapping = [
                {
                    "parameter_id": param_id,
                    "card_id": spec["card_id"],
                    "target": ["variable", ["template-tag", "selected_date"]]
                }
            ]
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
    print(f"[✓] Dashboard {dashboard_id}: Configured {len(dashcards)} cards with layout and date filter!\n")


def parse_common_args(dashboard_name):
    """Standard CLI argument parser for dashboard scripts."""
    parser = argparse.ArgumentParser(description=f"Provision Metabase {dashboard_name}")
    parser.add_argument("--url", default=None, help="Metabase base URL (e.g. http://localhost:3000)")
    parser.add_argument("--user", default=DEFAULT_USER, help="Admin username/email")
    parser.add_argument("--password", default=DEFAULT_PASS, help="Admin password")
    return parser.parse_args()
