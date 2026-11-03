# -*- coding: utf-8 -*-
import os
import sys
import json
import urllib.request
sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from common import get_metabase_url, authenticate

METABASE_URL = get_metabase_url()
USER = "admin@lakehouse.vn"
PASS = "Lakehouse@2026"

def get_session():
    return authenticate(METABASE_URL, USER, PASS)

token = get_session()

dashboards = [5, 4, 6]

for d_id in dashboards:
    req = urllib.request.Request(f"{METABASE_URL}/api/dashboard/{d_id}", headers={"X-Metabase-Session": token})
    d = json.loads(urllib.request.urlopen(req).read().decode("utf-8"))
    print(f"\n=======================================================")
    print(f"DASHBOARD {d_id}: {d['name']}")
    print(f"=======================================================")
    
    for dc in d.get("dashcards", []):
        card = dc.get("card", {})
        cid = card.get("id")
        cname = card.get("name")
        display = card.get("display")
        
        # Test executing the card query
        try:
            q_req = urllib.request.Request(
                f"{METABASE_URL}/api/card/{cid}/query",
                data=b"{}",
                headers={"X-Metabase-Session": token, "Content-Type": "application/json"}
            )
            q_res = json.loads(urllib.request.urlopen(q_req).read().decode("utf-8"))
            status = q_res.get("status")
            data = q_res.get("data", {})
            rows = data.get("rows", [])
            cols = [c["name"] for c in data.get("cols", [])]
            row_count = len(rows)
            sample = rows[0] if row_count > 0 else "EMPTY"
            print(f"  [OK] Card {cid} ({display}) '{cname}' -> status: {status}, cols: {cols}, rows: {row_count}, sample: {sample}")
        except urllib.error.HTTPError as e:
            err_body = e.read().decode('utf-8', errors='ignore')
            print(f"  [FAIL HTTP {e.code}] Card {cid} ({display}) '{cname}' -> Error: {err_body[:200]}")
        except Exception as e:
            print(f"  [ERROR] Card {cid} ({display}) '{cname}' -> {e}")
