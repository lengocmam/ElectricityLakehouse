import urllib.request
import json
import sys
import time
import re

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

TRINO_URL = "http://localhost:8082/v1/statement"
TRINO_USER = "admin"

def execute_trino_query(sql, catalog="iceberg", schema="gold"):
    cleaned_sql = sql.strip().rstrip(";")
    req = urllib.request.Request(
        TRINO_URL,
        data=cleaned_sql.encode('utf-8'),
        headers={
            "X-Trino-User": TRINO_USER,
            "X-Trino-Catalog": catalog,
            "X-Trino-Schema": schema,
            "Content-Type": "text/plain; charset=utf-8"
        }
    )
    with urllib.request.urlopen(req) as response:
        res = json.loads(response.read().decode('utf-8'))
    
    columns = [col['name'] for col in res.get('columns', [])]
    all_data = []
    if 'data' in res:
        all_data.extend(res['data'])
    
    # Follow nextUri until query finishes
    next_uri = res.get("nextUri")
    while next_uri:
        time.sleep(0.1)
        req = urllib.request.Request(next_uri, headers={"X-Trino-User": TRINO_USER})
        with urllib.request.urlopen(req) as response:
            res = json.loads(response.read().decode('utf-8'))
            if not columns and 'columns' in res:
                columns = [col['name'] for col in res.get('columns', [])]
            if 'data' in res:
                all_data.extend(res['data'])
            next_uri = res.get("nextUri")
    
    if "error" in res:
        raise Exception(f"Trino Query Error: {res['error']['message']}\nFull Error: {res['error']}")
    
    return columns, all_data

def apply_views():
    print(">>> Reading config/trino/views/07_dashboard1_and_2_semantic_views.sql...")
    with open("config/trino/views/07_dashboard1_and_2_semantic_views.sql", "r", encoding="utf-8") as f:
        content = f.read()

    # Split by semicolon
    parts = [p.strip() for p in content.split(';') if p.strip()]
    for p in parts:
        lines = [l for l in p.splitlines() if not l.strip().startswith('--')]
        stmt = '\n'.join(lines).strip()
        if not stmt.upper().startswith("CREATE OR REPLACE VIEW"):
            continue
        m = re.search(r'CREATE OR REPLACE VIEW\s+([^\s]+)', stmt, re.IGNORECASE)
        view_name = m.group(1) if m else "unknown"
        print(f"Applying view: {view_name} ...", end=" ", flush=True)
        try:
            execute_trino_query(stmt)
            print("[SUCCESS]")
        except Exception as e:
            print(f"[FAILED]: {e}")
            raise

def run_validations():
    print("\n" + "="*70)
    print("RUNNING THE 3 SPECIFIED VALIDATION CHECKS")
    print("="*70)

    # Check 1: Load missing regions check
    q1 = """
    SELECT date_value, hour_of_day, COUNT(DISTINCT region_name) AS n_regions, SUM(load_mw) AS total_load
    FROM gold.v_load_region_hourly
    GROUP BY 1, 2
    HAVING COUNT(DISTINCT region_name) < 3
    LIMIT 10
    """
    print("\n[Validation 1] Check region count < 3 (Expected: EMPTY):")
    cols1, data1 = execute_trino_query(q1)
    if len(data1) == 0:
        print("  -> PASSED: 0 rows returned. All hours have all 3 regions!")
    else:
        print(f"  -> WARNING: Found {len(data1)} hours with < 3 regions:", data1)

    # Check 2: Pmax check (Expected ~40,000 - 52,000 MW, NOT ~90,000 MW)
    q2 = """
    SELECT date_value, MAX(load_mw) AS pmax
    FROM gold.v_load_national_hourly
    GROUP BY 1
    ORDER BY pmax DESC
    LIMIT 5
    """
    print("\n[Validation 2] Top 5 Pmax days (Expected ~40k - 52k MW):")
    cols2, data2 = execute_trino_query(q2)
    for row in data2:
        print(f"  Date: {row[0]} | Pmax: {row[1]:,.1f} MW")
    
    # Check 3: Supply - Demand balance check on latest date
    q_date = "SELECT MAX(date_value) FROM gold.v_load_national_hourly"
    _, d_res = execute_trino_query(q_date)
    latest_date = d_res[0][0]
    print(f"\n[Validation 3] Supply-Demand balance for latest date: {latest_date}")
    q3 = f"""
    SELECT n.date_value, n.hour_of_day,
           n.load_mw,
           g.total_gen_mw,
           n.load_mw - g.total_gen_mw AS gap_mw,
           ROUND(100.0 * (n.load_mw - g.total_gen_mw) / n.load_mw, 2) AS gap_pct
    FROM gold.v_load_national_hourly n
    JOIN (SELECT date_value, hour_of_day, SUM(generation_mw) AS total_gen_mw
          FROM gold.v_gen_hourly GROUP BY 1, 2) g
      ON n.date_value = g.date_value AND n.hour_of_day = g.hour_of_day
    WHERE n.date_value = DATE '{latest_date}'
    ORDER BY n.hour_of_day
    """
    cols3, data3 = execute_trino_query(q3)
    max_gap_pct = 0.0
    for row in data3:
        gap_pct = abs(float(row[5])) if row[5] is not None else 0.0
        if gap_pct > max_gap_pct:
            max_gap_pct = gap_pct
        print(f"  Hour {row[1]:02d}: Load = {row[2]:,.1f} MW | Gen = {row[3]:,.1f} MW | Gap = {row[4]:,.1f} MW ({row[5]}%)")
    print(f"  -> Max Gap Pct across 24h: {max_gap_pct:.2f}%")

if __name__ == "__main__":
    apply_views()
    run_validations()
