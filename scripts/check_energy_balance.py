import urllib.request, json, sys

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

TRINO_URL = 'http://localhost:8082/v1/statement'

def run_sql(sql):
    req = urllib.request.Request(
        TRINO_URL,
        data=sql.strip().rstrip(';').encode('utf-8'),
        headers={'X-Trino-User': 'admin', 'X-Trino-Catalog': 'iceberg'}
    )
    with urllib.request.urlopen(req) as resp:
        res = json.loads(resp.read().decode('utf-8'))
    next_uri = res.get('nextUri')
    rows = res.get('data', [])
    cols = [c['name'] for c in res.get('columns', [])]
    while next_uri:
        req = urllib.request.Request(next_uri, headers={'X-Trino-User': 'admin'})
        with urllib.request.urlopen(req) as resp:
            res = json.loads(resp.read().decode('utf-8'))
            if not cols and 'columns' in res:
                cols = [c['name'] for c in res['columns']]
            if 'data' in res:
                rows.extend(res['data'])
            next_uri = res.get('nextUri')
    if 'error' in res:
        raise Exception(res['error']['message'])
    return cols, rows

if __name__ == '__main__':
    # Get hourly load for 2026-10-10
    _, load_rows = run_sql("SELECT hour_of_day, load_mw FROM gold.v_load_national_hourly WHERE date_value = DATE '2026-10-10' ORDER BY 1")
    load_by_hour = {r[0]: r[1] for r in load_rows}

    # Get daily generation for 2026-10-10
    q_gen = """
    SELECT 
        CASE 
            WHEN LOWER(energy_source) LIKE '%than%' THEN 'Than'
            WHEN LOWER(energy_source) LIKE '%khí%' OR LOWER(energy_source) LIKE '%gas%' THEN 'Khí'
            WHEN LOWER(energy_source) LIKE '%thủy điện%' THEN 'Thủy điện'
            WHEN LOWER(energy_source) LIKE '%mặt trời%' OR LOWER(energy_source) LIKE '%đmt%' THEN 'Mặt trời'
            WHEN LOWER(energy_source) LIKE '%gió%' THEN 'Gió'
            ELSE 'Khác'
        END AS source_group,
        SUM(generation_million_kwh * 1000.0) AS generation_mwh
    FROM iceberg.silver.generation
    WHERE data_date = DATE '2026-10-10' AND energy_source NOT LIKE '%thương phẩm%'
    GROUP BY 1
    """
    _, gen_rows = run_sql(q_gen)
    daily_gen = {r[0]: r[1] for r in gen_rows}

    print("Daily Generation (MWh):", daily_gen)
    total_daily_mwh = sum(daily_gen.values())
    total_load_mwh = sum(load_by_hour.values())
    print(f"Total Generation MWh: {total_daily_mwh:.1f}")
    print(f"Total National Load MWh: {total_load_mwh:.1f}")
    print(f"Overall Ratio (Gen / Load): {total_daily_mwh / total_load_mwh:.3f} (Generation is typically ~1.01 - 1.03x load due to transmission losses)")
