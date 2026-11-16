import urllib.request, json, time

token_req = urllib.request.Request(
    'http://localhost:3000/api/session',
    data=json.dumps({'username': 'admin@lakehouse.vn', 'password': 'Lakehouse@2026'}).encode('utf-8'),
    headers={'Content-Type': 'application/json'}
)
with urllib.request.urlopen(token_req) as resp:
    token = json.loads(resp.read().decode('utf-8'))['id']

req = urllib.request.Request('http://localhost:3000/api/dashboard', headers={'X-Metabase-Session': token})
with urllib.request.urlopen(req) as resp:
    dashboards = json.loads(resp.read().decode('utf-8'))
    print("ALL METABASE DASHBOARDS:")
    for d in dashboards:
        print(f"  ID={d['id']}, Name='{d['name']}'")

# Find ID of '02. Năng Lượng Tự Nhiên 3 Miền'
target_id = None
for d in dashboards:
    if "02. Năng Lượng Tự Nhiên 3 Miền" in d['name']:
        target_id = d['id']
        break

if not target_id:
    target_id = 5

print(f"\n=======================================================")
print(f"VERIFYING ALL 9 CARDS ON DASHBOARD {target_id}")
print(f"=======================================================")

req = urllib.request.Request(f'http://localhost:3000/api/dashboard/{target_id}', headers={'X-Metabase-Session': token})
with urllib.request.urlopen(req) as resp:
    dash = json.loads(resp.read().decode('utf-8'))

test_dates = ["2026-11-01", "2026-11-14", None]

for test_date in test_dates:
    lbl = f"Date = {test_date}" if test_date else "Date = None (Default)"
    print(f"\n--- Running Verification for {lbl} ---")
    all_ok = True
    for dc in dash['dashcards']:
        card = dc.get('card', {})
        cid = card.get('id')
        name = card.get('name')
        if not cid:
            continue
        
        q_body = {
            "parameters": [
                {
                    "id": "tag_selected_date",
                    "type": "date/single",
                    "target": ["variable", ["template-tag", "selected_date"]],
                    "value": test_date
                }
            ] if test_date else []
        }
        
        t0 = time.time()
        q_req = urllib.request.Request(
            f'http://localhost:3000/api/card/{cid}/query',
            data=json.dumps(q_body).encode('utf-8'),
            headers={'X-Metabase-Session': token, 'Content-Type': 'application/json'}
        )
        try:
            with urllib.request.urlopen(q_req, timeout=30) as q_resp:
                res = json.loads(q_resp.read().decode('utf-8'))
                elapsed = time.time() - t0
                status = res.get('status')
                data = res.get('data', {})
                rows = len(data.get('rows', []))
                cols = [c['name'] for c in data.get('cols', [])]
                print(f"  [PASS] Card {cid:3d} ({elapsed:4.2f}s) | Rows: {rows:2d} | Cols: {len(cols):2d} | '{name}'")
        except Exception as e:
            all_ok = False
            elapsed = time.time() - t0
            print(f"  [FAIL] Card {cid:3d} ({elapsed:4.2f}s) | Error: {e} | '{name}'")
    
    if all_ok:
        print(f"  --> ALL CARDS PASSED FOR {lbl}!")
