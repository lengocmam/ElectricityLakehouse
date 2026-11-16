import urllib.request, json, time, sys

token_req = urllib.request.Request(
    'http://localhost:3000/api/session',
    data=json.dumps({'username': 'admin@lakehouse.vn', 'password': 'Lakehouse@2026'}).encode('utf-8'),
    headers={'Content-Type': 'application/json'}
)
with urllib.request.urlopen(token_req) as resp:
    token = json.loads(resp.read().decode('utf-8'))['id']

req = urllib.request.Request('http://localhost:3000/api/dashboard/6', headers={'X-Metabase-Session': token})
with urllib.request.urlopen(req) as resp:
    dash = json.loads(resp.read().decode('utf-8'))

print(f"\n=======================================================")
print(f"VERIFYING ALL CARDS ON DASHBOARD 6 ('{dash['name']}')")
print(f"=======================================================")
sys.stdout.flush()

test_years = [2026, 2024, None]

for test_year in test_years:
    lbl = f"Year = {test_year}" if test_year else "Year = None (Default: 2026)"
    print(f"\n--- Running Verification for {lbl} ---")
    sys.stdout.flush()
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
                    "id": "tag_selected_year",
                    "type": "number",
                    "target": ["variable", ["template-tag", "selected_year"]],
                    "value": test_year
                }
            ] if test_year else []
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
                sys.stdout.flush()
        except Exception as e:
            all_ok = False
            elapsed = time.time() - t0
            print(f"  [FAIL] Card {cid:3d} ({elapsed:4.2f}s) | Error: {e} | '{name}'")
            sys.stdout.flush()
    
    if all_ok:
        print(f"  --> ALL CARDS PASSED FOR {lbl}!")
        sys.stdout.flush()
