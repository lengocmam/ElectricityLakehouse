import urllib.request, json, time

token_req = urllib.request.Request(
    'http://localhost:3000/api/session',
    data=json.dumps({'username': 'admin@lakehouse.vn', 'password': 'Lakehouse@2026'}).encode('utf-8'),
    headers={'Content-Type': 'application/json'}
)
with urllib.request.urlopen(token_req) as resp:
    token = json.loads(resp.read().decode('utf-8'))['id']

req = urllib.request.Request('http://localhost:3000/api/dashboard/3', headers={'X-Metabase-Session': token})
with urllib.request.urlopen(req) as resp:
    dash = json.loads(resp.read().decode('utf-8'))

print(f"\n=======================================================")
print(f"VERIFYING ALL 9 CARDS ON DASHBOARD 3 WITH DATE 2026-11-01")
print(f"=======================================================")

test_dates = ["2026-11-01", None]

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
                status = res.get('status')
                rows = res.get('data', {}).get('rows', [])
                dur = time.time() - t0
                if status == 'completed' and len(rows) > 0:
                    print(f"  [PASS] Card {cid} ({dur:.2f}s, {len(rows)} rows): {name}")
                else:
                    print(f"  [WARN] Card {cid} ({dur:.2f}s, status={status}, rows={len(rows)}): {name}")
                    all_ok = False
        except Exception as e:
            dur = time.time() - t0
            print(f"  [FAIL] Card {cid} ({dur:.2f}s): {name} -> {e}")
            all_ok = False

    if all_ok:
        print(f"\n[ALL 9 CARDS PASSED FOR {lbl}]")
    else:
        print(f"\n[SOME CARDS FAILED FOR {lbl}]")
