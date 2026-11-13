import urllib.request, json, sys

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

token_req = urllib.request.Request(
    'http://localhost:3000/api/session',
    data=json.dumps({'username': 'admin@lakehouse.vn', 'password': 'Lakehouse@2026'}).encode('utf-8'),
    headers={'Content-Type': 'application/json'}
)
with urllib.request.urlopen(token_req) as resp:
    token = json.loads(resp.read().decode('utf-8'))['id']

def verify_dash(dash_id):
    req = urllib.request.Request(f'http://localhost:3000/api/dashboard/{dash_id}', headers={'X-Metabase-Session': token})
    with urllib.request.urlopen(req, timeout=10) as resp:
        dash = json.loads(resp.read().decode('utf-8'))
    
    print(f"\nVerifying Dashboard {dash_id}: '{dash['name']}' ({len(dash['dashcards'])} cards)...", flush=True)
    for dc in dash['dashcards']:
        card = dc.get('card', {})
        cid = card.get('id')
        name = card.get('name')
        if not cid:
            continue
        print(f"  Testing Card {cid}: {name} ...", end=" ", flush=True)
        q_req = urllib.request.Request(
            f'http://localhost:3000/api/card/{cid}/query',
            data=json.dumps({"parameters": []}).encode('utf-8'),
            headers={'X-Metabase-Session': token, 'Content-Type': 'application/json'}
        )
        try:
            with urllib.request.urlopen(q_req, timeout=30) as resp:
                res = json.loads(resp.read().decode('utf-8'))
                status = res.get('status')
                row_cnt = len(res.get('data', {}).get('rows', []))
                print(f"-> Status: {status} | Rows: {row_cnt}", flush=True)
        except Exception as e:
            print(f"-> FAILED: {e}", flush=True)

if __name__ == '__main__':
    dash_id = int(sys.argv[1]) if len(sys.argv) > 1 else 12
    verify_dash(dash_id)
