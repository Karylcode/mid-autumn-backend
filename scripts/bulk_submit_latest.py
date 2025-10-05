import json
import sys
import time
import urllib.request

def submit_all(api_url: str, json_path: str) -> int:
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    items = data.get('items', [])
    ok = 0
    print(f"Submitting {len(items)} items to {api_url}")
    for i, item in enumerate(items, 1):
        payload = {
            'user_id': item.get('user_id', ''),
            'name': item.get('name', ''),
            'score': float(item.get('score', 0)),
            'avatar': item.get('avatar', ''),
        }
        req = urllib.request.Request(
            api_url,
            data=json.dumps(payload).encode('utf-8'),
            headers={'Content-Type': 'application/json'}
        )
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                # consume body to avoid resource warnings
                _ = resp.read()
                print(f"[{i}/{len(items)}] OK {payload['name']}: status {resp.status}")
                ok += 1
        except Exception as e:
            print(f"[{i}/{len(items)}] FAIL {payload['name']}: {e}")
        time.sleep(0.15)
    print(f"Done. success={ok}, failed={len(items)-ok}")
    return ok

def main():
    if len(sys.argv) < 3:
        print("Usage: python scripts/bulk_submit_latest.py <api_url> <json_path>")
        print("Example: python scripts/bulk_submit_latest.py https://mid-autumn-backend.onrender.com/api/leaderboard/submit data/backups/latest.json")
        sys.exit(2)
    api_url = sys.argv[1]
    json_path = sys.argv[2]
    ok = submit_all(api_url, json_path)
    sys.exit(0 if ok > 0 else 1)

if __name__ == '__main__':
    main()