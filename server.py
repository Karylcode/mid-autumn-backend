import json
import os
import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer

BASE_DIR = os.path.dirname(__file__)
DATA_FILE = os.path.join(BASE_DIR, 'leaderboard.json')
BACKUPS_DIR = os.path.join(BASE_DIR, 'Server data', 'backups')

def _items_len(obj):
    if isinstance(obj, dict):
        v = obj.get('items')
        return len(v) if isinstance(v, list) else 0
    if isinstance(obj, list):
        return len(obj)
    return 0

def _ensure_backups_dir():
    try:
        os.makedirs(BACKUPS_DIR, exist_ok=True)
    except Exception:
        pass

def _load_json(path):
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return None

def _save_backup_snapshot(items):
    """Write latest.json and a timestamped snapshot under backups, update manifest."""
    _ensure_backups_dir()
    payload = {'items': items}
    # latest.json
    latest_path = os.path.join(BACKUPS_DIR, 'latest.json')
    try:
        with open(latest_path, 'w', encoding='utf-8') as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except Exception:
        pass
    # timestamped snapshot
    ts = datetime.datetime.utcnow().strftime('%Y%m%d-%H%M%S')
    snap_name = f'leaderboard-{ts}.json'
    snap_path = os.path.join(BACKUPS_DIR, snap_name)
    try:
        with open(snap_path, 'w', encoding='utf-8') as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except Exception:
        pass
    # manifest
    manifest_path = os.path.join(BACKUPS_DIR, 'manifest.json')
    manifest = []
    try:
        if os.path.exists(manifest_path):
            with open(manifest_path, 'r', encoding='utf-8') as mf:
                manifest = json.load(mf) or []
    except Exception:
        manifest = []
    try:
        entry = {'timestamp': ts, 'file': snap_name, 'count': len(items)}
        manifest.append(entry)
        with open(manifest_path, 'w', encoding='utf-8') as mf:
            json.dump(manifest, mf, ensure_ascii=False, indent=2)
    except Exception:
        pass

def _find_best_local_snapshot():
    """Return best items list from local backups (latest.json preferred, otherwise max-count among known patterns)."""
    _ensure_backups_dir()
    # 1) latest.json
    latest = _load_json(os.path.join(BACKUPS_DIR, 'latest.json'))
    if latest:
        items = latest.get('items') if isinstance(latest, dict) else (latest if isinstance(latest, list) else [])
        if isinstance(items, list) and len(items) > 0:
            return items
    # 2) choose max-count from patterns
    patterns = ['backend-live-', 'data-live-', 'leaderboard-']
    best_items = []
    max_count = -1
    try:
        for name in sorted(os.listdir(BACKUPS_DIR)):
            if not name.endswith('.json'):
                continue
            if not any(name.startswith(p) for p in patterns):
                continue
            data = _load_json(os.path.join(BACKUPS_DIR, name))
            if data is None:
                continue
            items = data.get('items') if isinstance(data, dict) else (data if isinstance(data, list) else [])
            cnt = len(items) if isinstance(items, list) else 0
            if cnt > max_count:
                max_count = cnt
                best_items = items
    except Exception:
        pass
    return best_items

def load_data():
    if not os.path.exists(DATA_FILE):
        return []
    try:
        with open(DATA_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return []

def save_data(data):
    # persist to main data file
    with open(DATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    # also write backups (latest + timestamped + manifest)
    try:
        _save_backup_snapshot(data if isinstance(data, list) else [])
    except Exception:
        pass

class LeaderboardHandler(BaseHTTPRequestHandler):
    def _set_cors(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')

    def do_OPTIONS(self):
        self.send_response(204)
        self._set_cors()
        self.end_headers()

    def do_GET(self):
        if self.path.startswith('/api/leaderboard'):
            data = load_data()
            # 排序（分數高到低），並加入排名序號
            data_sorted = sorted(data, key=lambda x: (-x.get('score', 0), x.get('updated_at', '')), reverse=False)
            for idx, item in enumerate(data_sorted, start=1):
                item['rank'] = idx
            body = json.dumps({'items': data_sorted}, ensure_ascii=False).encode('utf-8')
            self.send_response(200)
            self._set_cors()
            self.send_header('Content-Type', 'application/json; charset=utf-8')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_response(404)
            self._set_cors()
            self.end_headers()

    def do_POST(self):
        if self.path.startswith('/api/leaderboard/submit'):
            length = int(self.headers.get('Content-Length') or 0)
            raw = self.rfile.read(length)
            try:
                payload = json.loads(raw.decode('utf-8'))
            except Exception:
                payload = {}
            user_id = str(payload.get('user_id') or '').strip()
            name = str(payload.get('name') or '').strip()
            score = float(payload.get('score') or 0)
            avatar = str(payload.get('avatar') or '').strip()
            if not user_id or score <= 0:
                body = json.dumps({'ok': False, 'error': 'invalid payload'}).encode('utf-8')
                self.send_response(400)
                self._set_cors()
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            data = load_data()
            found = False
            for item in data:
                if item.get('user_id') == user_id:
                    # 更新最高分
                    if score > float(item.get('score', 0)):
                        item['score'] = score
                    item['name'] = name or item.get('name')
                    if avatar:
                        item['avatar'] = avatar
                    item['updated_at'] = self.date_time_string()
                    found = True
                    break
            if not found:
                data.append({
                    'user_id': user_id,
                    'name': name,
                    'score': score,
                    'avatar': avatar,
                    'updated_at': self.date_time_string()
                })
            save_data(data)

            # 回傳目前排名
            data_sorted = sorted(data, key=lambda x: (-x.get('score', 0), x.get('updated_at', '')), reverse=False)
            rank = next((i+1 for i, it in enumerate(data_sorted) if it.get('user_id') == user_id), None)
            body = json.dumps({'ok': True, 'rank': rank}, ensure_ascii=False).encode('utf-8')
            self.send_response(200)
            self._set_cors()
            self.send_header('Content-Type', 'application/json; charset=utf-8')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_response(404)
            self._set_cors()
            self.end_headers()

def run_server():
    port = int(os.environ.get('PORT', '8001'))
    print(f"Leaderboard API listening on 0.0.0.0:{port}")
    httpd = HTTPServer(('0.0.0.0', port), LeaderboardHandler)
    httpd.serve_forever()

if __name__ == '__main__':
    # 建立資料檔或從備份自動還原
    need_restore = False
    if not os.path.exists(DATA_FILE):
        need_restore = True
    else:
        try:
            cur = load_data()
            if not isinstance(cur, list) or len(cur) == 0:
                need_restore = True
        except Exception:
            need_restore = True

    if need_restore:
        restored = _find_best_local_snapshot()
        if isinstance(restored, list) and len(restored) > 0:
            print(f"Auto-restore from local backups: {len(restored)} items")
            save_data(restored)
        else:
            print("No valid local backup found. Initializing empty dataset.")
            save_data([])
    run_server()