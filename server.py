import json
import os
import datetime
import base64
import threading
import time
import subprocess
import urllib.request
import urllib.error
from http.server import BaseHTTPRequestHandler, HTTPServer

BASE_DIR = os.path.dirname(__file__)

def _is_writable_dir(path):
    try:
        os.makedirs(path, exist_ok=True)
        test_file = os.path.join(path, '.writetest')
        with open(test_file, 'w', encoding='utf-8') as f:
            f.write('ok')
        os.remove(test_file)
        return True
    except Exception:
        return False

def _resolve_data_dir():
    # Prefer explicit env vars; fall back to Render persistent disk (/data) if available; else BASE_DIR
    candidates = [
        os.environ.get('PERSIST_DIR') or '',
        os.environ.get('DATA_DIR') or '',
        os.environ.get('RENDER_DATA_DIR') or '',
        '/data',  # Render persistent disk default path if provisioned
        BASE_DIR,
    ]
    for p in candidates:
        if not p:
            continue
        try:
            if _is_writable_dir(p):
                return p
        except Exception:
            continue
    return BASE_DIR

DATA_DIR = _resolve_data_dir()
DATA_FILE = os.path.join(DATA_DIR, 'leaderboard.json')
BACKUPS_DIR = os.path.join(DATA_DIR, 'backups')

def _items_len(obj):
    if isinstance(obj, dict):
        v = obj.get('items')
        return len(v) if isinstance(v, list) else 0
    if isinstance(obj, list):
        return len(obj)
    return 0

def _ensure_backups_dir():
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
        os.makedirs(BACKUPS_DIR, exist_ok=True)
    except Exception:
        pass

def _load_json(path):
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return None

def _atomic_write_json(path, payload):
    tmp = path + '.tmp'
    try:
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        os.replace(tmp, path)
    except Exception:
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass

def _save_backup_snapshot(items):
    """Write latest.json and a timestamped snapshot under backups, update manifest."""
    _ensure_backups_dir()
    payload = {'items': items}
    # latest.json
    latest_path = os.path.join(BACKUPS_DIR, 'latest.json')
    _atomic_write_json(latest_path, payload)
    # timestamped snapshot
    ts = datetime.datetime.utcnow().strftime('%Y%m%d-%H%M%S')
    snap_name = f'leaderboard-{ts}.json'
    snap_path = os.path.join(BACKUPS_DIR, snap_name)
    _atomic_write_json(snap_path, payload)
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
        _atomic_write_json(manifest_path, manifest)
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
    _ensure_backups_dir()
    _atomic_write_json(DATA_FILE, data)
    # also write backups (latest + timestamped + manifest)
    try:
        _save_backup_snapshot(data if isinstance(data, list) else [])
    except Exception:
        pass
    # try to sync to GitHub or local git
    try:
        _sync_leaderboard_to_remote()
    except Exception:
        pass

# -----------------
# GitHub/Git sync
# -----------------

def _github_upload_file(local_path, repo_path):
    token = os.environ.get('GH_TOKEN') or os.environ.get('GITHUB_TOKEN')
    repo = os.environ.get('GH_REPO')  # format: owner/repo
    branch = os.environ.get('GH_BRANCH', 'main')
    if not token or not repo:
        return False
    # read file content
    with open(local_path, 'rb') as f:
        content = f.read()
    b64 = base64.b64encode(content).decode('ascii')

    headers = {
        'Authorization': f'token {token}',
        'Accept': 'application/vnd.github+json',
        'Content-Type': 'application/json',
        'User-Agent': 'mid-autumn-leaderboard-sync'
    }

    # get existing sha if any
    sha = None
    get_url = f'https://api.github.com/repos/{repo}/contents/{repo_path}?ref={branch}'
    try:
        req = urllib.request.Request(get_url, headers=headers)
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode('utf-8'))
            sha = data.get('sha')
    except urllib.error.HTTPError:
        sha = None
    except Exception:
        sha = None

    payload = {
        'message': f"auto: update leaderboard {datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')}",
        'content': b64,
        'branch': branch
    }
    if sha:
        payload['sha'] = sha

    put_url = f'https://api.github.com/repos/{repo}/contents/{repo_path}'
    req = urllib.request.Request(put_url, data=json.dumps(payload).encode('utf-8'), headers=headers, method='PUT')
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            return 200 <= resp.status < 300
    except Exception:
        return False

def _git_cli_push(local_path):
    # Fallback: if .git exists and remote is configured, commit and push.
    repo_root = BASE_DIR
    if not os.path.exists(os.path.join(repo_root, '.git')):
        return False
    try:
        subprocess.run(['git', 'add', local_path], cwd=repo_root, check=True)
        msg = f"auto: update {os.path.basename(local_path)} {datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')}"
        subprocess.run(['git', 'commit', '-m', msg], cwd=repo_root, check=False)
        subprocess.run(['git', 'push'], cwd=repo_root, check=True)
        return True
    except Exception:
        return False

def _sync_leaderboard_to_remote():
    # env-controlled: only run when enabled
    if os.environ.get('GIT_AUTO_PUSH', '1') not in ('1', 'true', 'True'):
        return False
    repo_path = os.environ.get('GH_PATH', 'leaderboard.json')
    # prefer GitHub API, fallback to local git push
    ok = _github_upload_file(DATA_FILE, repo_path)
    if not ok:
        ok = _git_cli_push(DATA_FILE)
    return ok

# ---------------
# File watcher
# ---------------

_last_mtime = 0.0
_last_push_mtime = 0.0

def _start_file_watcher():
    def loop():
        global _last_mtime, _last_push_mtime
        while True:
            try:
                if os.path.exists(DATA_FILE):
                    m = os.path.getmtime(DATA_FILE)
                    if _last_mtime == 0.0:
                        _last_mtime = m
                    # if modified and not pushed for this mtime, sync
                    if m != _last_push_mtime and m != _last_mtime:
                        time.sleep(0.5)
                        _sync_leaderboard_to_remote()
                        _last_push_mtime = m
                        _last_mtime = m
                time.sleep(2.0)
            except Exception:
                time.sleep(3.0)
    t = threading.Thread(target=loop, daemon=True)
    t.start()

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
            # 讀取資料；若主檔不符合預期結構則回退到備份快照
            data = load_data()
            if not isinstance(data, list) or (len(data) > 0 and not isinstance(data[0], dict)):
                data = _find_best_local_snapshot()
            if not isinstance(data, list):
                data = []

            # 過濾無效項目：名稱空/未知且分數<=0，或缺少 user_id
            valid = []
            for it in data:
                if not isinstance(it, dict):
                    continue
                name = str(it.get('name') or '').strip()
                try:
                    score = float(it.get('score') or 0)
                except Exception:
                    score = 0.0
                uid = str(it.get('user_id') or '').strip()
                if not uid:
                    continue
                if score <= 0 and name in ('', '未知'):
                    continue
                valid.append(it)

            # 排序（分數高到低），並加入排名序號
            data_sorted = sorted(valid, key=lambda x: (-x.get('score', 0), x.get('updated_at', '')), reverse=False)
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
    print(f"Data directory: {DATA_DIR}")
    print(f"Backups directory: {BACKUPS_DIR}")
    # start watcher to upload on file changes
    _start_file_watcher()
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

# -----------------
# GitHub/Git sync
# -----------------

def _github_upload_file(local_path, repo_path):
    token = os.environ.get('GH_TOKEN') or os.environ.get('GITHUB_TOKEN')
    repo = os.environ.get('GH_REPO')  # format: owner/repo
    branch = os.environ.get('GH_BRANCH', 'main')
    if not token or not repo:
        return False
    # read file content
    with open(local_path, 'rb') as f:
        content = f.read()
    b64 = base64.b64encode(content).decode('ascii')

    headers = {
        'Authorization': f'token {token}',
        'Accept': 'application/vnd.github+json',
        'Content-Type': 'application/json',
        'User-Agent': 'mid-autumn-leaderboard-sync'
    }

    # get existing sha if any
    sha = None
    get_url = f'https://api.github.com/repos/{repo}/contents/{repo_path}?ref={branch}'
    try:
        req = urllib.request.Request(get_url, headers=headers)
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode('utf-8'))
            sha = data.get('sha')
    except urllib.error.HTTPError as e:
        # likely file not found; ignore
        sha = None
    except Exception:
        sha = None

    payload = {
        'message': f"auto: update leaderboard {datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')}",
        'content': b64,
        'branch': branch
    }
    if sha:
        payload['sha'] = sha

    put_url = f'https://api.github.com/repos/{repo}/contents/{repo_path}'
    req = urllib.request.Request(put_url, data=json.dumps(payload).encode('utf-8'), headers=headers, method='PUT')
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            # upload ok
            return 200 <= resp.status < 300
    except Exception:
        return False

def _git_cli_push(local_path):
    # Fallback: if .git exists and remote is configured, commit and push.
    repo_root = BASE_DIR
    if not os.path.exists(os.path.join(repo_root, '.git')):
        return False
    try:
        subprocess.run(['git', 'add', local_path], cwd=repo_root, check=True)
        msg = f"auto: update {os.path.basename(local_path)} {datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')}"
        subprocess.run(['git', 'commit', '-m', msg], cwd=repo_root, check=False)
        subprocess.run(['git', 'push'], cwd=repo_root, check=True)
        return True
    except Exception:
        return False

def _sync_leaderboard_to_remote():
    # env-controlled: only run when enabled
    if os.environ.get('GIT_AUTO_PUSH', '1') not in ('1', 'true', 'True'):
        return False
    repo_path = os.environ.get('GH_PATH', 'leaderboard.json')
    # prefer GitHub API, fallback to local git push
    ok = _github_upload_file(DATA_FILE, repo_path)
    if not ok:
        ok = _git_cli_push(DATA_FILE)
    return ok

# ---------------
# File watcher
# ---------------

_last_mtime = 0.0
_last_push_mtime = 0.0

def _start_file_watcher():
    def loop():
        global _last_mtime, _last_push_mtime
        while True:
            try:
                if os.path.exists(DATA_FILE):
                    m = os.path.getmtime(DATA_FILE)
                    if _last_mtime == 0.0:
                        _last_mtime = m
                    # if modified and not pushed for this mtime, sync
                    if m != _last_push_mtime and m != _last_mtime:
                        # slight debounce window
                        time.sleep(0.5)
                        _sync_leaderboard_to_remote()
                        _last_push_mtime = m
                        _last_mtime = m
                time.sleep(2.0)
            except Exception:
                time.sleep(3.0)
    t = threading.Thread(target=loop, daemon=True)
    t.start()