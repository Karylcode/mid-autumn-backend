import json
import os
from http.server import BaseHTTPRequestHandler, HTTPServer

DATA_FILE = os.path.join(os.path.dirname(__file__), 'leaderboard.json')

def load_data():
    if not os.path.exists(DATA_FILE):
        return []
    try:
        with open(DATA_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return []

def save_data(data):
    with open(DATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

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
    # 建立資料檔
    if not os.path.exists(DATA_FILE):
        save_data([])
    run_server()