"""Run with NGINX_BIN=/path/to/nginx python3 tests/gateway/smoke.py.
Uses temporary ports and a mock upstream; never accesses business services.
"""
import concurrent.futures
import http.server
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import threading
import time
import urllib.request
import urllib.error

ROOT = Path(__file__).resolve().parents[2]
class Backend(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        body = json.dumps({'path': self.path, 'ip': self.headers.get('X-Forwarded-For')}).encode()
        self.send_response(200)
        self.end_headers()
        self.wfile.write(body)
    def log_message(self, *args):
        pass

backend = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Backend)
threading.Thread(target=backend.serve_forever, daemon=True).start()
with socket.socket() as sock:
    sock.bind(('127.0.0.1', 0))
    port = sock.getsockname()[1]
with tempfile.TemporaryDirectory() as directory:
    d = Path(directory)
    config = (ROOT / 'deploy/nginx/nginx.conf').read_text()
    config = config.replace('include /etc/nginx/mime.types;', '')
    config = config.replace('/tmp/nginx.pid', str(d / 'nginx.pid'))
    config = config.replace('/dev/stderr', str(d / 'error.log')).replace('/dev/stdout', str(d / 'access.log'))
    config = config.replace('listen 8080;', f'listen {port};')
    config = config.replace('127.0.0.1:8085', f'127.0.0.1:{backend.server_port}')
    config = config.replace('http {', f'http {{\nclient_body_temp_path {d}/body;\nproxy_temp_path {d}/proxy;\nfastcgi_temp_path {d}/fastcgi;\nuwsgi_temp_path {d}/uwsgi;\nscgi_temp_path {d}/scgi;')
    (d / 'nginx.conf').write_text(config)
    command = [os.environ.get('NGINX_BIN', 'nginx'), '-p', directory + '/', '-c', str(d / 'nginx.conf')]
    subprocess.run(command + ['-t'], check=True)
    process = subprocess.Popen(command + ['-g', 'daemon off;'])
    def request(path):
        req = urllib.request.Request(f'http://127.0.0.1:{port}/api/{path}', headers={'X-Forwarded-For': '198.51.100.99'})
        try:
            response = urllib.request.urlopen(req, timeout=3)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            return response.status, response.headers, json.loads(response.read())
    try:
        for _ in range(100):
            try:
                with socket.create_connection(('127.0.0.1', port), timeout=.1):
                    break
            except OSError:
                time.sleep(.02)
        with concurrent.futures.ThreadPoolExecutor(max_workers=30) as pool:
            responses = list(pool.map(lambda _: request('voucher-order/seckill/123'), range(40)))
        assert any(r[0] == 200 for r in responses)
        limited = [r for r in responses if r[0] == 429]
        assert limited, 'burst should be limited'
        assert all(r[1]['Retry-After'] == '1' and r[2]['code'] == 'RATE_LIMITED' for r in limited)
        status, _, body = request('voucher-order/seckill/result?voucherId=123&requestId=test')
        assert status == 200, 'query must have an independent budget'
        assert body['path'].startswith('/voucher-order/seckill/result?'), body
        assert body['ip'] == '127.0.0.1', 'untrusted XFF must be overwritten'
        assert request('voucher-order/seckill/token/123')[0] == 200
        assert request('shop/123')[0] == 200
        print('PASS: syntax, burst 429, JSON/retry header, independent query/token, path and IP forwarding')
    finally:
        process.terminate()
        process.wait(timeout=5)
        backend.shutdown()
