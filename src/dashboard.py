# -*- coding: utf-8 -*-
"""本地仪表盘：把 state/latest.json 渲染成网页，并暴露 /api/state 给前端。

只监听 127.0.0.1，不对外网开放。用标准库 http.server，无需安装任何依赖。
"""
from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from .util import STATE_DIR, WEB_DIR, log, read_json

LATEST = STATE_DIR / "latest.json"


class Handler(BaseHTTPRequestHandler):
    server_version = "SolSignal/1.0"

    def _send(self, body: bytes, ctype: str, code: int = 200):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):  # noqa: N802
        p = self.path.split("?")[0]
        if p in ("/", "/index.html"):
            f = WEB_DIR / "index.html"
            if not f.exists():
                return self._send("dashboard not built".encode(), "text/plain; charset=utf-8", 500)
            return self._send(f.read_bytes(), "text/html; charset=utf-8")
        if p == "/api/state":
            data = read_json(LATEST)
            if data is None:
                return self._send(json.dumps({"error": "尚无快照"}, ensure_ascii=False).encode(),
                                  "application/json; charset=utf-8", 503)
            return self._send(json.dumps(data, ensure_ascii=False, default=str).encode(),
                              "application/json; charset=utf-8")
        return self._send(b"not found", "text/plain; charset=utf-8", 404)

    def log_message(self, fmt, *args):  # 静默访问日志
        return


def serve(port: int = 8787, host: str = "127.0.0.1") -> ThreadingHTTPServer:
    httpd = ThreadingHTTPServer((host, port), Handler)
    t = threading.Thread(target=httpd.serve_forever, daemon=True, name="dashboard")
    t.start()
    log(f"仪表盘已启动: http://{host}:{port}/")
    return httpd
