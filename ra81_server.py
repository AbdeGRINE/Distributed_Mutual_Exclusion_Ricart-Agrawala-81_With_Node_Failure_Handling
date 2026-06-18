#!/usr/bin/env python3
"""
ra81_server.py  —  Tiny HTTP server for the RA81 dashboard.

Serves:
  GET /          → dashboard HTML
  GET /state     → JSON with all 10 process states (reads /tmp/ra81_proc_*.json)
  POST /fail/<id> → toggles FAILED state of process <id> via SIGUSR1

Usage:  python3 ra81_server.py [port]   (default 8080)
"""

import json, os, signal, subprocess, sys, time
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

N = 10
STATE_DIR = "/tmp"

def read_all_states():
    states = []
    for i in range(1, N + 1):
        path = Path(f"{STATE_DIR}/ra81_proc_{i}.json")
        try:
            data = json.loads(path.read_text())
        except Exception:
            data = {
                "id": i, "state": "OFFLINE", "clock": 0, "osn": 0,
                "replies_needed": 0, "sc_wanted": False,
                "deferred": [], "peer_req_ts": {}, "ts": 0
            }
        states.append(data)
    return states

def find_pid(proc_id):
    """Find PID of ra81_process <id>"""
    try:
        result = subprocess.check_output(
            ["pgrep", "-f", f"ra81_process {proc_id}"],
            text=True
        ).strip()
        # pgrep may return multiple lines; take first
        return int(result.split()[0])
    except Exception:
        return None

class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args): pass  # suppress access logs

    def do_GET(self):
        if self.path == "/":
            self.serve_dashboard()
        elif self.path == "/state":
            self.serve_state()
        else:
            self.send_error(404)

    def do_POST(self):
        if self.path.startswith("/fail/"):
            proc_id = int(self.path.split("/")[-1])
            pid = find_pid(proc_id)
            if pid:
                os.kill(pid, signal.SIGUSR1)
                self.send_json({"ok": True, "pid": pid, "proc": proc_id})
            else:
                self.send_json({"ok": False, "error": f"P{proc_id} not found"})
        else:
            self.send_error(404)

    def serve_state(self):
        data = json.dumps(read_all_states())
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(data.encode())

    def send_json(self, obj):
        data = json.dumps(obj).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(data)

    def serve_dashboard(self):
        html_path = Path(__file__).parent / "gui" / "dashboard.html"
        html = html_path.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(html)

if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8080
    srv = HTTPServer(("0.0.0.0", port), Handler)
    print(f"RA81 Dashboard: http://localhost:{port}/")
    srv.serve_forever()
