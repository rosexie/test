#!/usr/bin/env python3
"""Run dashboard verifier against a local mock API server.

This gives a deterministic integration check when real environment is unavailable.
"""

from __future__ import annotations

import json
import threading
import sys
from pathlib import Path
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.verify_dashboard import CheckFailed, verify


INDEX_HTML = """<!doctype html><html><body>
<button id=\"refreshBtn\">刷新看板</button>
<div id=\"queueOverview\"></div>
<div id=\"dailyApps\"></div>
<div id=\"queueAppSummary\"></div>
</body></html>"""

APP_JS = "document.getElementById('refreshBtn').addEventListener('click', refreshDashboard);"


class MockHandler(BaseHTTPRequestHandler):
    def _write_json(self, payload: object) -> None:
        raw = json.dumps(payload).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def _write_text(self, text: str) -> None:
        raw = text.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def log_message(self, format: str, *args):  # noqa: A003
        return

    def do_GET(self):  # noqa: N802
        path = urlparse(self.path).path
        query = parse_qs(urlparse(self.path).query)

        if path == "/":
            return self._write_text(INDEX_HTML)
        if path == "/static/app.js":
            return self._write_text(APP_JS)
        if path == "/api/meta/pages":
            return self._write_json({"data": [{"KEY": "dashboard", "TITLE": "Dashboard"}]})
        if path == "/api/dashboard/queue/overview":
            return self._write_json({"rows": [{"QUEUE_PATH": "root.a", "PEAK_USED_MEMORY_MB": 4000, "P95_USED_MEMORY_MB": 3600, "AVG_USED_MEMORY_MB": 2800}]})
        if path == "/api/dashboard/apps/daily-summary":
            days = query.get("days", ["14"])[0]
            return self._write_json({"items": [{"BUCKET_DAY": "2026-03-04", "TOTAL_APPS": int(days), "SUCCESS_APPS": 11, "FAILED_APPS": 1, "RUNNING_APPS": 2, "P95_MAX_ALLOCATED_MB": 3072}]})
        if path == "/api/dashboard/apps/queue-summary":
            return self._write_json({"result": [{"QUEUE_NAME": "default", "TOTAL_APPS": 9, "SUCCESS_APPS": 7, "FAILED_APPS": 1, "RUNNING_APPS": 1, "P95_MAX_ALLOCATED_MB": 2048}]})
        if path == "/api/dashboard/apps/recent":
            return self._write_json({"data": [{"APP_ID": "app_1", "QUEUE_NAME": "default", "APP_NAME": "demo-job", "RESULT_TAG": "success", "MAX_ALLOCATED_MB": 1024, "START_TIME": "2026-03-04T10:00:00"}]})

        self.send_response(404)
        self.end_headers()


@dataclass
class MockServer:
    server: ThreadingHTTPServer
    thread: threading.Thread


def start_server() -> MockServer:
    server = ThreadingHTTPServer(("127.0.0.1", 0), MockHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return MockServer(server=server, thread=thread)


def main() -> int:
    mock = start_server()
    base_url = f"http://127.0.0.1:{mock.server.server_port}"
    try:
        verify(base_url, timeout=5.0)
    except CheckFailed as exc:
        print(f"[FAIL] mock verification failed: {exc}")
        return 1
    finally:
        mock.server.shutdown()
        mock.server.server_close()

    print(f"[PASS] mock verification succeeded at {base_url}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
