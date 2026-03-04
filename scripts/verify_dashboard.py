#!/usr/bin/env python3
"""Lightweight end-to-end verifier for dashboard pages and data APIs.

Usage:
  python scripts/verify_dashboard.py --base-url http://127.0.0.1:8000
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.parse
import urllib.request
from typing import Any


class CheckFailed(Exception):
    pass


def fetch_json(base_url: str, path: str, timeout: float) -> Any:
    url = urllib.parse.urljoin(base_url.rstrip("/") + "/", path.lstrip("/"))
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        raise CheckFailed(f"{path} returned HTTP {exc.code}") from exc
    except urllib.error.URLError as exc:
        raise CheckFailed(f"cannot reach {url}: {exc}") from exc


def fetch_text(base_url: str, path: str, timeout: float) -> str:
    url = urllib.parse.urljoin(base_url.rstrip("/") + "/", path.lstrip("/"))
    req = urllib.request.Request(url, headers={"Accept": "text/html"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        raise CheckFailed(f"{path} returned HTTP {exc.code}") from exc
    except urllib.error.URLError as exc:
        raise CheckFailed(f"cannot reach {url}: {exc}") from exc


def ensure_non_empty_list(value: Any, name: str) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        raise CheckFailed(f"{name} is not a list")
    if not value:
        raise CheckFailed(f"{name} returned empty list")
    if not isinstance(value[0], dict):
        raise CheckFailed(f"{name} first item is not an object")
    return value


def ensure_keys(row: dict[str, Any], keys: list[str], name: str) -> None:
    missing = [k for k in keys if k not in row]
    if missing:
        raise CheckFailed(f"{name} missing keys: {missing}")


def verify(base_url: str, timeout: float) -> None:
    pages = fetch_json(base_url, "/api/meta/pages", timeout)
    pages = ensure_non_empty_list(pages, "/api/meta/pages")
    page_keys = {p.get("key") for p in pages}
    if "dashboard" not in page_keys:
        raise CheckFailed("/api/meta/pages missing 'dashboard'")

    # 1) 队列资源总览（Peak/P95/Avg）
    overview = fetch_json(base_url, "/api/dashboard/queue/overview?period=day&top_n=8", timeout)
    overview = ensure_non_empty_list(overview, "/api/dashboard/queue/overview")
    ensure_keys(overview[0], ["queue_path", "peak_used_memory_mb", "p95_used_memory_mb", "avg_used_memory_mb"], "queue overview")

    # 2) 每日任务量 / 成功率 / P95资源
    daily = fetch_json(base_url, "/api/dashboard/apps/daily-summary?days=14", timeout)
    daily = ensure_non_empty_list(daily, "/api/dashboard/apps/daily-summary")
    ensure_keys(daily[0], ["bucket_day", "total_apps", "success_apps", "p95_max_allocated_mb"], "daily summary")

    # 3) 队列任务汇总（窗口内）
    queue_summary = fetch_json(base_url, "/api/dashboard/apps/queue-summary?days=14&top_n=8", timeout)
    queue_summary = ensure_non_empty_list(queue_summary, "/api/dashboard/apps/queue-summary")
    ensure_keys(queue_summary[0], ["queue_name", "total_apps", "success_apps", "p95_max_allocated_mb"], "queue summary")

    # 4) 刷新看板按钮 wiring + 页面元素存在
    html = fetch_text(base_url, "/", timeout)
    required_tokens = [
        'id="refreshBtn"',
        "id=\"queueOverview\"",
        "id=\"dailyApps\"",
        "id=\"queueAppSummary\"",
    ]
    for token in required_tokens:
        if token not in html:
            raise CheckFailed(f"index page missing token: {token}")

    app_js = fetch_text(base_url, "/static/app.js", timeout)
    if "getElementById('refreshBtn').addEventListener('click', refreshDashboard)" not in app_js:
        raise CheckFailed("refresh button is not bound to refreshDashboard")


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify dashboard data panels and refresh wiring")
    parser.add_argument("--base-url", default="http://127.0.0.1:8000", help="dashboard service base URL")
    parser.add_argument("--timeout", type=float, default=8.0, help="request timeout seconds")
    args = parser.parse_args()

    try:
        verify(args.base_url, args.timeout)
    except CheckFailed as exc:
        print(f"[FAIL] {exc}")
        return 1

    print("[PASS] dashboard panel/data checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
