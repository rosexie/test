from __future__ import annotations

import os
from datetime import date, timedelta
from typing import Any, Dict, List

from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.requests import Request

app = FastAPI(title="YARN 资源看板")
app.mount("/static", StaticFiles(directory="web/static"), name="static")
templates = Jinja2Templates(directory="web/templates")


def get_conn():
    import cx_Oracle

    return cx_Oracle.connect(
        user=os.getenv("ORACLE_USER", "BIGDATA_VISION"),
        password=os.getenv("ORACLE_PASSWORD", "BIGDATA_VISION"),
        dsn=os.getenv("ORACLE_DSN", "10.195.227.115:1526/VISION"),
        encoding="UTF-8",
    )


@app.get("/", response_class=HTMLResponse)
def index(request: Request) -> HTMLResponse:
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/api/queue/stats")
def queue_stats(period: str = Query("day", pattern="^(day|week)$")) -> List[Dict[str, Any]]:
    trunc_expr = "TRUNC(SNAP_TIME)" if period == "day" else "TRUNC(SNAP_TIME, 'IW')"
    sql = f"""
    SELECT {trunc_expr} AS bucket_time,
           QUEUE_PATH,
           MIN(USED_MEMORY_MB) AS min_used_memory_mb,
           MAX(USED_MEMORY_MB) AS max_used_memory_mb,
           PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY USED_MEMORY_MB) AS p95_used_memory_mb
      FROM YARN_QUEUE_RESOURCE_SNAP
     WHERE SNAP_TIME >= TRUNC(SYSDATE) - CASE WHEN :period = 'day' THEN 7 ELSE 56 END
     GROUP BY {trunc_expr}, QUEUE_PATH
     ORDER BY bucket_time, QUEUE_PATH
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, {"period": period})
        return [
            {
                "bucket_time": row[0].isoformat(),
                "queue_path": row[1],
                "min_used_memory_mb": float(row[2] or 0),
                "max_used_memory_mb": float(row[3] or 0),
                "p95_used_memory_mb": float(row[4] or 0),
            }
            for row in cur.fetchall()
        ]


@app.get("/api/today/usage")
def today_usage() -> Dict[str, Any]:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT TO_CHAR(SNAP_TIME, 'YYYY-MM-DD HH24:MI') AS ts, MB_ALLOCATED
              FROM YARN_CLUSTER_RESOURCE_SNAP
             WHERE SNAP_TIME >= TRUNC(SYSDATE)
             ORDER BY SNAP_TIME
            """
        )
        cluster = [{"ts": r[0], "allocated_mb": float(r[1] or 0)} for r in cur.fetchall()]

        cur.execute(
            """
            SELECT TO_CHAR(SNAP_TIME, 'YYYY-MM-DD HH24:MI') AS ts,
                   QUEUE_PATH,
                   USED_MEMORY_MB
              FROM YARN_QUEUE_RESOURCE_SNAP
             WHERE SNAP_TIME >= TRUNC(SYSDATE)
             ORDER BY SNAP_TIME
            """
        )
        queue_rows = cur.fetchall()

    queue_series: Dict[str, List[Dict[str, Any]]] = {}
    for ts, queue_path, used_memory_mb in queue_rows:
        queue_series.setdefault(queue_path, []).append({"ts": ts, "used_memory_mb": float(used_memory_mb or 0)})
    return {"cluster": cluster, "queues": queue_series}


@app.get("/api/apps/daily-summary")
def apps_daily_summary(days: int = Query(14, ge=3, le=90)) -> List[Dict[str, Any]]:
    sql = """
    SELECT TRUNC(FIRST_SEEN_TIME) AS bucket_day,
           COUNT(*) AS total_apps,
           SUM(CASE WHEN RESULT_TAG = 'success' THEN 1 ELSE 0 END) AS success_apps,
           SUM(CASE WHEN RESULT_TAG = 'failed' THEN 1 ELSE 0 END) AS failed_apps,
           SUM(CASE WHEN RESULT_TAG = 'running' THEN 1 ELSE 0 END) AS running_apps,
           AVG(MAX_ALLOCATED_MB) AS avg_max_allocated_mb,
           PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY MAX_ALLOCATED_MB) AS p95_max_allocated_mb
      FROM YARN_APP_LIFECYCLE
     WHERE FIRST_SEEN_TIME >= TRUNC(SYSDATE) - :days
     GROUP BY TRUNC(FIRST_SEEN_TIME)
     ORDER BY bucket_day
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, {"days": days})
        return [
            {
                "bucket_day": r[0].isoformat(),
                "total_apps": int(r[1] or 0),
                "success_apps": int(r[2] or 0),
                "failed_apps": int(r[3] or 0),
                "running_apps": int(r[4] or 0),
                "avg_max_allocated_mb": float(r[5] or 0),
                "p95_max_allocated_mb": float(r[6] or 0),
            }
            for r in cur.fetchall()
        ]


@app.get("/api/apps/by-queue")
def apps_by_queue(
    queue: str | None = None,
    result_tag: str | None = Query(None, pattern="^(success|failed|running)$"),
    day: date | None = None,
) -> List[Dict[str, Any]]:
    filters = ["1=1"]
    params: Dict[str, Any] = {}
    if queue:
        filters.append("QUEUE_NAME = :queue")
        params["queue"] = queue
    if result_tag:
        filters.append("RESULT_TAG = :result_tag")
        params["result_tag"] = result_tag
    if day:
        filters.append("FIRST_SEEN_TIME >= :day_start AND FIRST_SEEN_TIME < :day_end")
        params["day_start"] = day
        params["day_end"] = day + timedelta(days=1)

    sql = f"""
    SELECT APP_ID, APP_NAME, USER_NAME, QUEUE_NAME, RESULT_TAG, LAST_STATE, FINAL_STATUS,
           MAX_ALLOCATED_MB, MAX_ALLOCATED_VCORES, MAX_RUNNING_CONTAINERS,
           FIRST_SEEN_TIME, NVL(FINISHED_TIME, LAST_SEEN_TIME)
      FROM YARN_APP_LIFECYCLE
     WHERE {' AND '.join(filters)}
     ORDER BY FIRST_SEEN_TIME DESC
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        return [
            {
                "app_id": r[0],
                "app_name": r[1],
                "user_name": r[2],
                "queue_name": r[3],
                "result_tag": r[4],
                "last_state": r[5],
                "final_status": r[6],
                "max_allocated_mb": float(r[7] or 0),
                "max_allocated_vcores": float(r[8] or 0),
                "max_running_containers": float(r[9] or 0),
                "start_time": r[10].isoformat() if r[10] else None,
                "end_time": r[11].isoformat() if r[11] else None,
            }
            for r in cur.fetchall()
        ]
