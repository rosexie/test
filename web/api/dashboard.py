from __future__ import annotations

from datetime import date, timedelta
from typing import Any, Dict, List

from fastapi import APIRouter, Query

from web.db import get_conn

router = APIRouter(prefix="/api/dashboard", tags=["dashboard"])
legacy_router = APIRouter(prefix="/api", tags=["dashboard-legacy"])


def _queue_stats(period: str) -> List[Dict[str, Any]]:
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


@router.get("/queue/stats")
def queue_stats(period: str = Query("day", pattern="^(day|week)$")) -> List[Dict[str, Any]]:
    return _queue_stats(period)


@legacy_router.get("/queue/stats")
def queue_stats_legacy(period: str = Query("day", pattern="^(day|week)$")) -> List[Dict[str, Any]]:
    return _queue_stats(period)


def _queue_overview(period: str, top_n: int) -> List[Dict[str, Any]]:
    lookback_days = 7 if period == "day" else 56
    sql = """
    SELECT *
      FROM (
        SELECT QUEUE_PATH,
               AVG(USED_MEMORY_MB) AS avg_used_memory_mb,
               MAX(USED_MEMORY_MB) AS peak_used_memory_mb,
               PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY USED_MEMORY_MB) AS p95_used_memory_mb,
               MAX(SNAP_TIME) AS last_snap_time
          FROM YARN_QUEUE_RESOURCE_SNAP
         WHERE SNAP_TIME >= TRUNC(SYSDATE) - :lookback_days
         GROUP BY QUEUE_PATH
         ORDER BY peak_used_memory_mb DESC
      )
     WHERE ROWNUM <= :top_n
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, {"lookback_days": lookback_days, "top_n": top_n})
        return [
            {
                "queue_path": row[0],
                "avg_used_memory_mb": float(row[1] or 0),
                "peak_used_memory_mb": float(row[2] or 0),
                "p95_used_memory_mb": float(row[3] or 0),
                "last_snap_time": row[4].isoformat() if row[4] else None,
            }
            for row in cur.fetchall()
        ]


@router.get("/queue/overview")
def queue_overview(period: str = Query("day", pattern="^(day|week)$"), top_n: int = Query(8, ge=3, le=20)) -> List[Dict[str, Any]]:
    return _queue_overview(period, top_n)


def _today_usage() -> Dict[str, Any]:
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


@router.get("/usage/today")
def today_usage() -> Dict[str, Any]:
    return _today_usage()


@legacy_router.get("/today/usage")
def today_usage_legacy() -> Dict[str, Any]:
    return _today_usage()


def _apps_daily_summary(days: int) -> List[Dict[str, Any]]:
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


@router.get("/apps/daily-summary")
def apps_daily_summary(days: int = Query(14, ge=3, le=90)) -> List[Dict[str, Any]]:
    return _apps_daily_summary(days)


def _apps_queue_summary(days: int, top_n: int) -> List[Dict[str, Any]]:
    sql = """
    SELECT *
      FROM (
        SELECT NVL(QUEUE_NAME, '(unknown)') AS queue_name,
               COUNT(*) AS total_apps,
               SUM(CASE WHEN RESULT_TAG = 'success' THEN 1 ELSE 0 END) AS success_apps,
               SUM(CASE WHEN RESULT_TAG = 'failed' THEN 1 ELSE 0 END) AS failed_apps,
               SUM(CASE WHEN RESULT_TAG = 'running' THEN 1 ELSE 0 END) AS running_apps,
               AVG(MAX_ALLOCATED_MB) AS avg_max_allocated_mb,
               PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY MAX_ALLOCATED_MB) AS p95_max_allocated_mb
          FROM YARN_APP_LIFECYCLE
         WHERE FIRST_SEEN_TIME >= TRUNC(SYSDATE) - :days
         GROUP BY NVL(QUEUE_NAME, '(unknown)')
         ORDER BY total_apps DESC
      )
     WHERE ROWNUM <= :top_n
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, {"days": days, "top_n": top_n})
        return [
            {
                "queue_name": r[0],
                "total_apps": int(r[1] or 0),
                "success_apps": int(r[2] or 0),
                "failed_apps": int(r[3] or 0),
                "running_apps": int(r[4] or 0),
                "avg_max_allocated_mb": float(r[5] or 0),
                "p95_max_allocated_mb": float(r[6] or 0),
            }
            for r in cur.fetchall()
        ]


@router.get("/apps/queue-summary")
def apps_queue_summary(days: int = Query(7, ge=1, le=30), top_n: int = Query(12, ge=5, le=30)) -> List[Dict[str, Any]]:
    return _apps_queue_summary(days, top_n)


def _apps_recent(queue: str | None, result_tag: str | None, day: date | None) -> List[Dict[str, Any]]:
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


@router.get("/apps/recent")
def apps_recent(
    queue: str | None = None,
    result_tag: str | None = Query(None, pattern="^(success|failed|running)$"),
    day: date | None = None,
) -> List[Dict[str, Any]]:
    return _apps_recent(queue, result_tag, day)


@legacy_router.get("/apps/by-queue")
def apps_by_queue_legacy(
    queue: str | None = None,
    result_tag: str | None = Query(None, pattern="^(success|failed|running)$"),
    day: date | None = None,
) -> List[Dict[str, Any]]:
    return _apps_recent(queue, result_tag, day)
