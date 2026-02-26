#!/usr/bin/env python3
"""YARN 任务与队列资源采集脚本（每分钟采集并落库 Oracle/cx_Oracle）。"""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional

import cx_Oracle
import requests

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(message)s",
)
LOGGER = logging.getLogger("yarn_collector")


@dataclass
class Settings:
    yarn_base_url: str = os.getenv("YARN_BASE_URL", "http://persp-54.persp.net:8088")
    socks5_proxy: str = os.getenv("SOCKS5_PROXY", "socks5h://10.195.229.16:1080")
    oracle_user: str = os.getenv("ORACLE_USER", "BIGDATA_VISION")
    oracle_password: str = os.getenv("ORACLE_PASSWORD", "BIGDATA_VISION")
    oracle_dsn: str = os.getenv("ORACLE_DSN", "10.195.227.115:1526/VISION")
    collect_interval_seconds: int = int(os.getenv("COLLECT_INTERVAL_SECONDS", "60"))

    @property
    def proxies(self) -> Dict[str, str]:
        return {"http": self.socks5_proxy, "https": self.socks5_proxy}


class YarnOracleCollector:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.session = requests.Session()
        self.session.proxies.update(settings.proxies)

    def ensure_tables(self, conn: cx_Oracle.Connection) -> None:
        ddl_statements = [
            """
            BEGIN
              EXECUTE IMMEDIATE '
                CREATE TABLE YARN_QUEUE_RESOURCE_SNAP (
                  SNAP_TIME            TIMESTAMP NOT NULL,
                  QUEUE_PATH           VARCHAR2(512) NOT NULL,
                  QUEUE_NAME           VARCHAR2(256),
                  QUEUE_STATE          VARCHAR2(64),
                  USED_CAPACITY        NUMBER,
                  CAPACITY             NUMBER,
                  MAX_CAPACITY         NUMBER,
                  ABS_USED_CAPACITY    NUMBER,
                  ABS_CAPACITY         NUMBER,
                  ABS_MAX_CAPACITY     NUMBER,
                  USED_MEMORY_MB       NUMBER,
                  USED_VCORES          NUMBER,
                  NUM_APPLICATIONS     NUMBER
                )
              ';
            EXCEPTION WHEN OTHERS THEN IF SQLCODE != -955 THEN RAISE; END IF;
            END;
            """,
            """
            BEGIN
              EXECUTE IMMEDIATE '
                CREATE TABLE YARN_CLUSTER_RESOURCE_SNAP (
                  SNAP_TIME            TIMESTAMP NOT NULL,
                  APPS_RUNNING         NUMBER,
                  APPS_PENDING         NUMBER,
                  APPS_COMPLETED       NUMBER,
                  APPS_FAILED          NUMBER,
                  APPS_KILLED          NUMBER,
                  MB_TOTAL             NUMBER,
                  MB_ALLOCATED         NUMBER,
                  MB_AVAILABLE         NUMBER,
                  VC_TOTAL             NUMBER,
                  VC_ALLOCATED         NUMBER,
                  VC_AVAILABLE         NUMBER,
                  CONTAINERS_ALLOCATED NUMBER,
                  NODES_ACTIVE         NUMBER,
                  NODES_LOST           NUMBER,
                  NODES_UNHEALTHY      NUMBER
                )
              ';
            EXCEPTION WHEN OTHERS THEN IF SQLCODE != -955 THEN RAISE; END IF;
            END;
            """,
            """
            BEGIN
              EXECUTE IMMEDIATE '
                CREATE TABLE YARN_APP_LIFECYCLE (
                  APP_ID                 VARCHAR2(128) PRIMARY KEY,
                  APP_NAME               VARCHAR2(1024),
                  USER_NAME              VARCHAR2(128),
                  QUEUE_NAME             VARCHAR2(256),
                  FIRST_SEEN_TIME        TIMESTAMP,
                  LAST_SEEN_TIME         TIMESTAMP,
                  LAST_STATE             VARCHAR2(64),
                  FINAL_STATUS           VARCHAR2(64),
                  RESULT_TAG             VARCHAR2(32),
                  MAX_RUNNING_CONTAINERS NUMBER,
                  MAX_ALLOCATED_MB       NUMBER,
                  MAX_ALLOCATED_VCORES   NUMBER,
                  MAX_RESERVED_MB        NUMBER,
                  MAX_RESERVED_VCORES    NUMBER,
                  MAX_PROGRESS           NUMBER,
                  ELAPSED_TIME_MS        NUMBER,
                  FINISHED_TIME          TIMESTAMP,
                  FINISH_RECORDED        NUMBER(1) DEFAULT 0
                )
              ';
            EXCEPTION WHEN OTHERS THEN IF SQLCODE != -955 THEN RAISE; END IF;
            END;
            """,
        ]
        with conn.cursor() as cur:
            for ddl in ddl_statements:
                cur.execute(ddl)
        conn.commit()

    def _get_json(self, url: str) -> Dict[str, Any]:
        resp = self.session.get(url, timeout=20)
        resp.raise_for_status()
        return resp.json()

    def fetch_apps(self) -> List[Dict[str, Any]]:
        payload = self._get_json(f"{self.settings.yarn_base_url}/ws/v1/cluster/apps")
        apps = payload.get("apps", {}).get("app", [])
        return apps if isinstance(apps, list) else []

    def fetch_scheduler(self) -> Dict[str, Any]:
        payload = self._get_json(f"{self.settings.yarn_base_url}/ws/v1/cluster/scheduler")
        return payload.get("scheduler", {}).get("schedulerInfo", {})

    def fetch_cluster_metrics(self) -> Dict[str, Any]:
        payload = self._get_json(f"{self.settings.yarn_base_url}/ws/v1/cluster/metrics")
        return payload.get("clusterMetrics", {})

    def _iter_child_queues(self, queue_info: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
        for key in ("queues", "childQueues"):
            node = queue_info.get(key)
            if isinstance(node, dict):
                for q in node.get("queue", []) or []:
                    if isinstance(q, dict):
                        yield q
            elif isinstance(node, list):
                for q in node:
                    if isinstance(q, dict):
                        yield q

    def flatten_queues(self, root: Dict[str, Any], parent_path: str = "") -> List[Dict[str, Any]]:
        queue_name = root.get("queueName") or root.get("queuePath") or "root"
        queue_path = f"{parent_path}.{queue_name}" if parent_path else str(queue_name)
        used_resources = root.get("resourcesUsed") or root.get("usedResources") or {}

        row = {
            "queue_path": queue_path,
            "queue_name": queue_name,
            "queue_state": root.get("state"),
            "used_capacity": root.get("usedCapacity"),
            "capacity": root.get("capacity"),
            "max_capacity": root.get("maxCapacity"),
            "abs_used_capacity": root.get("absoluteUsedCapacity"),
            "abs_capacity": root.get("absoluteCapacity"),
            "abs_max_capacity": root.get("absoluteMaxCapacity"),
            "used_memory_mb": used_resources.get("memory") or used_resources.get("memoryMB"),
            "used_vcores": used_resources.get("vCores") or used_resources.get("vcores"),
            "num_applications": root.get("numApplications"),
        }

        rows = [row]
        for child in self._iter_child_queues(root):
            rows.extend(self.flatten_queues(child, parent_path=queue_path))
        return rows

    @staticmethod
    def _result_tag(state: Optional[str], final_status: Optional[str]) -> str:
        if final_status == "SUCCEEDED":
            return "success"
        if state in {"FAILED", "KILLED"} or final_status in {"FAILED", "KILLED"}:
            return "failed"
        return "running"

    def save_snapshot(
        self,
        conn: cx_Oracle.Connection,
        snap_time: datetime,
        app_rows: List[Dict[str, Any]],
        queue_rows: List[Dict[str, Any]],
        cluster_metrics: Dict[str, Any],
    ) -> None:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO YARN_QUEUE_RESOURCE_SNAP (
                  SNAP_TIME, QUEUE_PATH, QUEUE_NAME, QUEUE_STATE,
                  USED_CAPACITY, CAPACITY, MAX_CAPACITY,
                  ABS_USED_CAPACITY, ABS_CAPACITY, ABS_MAX_CAPACITY,
                  USED_MEMORY_MB, USED_VCORES, NUM_APPLICATIONS
                ) VALUES (
                  :1, :2, :3, :4,
                  :5, :6, :7,
                  :8, :9, :10,
                  :11, :12, :13
                )
                """,
                [(
                    snap_time,
                    q["queue_path"],
                    q["queue_name"],
                    q["queue_state"],
                    q["used_capacity"],
                    q["capacity"],
                    q["max_capacity"],
                    q["abs_used_capacity"],
                    q["abs_capacity"],
                    q["abs_max_capacity"],
                    q["used_memory_mb"],
                    q["used_vcores"],
                    q["num_applications"],
                ) for q in queue_rows],
            )

            cur.execute(
                """
                INSERT INTO YARN_CLUSTER_RESOURCE_SNAP (
                  SNAP_TIME, APPS_RUNNING, APPS_PENDING, APPS_COMPLETED, APPS_FAILED, APPS_KILLED,
                  MB_TOTAL, MB_ALLOCATED, MB_AVAILABLE,
                  VC_TOTAL, VC_ALLOCATED, VC_AVAILABLE,
                  CONTAINERS_ALLOCATED, NODES_ACTIVE, NODES_LOST, NODES_UNHEALTHY
                ) VALUES (
                  :1, :2, :3, :4, :5, :6,
                  :7, :8, :9,
                  :10, :11, :12,
                  :13, :14, :15, :16
                )
                """,
                (
                    snap_time,
                    cluster_metrics.get("appsRunning"),
                    cluster_metrics.get("appsPending"),
                    cluster_metrics.get("appsCompleted"),
                    cluster_metrics.get("appsFailed"),
                    cluster_metrics.get("appsKilled"),
                    cluster_metrics.get("totalMB"),
                    cluster_metrics.get("allocatedMB"),
                    cluster_metrics.get("availableMB"),
                    cluster_metrics.get("totalVirtualCores"),
                    cluster_metrics.get("allocatedVirtualCores"),
                    cluster_metrics.get("availableVirtualCores"),
                    cluster_metrics.get("containersAllocated"),
                    cluster_metrics.get("activeNodes"),
                    cluster_metrics.get("lostNodes"),
                    cluster_metrics.get("unhealthyNodes"),
                ),
            )

            for app in app_rows:
                state = app.get("state")
                final_status = app.get("finalStatus")
                is_finished = state in {"FINISHED", "FAILED", "KILLED"}
                cur.execute(
                    """
                    MERGE INTO YARN_APP_LIFECYCLE t
                    USING (
                      SELECT :app_id AS app_id, :app_name AS app_name, :user_name AS user_name, :queue_name AS queue_name,
                             :snap_time AS snap_time, :state AS state, :final_status AS final_status,
                             :running_containers AS running_containers, :allocated_mb AS allocated_mb,
                             :allocated_vcores AS allocated_vcores, :reserved_mb AS reserved_mb,
                             :reserved_vcores AS reserved_vcores, :progress AS progress,
                             :elapsed_time_ms AS elapsed_time_ms, :result_tag AS result_tag,
                             :is_finished AS is_finished
                      FROM dual
                    ) s
                    ON (t.APP_ID = s.app_id)
                    WHEN MATCHED THEN UPDATE SET
                      t.APP_NAME = s.app_name,
                      t.USER_NAME = s.user_name,
                      t.QUEUE_NAME = s.queue_name,
                      t.LAST_SEEN_TIME = s.snap_time,
                      t.LAST_STATE = s.state,
                      t.FINAL_STATUS = s.final_status,
                      t.RESULT_TAG = s.result_tag,
                      t.MAX_RUNNING_CONTAINERS = GREATEST(NVL(t.MAX_RUNNING_CONTAINERS, 0), NVL(s.running_containers, 0)),
                      t.MAX_ALLOCATED_MB = GREATEST(NVL(t.MAX_ALLOCATED_MB, 0), NVL(s.allocated_mb, 0)),
                      t.MAX_ALLOCATED_VCORES = GREATEST(NVL(t.MAX_ALLOCATED_VCORES, 0), NVL(s.allocated_vcores, 0)),
                      t.MAX_RESERVED_MB = GREATEST(NVL(t.MAX_RESERVED_MB, 0), NVL(s.reserved_mb, 0)),
                      t.MAX_RESERVED_VCORES = GREATEST(NVL(t.MAX_RESERVED_VCORES, 0), NVL(s.reserved_vcores, 0)),
                      t.MAX_PROGRESS = GREATEST(NVL(t.MAX_PROGRESS, 0), NVL(s.progress, 0)),
                      t.ELAPSED_TIME_MS = NVL(s.elapsed_time_ms, t.ELAPSED_TIME_MS),
                      t.FINISHED_TIME = CASE WHEN s.is_finished = 1 THEN s.snap_time ELSE t.FINISHED_TIME END,
                      t.FINISH_RECORDED = CASE WHEN s.is_finished = 1 THEN 1 ELSE t.FINISH_RECORDED END
                    WHEN NOT MATCHED THEN INSERT (
                      APP_ID, APP_NAME, USER_NAME, QUEUE_NAME,
                      FIRST_SEEN_TIME, LAST_SEEN_TIME, LAST_STATE, FINAL_STATUS, RESULT_TAG,
                      MAX_RUNNING_CONTAINERS, MAX_ALLOCATED_MB, MAX_ALLOCATED_VCORES,
                      MAX_RESERVED_MB, MAX_RESERVED_VCORES, MAX_PROGRESS,
                      ELAPSED_TIME_MS, FINISHED_TIME, FINISH_RECORDED
                    ) VALUES (
                      s.app_id, s.app_name, s.user_name, s.queue_name,
                      s.snap_time, s.snap_time, s.state, s.final_status, s.result_tag,
                      NVL(s.running_containers, 0), NVL(s.allocated_mb, 0), NVL(s.allocated_vcores, 0),
                      NVL(s.reserved_mb, 0), NVL(s.reserved_vcores, 0), NVL(s.progress, 0),
                      s.elapsed_time_ms, CASE WHEN s.is_finished = 1 THEN s.snap_time ELSE NULL END,
                      CASE WHEN s.is_finished = 1 THEN 1 ELSE 0 END
                    )
                    """,
                    {
                        "app_id": app.get("id"),
                        "app_name": app.get("name"),
                        "user_name": app.get("user"),
                        "queue_name": app.get("queue"),
                        "snap_time": snap_time,
                        "state": state,
                        "final_status": final_status,
                        "running_containers": app.get("runningContainers"),
                        "allocated_mb": app.get("allocatedMB"),
                        "allocated_vcores": app.get("allocatedVCores"),
                        "reserved_mb": app.get("reservedMB"),
                        "reserved_vcores": app.get("reservedVCores"),
                        "progress": app.get("progress"),
                        "elapsed_time_ms": app.get("elapsedTime"),
                        "result_tag": self._result_tag(state, final_status),
                        "is_finished": 1 if is_finished else 0,
                    },
                )
        conn.commit()

    def collect_once(self, conn: cx_Oracle.Connection) -> None:
        snap_time = datetime.now()
        apps = self.fetch_apps()
        scheduler_root = self.fetch_scheduler()
        queues = self.flatten_queues(scheduler_root)
        cluster_metrics = self.fetch_cluster_metrics()

        self.save_snapshot(conn, snap_time, apps, queues, cluster_metrics)
        LOGGER.info("采集完成: apps=%s queues=%s snap_time=%s", len(apps), len(queues), snap_time.isoformat(timespec="seconds"))

    def run_forever(self) -> None:
        with cx_Oracle.connect(
            user=self.settings.oracle_user,
            password=self.settings.oracle_password,
            dsn=self.settings.oracle_dsn,
            encoding="UTF-8",
        ) as conn:
            self.ensure_tables(conn)
            LOGGER.info("YARN采集启动，周期=%s秒", self.settings.collect_interval_seconds)
            while True:
                start = time.monotonic()
                try:
                    self.collect_once(conn)
                except Exception:  # noqa: BLE001
                    LOGGER.exception("采集失败")
                elapsed = time.monotonic() - start
                time.sleep(max(self.settings.collect_interval_seconds - elapsed, 1))


def main() -> None:
    settings = Settings()
    LOGGER.info("当前配置: %s", json.dumps(settings.__dict__, ensure_ascii=False))
    YarnOracleCollector(settings).run_forever()


if __name__ == "__main__":
    main()
