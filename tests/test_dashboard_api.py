import unittest
from datetime import datetime
from unittest.mock import patch

try:
    from fastapi.testclient import TestClient
    from web.app import create_app
except ModuleNotFoundError:  # pragma: no cover - environment dependency
    TestClient = None
    create_app = None


class FakeCursor:
    def __init__(self):
        self.sql = ""
        self.call_count = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        self.sql = sql
        self.call_count += 1

    def fetchall(self):
        if "FROM YARN_CLUSTER_RESOURCE_SNAP" in self.sql:
            return [("2026-01-01 10:00", 2048.0)]
        if "FROM YARN_QUEUE_RESOURCE_SNAP" in self.sql and "GROUP BY" in self.sql and "QUEUE_PATH" in self.sql and "min_used_memory_mb" in self.sql:
            return [(datetime(2026, 1, 1), "root.a", 100.0, 300.0, 280.0)]
        if "FROM YARN_QUEUE_RESOURCE_SNAP" in self.sql and "avg_used_memory_mb" in self.sql:
            return [("root.a", 200.0, 320.0, 300.0, datetime(2026, 1, 1, 10, 0))]
        if "FROM YARN_QUEUE_RESOURCE_SNAP" in self.sql and "ORDER BY SNAP_TIME" in self.sql and "QUEUE_PATH" in self.sql:
            return [("2026-01-01 10:00", "root.a", 220.0)]
        if "FROM YARN_APP_LIFECYCLE" in self.sql and "bucket_day" in self.sql:
            return [(datetime(2026, 1, 1), 5, 4, 1, 0, 180.0, 250.0)]
        if "FROM YARN_APP_LIFECYCLE" in self.sql and "GROUP BY NVL(QUEUE_NAME" in self.sql:
            return [("root.a", 10, 8, 1, 1, 256.0, 400.0)]
        if "FROM YARN_APP_LIFECYCLE" in self.sql and "SELECT APP_ID" in self.sql:
            return [(
                "app_1",
                "demo",
                "alice",
                "root.a",
                "success",
                "FINISHED",
                "SUCCEEDED",
                1024.0,
                2.0,
                1.0,
                datetime(2026, 1, 1, 9, 0),
                datetime(2026, 1, 1, 10, 0),
            )]
        return []


class FakeConn:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self):
        return FakeCursor()


def fake_get_conn():
    return FakeConn()


class DashboardApiTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if TestClient is None:
            raise unittest.SkipTest("fastapi/testclient is not available in this environment")

    def setUp(self):
        self.client = TestClient(create_app())

    @patch("web.api.dashboard.get_conn", side_effect=fake_get_conn)
    def test_each_dashboard_panel_endpoint_returns_data(self, _mock_conn):
        endpoints = [
            "/api/dashboard/queue/overview?period=day&top_n=8",
            "/api/dashboard/apps/daily-summary?days=14",
            "/api/dashboard/apps/queue-summary?days=14&top_n=8",
            "/api/dashboard/apps/recent",
        ]
        for url in endpoints:
            with self.subTest(url=url):
                resp = self.client.get(url)
                self.assertEqual(resp.status_code, 200)
                data = resp.json()
                self.assertIsInstance(data, list)
                self.assertGreater(len(data), 0)

    @patch("web.api.dashboard.get_conn", side_effect=fake_get_conn)
    def test_legacy_aliases_match_expected_shapes(self, _mock_conn):
        resp_overview = self.client.get("/api/queue/overview?period=day&top_n=8")
        self.assertEqual(resp_overview.status_code, 200)
        self.assertIn("peak_used_memory_mb", resp_overview.json()[0])

        resp_daily = self.client.get("/api/apps/daily-summary?days=14")
        self.assertEqual(resp_daily.status_code, 200)
        self.assertIn("p95_max_allocated_mb", resp_daily.json()[0])

        resp_summary = self.client.get("/api/apps/by-queue?days=14&top_n=8")
        self.assertEqual(resp_summary.status_code, 200)
        self.assertIn("total_apps", resp_summary.json()[0])

        resp_recent = self.client.get("/api/apps/recent")
        self.assertEqual(resp_recent.status_code, 200)
        self.assertIn("app_id", resp_recent.json()[0])


if __name__ == "__main__":
    unittest.main()
