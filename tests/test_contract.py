import unittest
from pathlib import Path

from web.pages import PAGES


class DashboardContractTest(unittest.TestCase):
    def test_pages_registry_is_extensible(self):
        keys = {p['key'] for p in PAGES}
        self.assertIn('dashboard', keys)
        self.assertIn('capacity-planning', keys)

    def test_dashboard_router_contract(self):
        content = Path('web/api/dashboard.py').read_text(encoding='utf-8')
        self.assertIn('APIRouter(prefix="/api/dashboard"', content)
        for path in [
            '/queue/stats',
            '/queue/overview',
            '/usage/today',
            '/apps/daily-summary',
            '/apps/queue-summary',
            '/apps/recent',
        ]:
            self.assertIn(f'@router.get("{path}")', content)

    def test_legacy_router_contract(self):
        content = Path('web/api/dashboard.py').read_text(encoding='utf-8')
        self.assertIn('APIRouter(prefix="/api"', content)
        for path in ['/queue/stats', '/today/usage', '/apps/by-queue']:
            self.assertIn(f'@legacy_router.get("{path}")', content)

    def test_frontend_uses_namespaced_api(self):
        content = Path('web/static/app.js').read_text(encoding='utf-8')
        self.assertIn('/api/dashboard/queue/stats', content)
        self.assertIn('/api/meta/pages', content)
        self.assertIn('showEmpty', content)


if __name__ == '__main__':
    unittest.main()
