## YARN 资源采集与看板

### 1. 采集
```bash
python yarn_collector.py
```

### 2. 看板
```bash
uvicorn web.app:app --host 0.0.0.0 --port 8000
```

### 3. 测试
```bash
python -m unittest discover -s tests -p 'test_*.py' -v
```

## 页面与接口结构（可扩展）
- 页面元数据：`GET /api/meta/pages`
- 看板接口统一挂在：`/api/dashboard/*`
  - `GET /api/dashboard/queue/stats`
  - `GET /api/dashboard/queue/overview`
  - `GET /api/dashboard/usage/today`
  - `GET /api/dashboard/apps/daily-summary`
  - `GET /api/dashboard/apps/queue-summary`
  - `GET /api/dashboard/apps/recent`

> 后续新增页面时，优先在 `web/pages.py` 注册页面，再按页面域在 `web/api/` 下新增 router。

- 兼容旧版接口（避免已有页面/脚本404）：
  - `GET /api/queue/stats`
  - `GET /api/queue/overview`
  - `GET /api/today/usage`
  - `GET /api/apps/daily-summary`
  - `GET /api/apps/by-queue`
  - `GET /api/apps/recent`

### 环境变量
- `YARN_BASE_URL`
- `SOCKS5_PROXY`
- `ORACLE_USER`
- `ORACLE_PASSWORD`
- `ORACLE_DSN`
- `COLLECT_INTERVAL_SECONDS`
- `APP_HEARTBEAT_PERSIST_SECONDS`（默认300秒，控制长跑任务状态的最小持久化间隔，降低频繁更新）
