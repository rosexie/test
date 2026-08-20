## YARN 资源采集与看板

### 1. 采集
```bash
python yarn_collector.py
```

### 2. 看板
```bash
uvicorn web.app:app --host 0.0.0.0 --port 8000
```

### 环境变量
- `YARN_BASE_URL`
- `SOCKS5_PROXY`
- `ORACLE_USER`
- `ORACLE_PASSWORD`
- `ORACLE_DSN`
- `COLLECT_INTERVAL_SECONDS`
- `APP_HEARTBEAT_PERSIST_SECONDS`（默认300秒，控制长跑任务状态的最小持久化间隔，降低频繁更新）
