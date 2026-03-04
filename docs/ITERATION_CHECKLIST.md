## Dashboard Iteration Checklist (must pass before delivery)

### 0) Start app
```bash
uvicorn web.app:app --host 0.0.0.0 --port 8000
```

### 1) Mandatory automated checks
```bash
python -m unittest discover -s tests -p 'test_*.py' -v
node --check web/static/app.js
python scripts/verify_dashboard.py --base-url http://127.0.0.1:8000
```

### 2) What `verify_dashboard.py` validates
- `GET /api/dashboard/queue/overview` returns non-empty rows with `peak/p95/avg` keys.
- `GET /api/dashboard/apps/daily-summary` returns non-empty rows for daily app metrics.
- `GET /api/dashboard/apps/queue-summary` returns non-empty queue summary rows.
- `GET /` includes panel containers (`queueOverview`, `dailyApps`, `queueAppSummary`) and refresh button.
- `/static/app.js` binds `refreshBtn` to `refreshDashboard`.

### 3) Manual sanity checks (UI)
- Open dashboard and click **刷新看板** once.
- Confirm these 3 panels are not blank:
  - 队列资源总览（Peak/P95/Avg）
  - 每日任务量 / 成功率 / P95资源
  - 队列任务汇总（窗口内）
- Switch 参数（近7/14/30天、TopN）后再次点击刷新，确认内容变化。

### 4) Failure policy
- Any failed check blocks delivery.
- Include failing endpoint and payload snippet in commit/PR notes.
