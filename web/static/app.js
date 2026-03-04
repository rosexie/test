async function getJSON(url) {
  const r = await fetch(url);
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}

function normalizeKey(key) {
  return String(key || '').trim().toLowerCase();
}

function normalizeObjectKeys(row) {
  if (!row || typeof row !== 'object' || Array.isArray(row)) return row;
  const out = {};
  Object.entries(row).forEach(([k, v]) => {
    out[normalizeKey(k)] = v;
  });
  return out;
}

function extractRows(payload) {
  if (Array.isArray(payload)) return payload.map(normalizeObjectKeys);
  if (payload && typeof payload === 'object') {
    for (const key of ['rows', 'items', 'data', 'result']) {
      if (Array.isArray(payload[key])) return payload[key].map(normalizeObjectKeys);
    }
  }
  return [];
}

async function getRowsWithFallback(urls) {
  const errors = [];
  for (const url of urls) {
    try {
      const payload = await getJSON(url);
      return extractRows(payload);
    } catch (err) {
      errors.push(`${url}: ${err.message}`);
    }
  }
  throw new Error(`all endpoints failed -> ${errors.join(' | ')}`);
}

function escapeHTML(value) {
  return String(value ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}

function toGB(mb) {
  return `${(Number(mb || 0) / 1024).toFixed(1)} GB`;
}

function toPercent(v) {
  return `${(Number(v || 0) * 100).toFixed(1)}%`;
}

function normalizeResultTag(tag) {
  const t = String(tag || '').toLowerCase();
  return ['success', 'failed', 'running'].includes(t) ? t : 'running';
}

function showEmpty(id, text = '暂无数据') {
  document.getElementById(id).innerHTML = `<div class="empty">${escapeHTML(text)}</div>`;
}

function requireKeys(rows, keys, sectionName) {
  if (!rows.length) return;
  const missing = keys.filter(k => !(k in rows[0]));
  if (missing.length) throw new Error(`${sectionName} 返回字段异常，缺少: ${missing.join(', ')}`);
}

const state = { period: 'day', days: 14, topN: 8, queueMetric: 'max_used_memory_mb' };
const runtime = { lastQueueStats: [], recentAppsPromise: null };

const queueChart = echarts.init(document.getElementById('queueStats'));
const usageChart = echarts.init(document.getElementById('todayUsage'));
const dailyAppsChart = echarts.init(document.getElementById('dailyApps'));
const queueOverviewChart = echarts.init(document.getElementById('queueOverview'));

function updateRefreshStatus(text, cls = '') {
  const node = document.getElementById('refreshStatus');
  if (!node) return;
  node.className = `refresh-status ${cls}`.trim();
  node.textContent = text;
}

function pickTopQueuesFromStats(data, topN) {
  return Object.entries(
    data.reduce((acc, row) => {
      acc[row.queue_path] = Math.max(acc[row.queue_path] || 0, Number(row.max_used_memory_mb || 0));
      return acc;
    }, {})
  ).sort((a, b) => b[1] - a[1]).slice(0, topN).map(([queue]) => queue);
}

async function loadQueueStats() {
  const data = await getRowsWithFallback([
    `/api/dashboard/queue/stats?period=${state.period}`,
    `/api/queue/stats?period=${state.period}`,
  ]);
  runtime.lastQueueStats = data;
  if (!data.length) {
    queueChart.clear();
    return { topQueues: [] };
  }

  requireKeys(data, ['bucket_time', 'queue_path', 'max_used_memory_mb', 'p95_used_memory_mb', 'min_used_memory_mb'], '队列趋势');

  const buckets = [...new Set(data.map(d => String(d.bucket_time || '').slice(0, 10)))];
  const topQueues = pickTopQueuesFromStats(data, state.topN);
  const queueByBucket = {};
  topQueues.forEach(q => { queueByBucket[q] = Array(buckets.length).fill(0); });

  data.forEach(d => {
    if (!queueByBucket[d.queue_path]) return;
    const idx = buckets.indexOf(String(d.bucket_time || '').slice(0, 10));
    if (idx >= 0) queueByBucket[d.queue_path][idx] = Number(d[state.queueMetric] || 0);
  });

  const metricLabel = {
    max_used_memory_mb: '队列峰值内存',
    p95_used_memory_mb: '队列P95内存',
    min_used_memory_mb: '队列最小内存',
  }[state.queueMetric];

  queueChart.setOption({
    title: { text: `${metricLabel} 趋势`, left: 10, textStyle: { fontSize: 13 } },
    tooltip: { trigger: 'axis' },
    legend: { type: 'scroll' },
    xAxis: { type: 'category', data: buckets },
    yAxis: { type: 'value', name: 'MB' },
    series: Object.entries(queueByBucket).map(([name, values]) => ({ name, type: 'line', smooth: true, data: values })),
  });
  return { topQueues };
}

function renderQueueOverview(rows) {
  queueOverviewChart.setOption({
    tooltip: { trigger: 'axis' },
    legend: { type: 'scroll' },
    xAxis: { type: 'value', name: 'MB' },
    yAxis: { type: 'category', inverse: true, data: rows.map(r => r.queue_path) },
    series: [
      { name: '峰值', type: 'bar', data: rows.map(r => Number(r.peak_used_memory_mb || 0)) },
      { name: 'P95', type: 'bar', data: rows.map(r => Number(r.p95_used_memory_mb || 0)) },
      { name: '平均', type: 'bar', data: rows.map(r => Number(r.avg_used_memory_mb || 0)) },
    ],
  });
}

function deriveQueueOverviewFromStats(statsRows) {
  const acc = {};
  statsRows.forEach(r => {
    const q = r.queue_path;
    const v = Number(r.max_used_memory_mb || 0);
    if (!acc[q]) acc[q] = { queue_path: q, peak_used_memory_mb: 0, total: 0, count: 0, p95_used_memory_mb: 0 };
    acc[q].peak_used_memory_mb = Math.max(acc[q].peak_used_memory_mb, v);
    acc[q].p95_used_memory_mb = Math.max(acc[q].p95_used_memory_mb, Number(r.p95_used_memory_mb || 0));
    acc[q].total += v;
    acc[q].count += 1;
  });
  return Object.values(acc)
    .map(r => ({ ...r, avg_used_memory_mb: r.count ? r.total / r.count : 0 }))
    .sort((a, b) => b.peak_used_memory_mb - a.peak_used_memory_mb)
    .slice(0, state.topN);
}

async function loadQueueOverview() {
  try {
    const rows = await getRowsWithFallback([
      `/api/dashboard/queue/overview?period=${state.period}&top_n=${state.topN}`,
      `/api/queue/overview?period=${state.period}&top_n=${state.topN}`,
    ]);
    if (!rows.length) {
      queueOverviewChart.clear();
      return [];
    }
    requireKeys(rows, ['queue_path', 'peak_used_memory_mb', 'p95_used_memory_mb', 'avg_used_memory_mb'], '队列资源总览');
    renderQueueOverview(rows);
    return rows;
  } catch (err) {
    if (runtime.lastQueueStats.length) {
      const rows = deriveQueueOverviewFromStats(runtime.lastQueueStats);
      if (rows.length) {
        renderQueueOverview(rows);
        return rows;
      }
    }
    throw err;
  }
}

async function loadTodayUsage(preferredTopQueues = []) {
  const payload = await getJSON(`/api/dashboard/usage/today`).catch(async () => getJSON('/api/today/usage'));
  const data = {
    cluster: extractRows(payload.cluster || payload.cluster_rows || payload.clusterData || []),
    queues: payload.queues || {},
  };

  if (!data.cluster.length) {
    usageChart.clear();
    return data;
  }

  const x = data.cluster.map(d => String(d.ts || '').slice(11));
  const topQueues = preferredTopQueues.length ? preferredTopQueues : Object.keys(data.queues).slice(0, state.topN);
  const series = [{ name: '集群已分配MB', type: 'line', smooth: true, lineStyle: { width: 3 }, data: data.cluster.map(d => Number(d.allocated_mb || 0)) }];
  topQueues.forEach(q => {
    series.push({ name: q, type: 'line', smooth: true, data: (data.queues[q] || []).map(p => Number(p.used_memory_mb || 0)) });
  });

  usageChart.setOption({
    tooltip: { trigger: 'axis' },
    legend: { type: 'scroll' },
    xAxis: { type: 'category', data: x },
    yAxis: { type: 'value', name: 'MB' },
    series,
  });
  return data;
}

function toDateKey(v) {
  return String(v || '').slice(0, 10) || 'unknown';
}

function getRecentAppsRowsCached() {
  if (!runtime.recentAppsPromise) {
    runtime.recentAppsPromise = getRowsWithFallback(['/api/dashboard/apps/recent', '/api/apps/recent'])
      .finally(() => { runtime.recentAppsPromise = null; });
  }
  return runtime.recentAppsPromise;
}

function deriveDailySummaryFromApps(rows) {
  const acc = {};
  rows.forEach(r => {
    const day = toDateKey(r.start_time || r.first_seen_time || r.last_seen_time);
    const item = acc[day] || { bucket_day: day, total_apps: 0, success_apps: 0, failed_apps: 0, running_apps: 0, p95_max_allocated_mb: 0, _mb: [] };
    item.total_apps += 1;
    const tag = normalizeResultTag(r.result_tag);
    item[`${tag}_apps`] += 1;
    const mb = Number(r.max_allocated_mb || 0);
    item._mb.push(mb);
    acc[day] = item;
  });
  return Object.values(acc).sort((a, b) => String(a.bucket_day).localeCompare(String(b.bucket_day))).map(x => {
    const sorted = x._mb.sort((a, b) => a - b);
    const idx = Math.max(0, Math.ceil(sorted.length * 0.95) - 1);
    return { ...x, p95_max_allocated_mb: sorted[idx] || 0 };
  });
}

async function loadDailyAppsSummary() {
  try {
    const rows = await getRowsWithFallback([
      `/api/dashboard/apps/daily-summary?days=${state.days}`,
      `/api/apps/daily-summary?days=${state.days}`,
    ]);
    if (!rows.length) {
      dailyAppsChart.clear();
      return rows;
    }
    requireKeys(rows, ['bucket_day', 'total_apps', 'success_apps', 'failed_apps', 'running_apps', 'p95_max_allocated_mb'], '每日任务趋势');
    renderDailyApps(rows);
    return rows;
  } catch (err) {
    const fallbackRows = deriveDailySummaryFromApps(await getRecentAppsRowsCached());
    if (fallbackRows.length) {
      renderDailyApps(fallbackRows);
      return fallbackRows;
    }
    throw err;
  }
}

function renderDailyApps(rows) {
  const days = rows.map(d => toDateKey(d.bucket_day));
  const successRate = rows.map(d => (Number(d.total_apps || 0) > 0 ? Number(d.success_apps || 0) / Number(d.total_apps || 0) : 0));
  dailyAppsChart.setOption({
    tooltip: { trigger: 'axis' },
    legend: { type: 'scroll' },
    xAxis: [{ type: 'category', data: days }],
    yAxis: [
      { type: 'value', name: '任务数' },
      { type: 'value', name: '资源MB' },
      { type: 'value', name: '成功率', min: 0, max: 1, axisLabel: { formatter: v => `${Math.round(v * 100)}%` } },
    ],
    series: [
      { name: 'success', type: 'bar', stack: 'count', data: rows.map(d => Number(d.success_apps || 0)) },
      { name: 'failed', type: 'bar', stack: 'count', data: rows.map(d => Number(d.failed_apps || 0)) },
      { name: 'running', type: 'bar', stack: 'count', data: rows.map(d => Number(d.running_apps || 0)) },
      { name: 'P95内存', type: 'line', yAxisIndex: 1, smooth: true, data: rows.map(d => Number(d.p95_max_allocated_mb || 0)) },
      { name: '成功率', type: 'line', yAxisIndex: 2, smooth: true, data: successRate },
    ],
  });
}

function deriveQueueSummaryFromApps(rows) {
  const acc = {};
  rows.forEach(r => {
    const q = r.queue_name || '(unknown)';
    const item = acc[q] || { queue_name: q, total_apps: 0, success_apps: 0, failed_apps: 0, running_apps: 0, avg_max_allocated_mb: 0, p95_max_allocated_mb: 0, _sum: 0, _mb: [] };
    item.total_apps += 1;
    item[`${normalizeResultTag(r.result_tag)}_apps`] += 1;
    const mb = Number(r.max_allocated_mb || 0);
    item._sum += mb;
    item._mb.push(mb);
    acc[q] = item;
  });
  return Object.values(acc)
    .map(x => {
      const sorted = x._mb.sort((a, b) => a - b);
      const idx = Math.max(0, Math.ceil(sorted.length * 0.95) - 1);
      return { ...x, avg_max_allocated_mb: x.total_apps ? x._sum / x.total_apps : 0, p95_max_allocated_mb: sorted[idx] || 0 };
    })
    .sort((a, b) => b.total_apps - a.total_apps)
    .slice(0, Math.max(state.topN, 8));
}

function renderQueueAppSummary(rows) {
  const html = ['<table><thead><tr><th>queue</th><th>total</th><th>success_rate</th><th>failed</th><th>running</th><th>avg_mb</th><th>p95_mb</th></tr></thead><tbody>'];
  rows.forEach(r => {
    const rate = Number(r.total_apps || 0) > 0 ? Number(r.success_apps || 0) / Number(r.total_apps || 0) : 0;
    html.push(`<tr><td>${escapeHTML(r.queue_name)}</td><td>${Number(r.total_apps || 0)}</td><td>${toPercent(rate)}</td><td>${Number(r.failed_apps || 0)}</td><td>${Number(r.running_apps || 0)}</td><td>${Math.round(Number(r.avg_max_allocated_mb || 0))}</td><td>${Math.round(Number(r.p95_max_allocated_mb || 0))}</td></tr>`);
  });
  html.push('</tbody></table>');
  document.getElementById('queueAppSummary').innerHTML = html.join('');
}

async function loadQueueAppSummary() {
  try {
    const rows = await getRowsWithFallback([
      `/api/dashboard/apps/queue-summary?days=${Math.min(state.days, 30)}&top_n=${Math.max(state.topN, 8)}`,
      `/api/apps/by-queue?days=${Math.min(state.days, 30)}&top_n=${Math.max(state.topN, 8)}`,
    ]);
    if (!rows.length) return showEmpty('queueAppSummary', '当前窗口无队列任务汇总数据');
    requireKeys(rows, ['queue_name', 'total_apps', 'success_apps', 'failed_apps', 'running_apps', 'avg_max_allocated_mb', 'p95_max_allocated_mb'], '队列任务汇总');
    renderQueueAppSummary(rows);
    return rows;
  } catch (err) {
    const fallbackRows = deriveQueueSummaryFromApps(await getRecentAppsRowsCached());
    if (!fallbackRows.length) throw err;
    renderQueueAppSummary(fallbackRows);
    return fallbackRows;
  }
}

function renderSummaryCards(todayUsage, dailySummary, queueOverviewRows) {
  const latestCluster = todayUsage.cluster[todayUsage.cluster.length - 1];
  const latestDaily = dailySummary[dailySummary.length - 1] || { total_apps: 0, success_apps: 0, failed_apps: 0, p95_max_allocated_mb: 0 };
  const successRate = Number(latestDaily.total_apps || 0) > 0 ? Number(latestDaily.success_apps || 0) / Number(latestDaily.total_apps || 0) : 0;
  const hottestQueue = queueOverviewRows[0];
  const cards = [
    { label: '当前集群已分配', value: latestCluster ? toGB(latestCluster.allocated_mb) : '-' },
    { label: '今日任务数', value: String(latestDaily.total_apps || 0) },
    { label: '今日成功率', value: toPercent(successRate) },
    { label: '任务P95峰值内存', value: toGB(latestDaily.p95_max_allocated_mb) },
    { label: '热点队列', value: hottestQueue ? `${hottestQueue.queue_path} (${toGB(hottestQueue.peak_used_memory_mb)})` : '-' },
  ];

  document.getElementById('summaryCards').innerHTML = cards
    .map(c => `<div class="summary-item"><div class="label">${escapeHTML(c.label)}</div><div class="value">${escapeHTML(c.value)}</div></div>`)
    .join('');
}

async function loadApps() {
  const rows = await getRecentAppsRowsCached();
  if (!rows.length) return showEmpty('appTable', '暂无任务明细数据');
  requireKeys(rows, ['app_id', 'queue_name', 'app_name', 'result_tag', 'max_allocated_mb', 'max_allocated_vcores', 'start_time'], '任务明细');

  const html = ['<table><thead><tr><th>app_id</th><th>queue</th><th>name</th><th>result</th><th>max_mb</th><th>max_vcores</th><th>time</th></tr></thead><tbody>'];
  rows.slice(0, 200).forEach(a => {
    const resultTag = normalizeResultTag(a.result_tag);
    html.push(`<tr><td>${escapeHTML(a.app_id)}</td><td>${escapeHTML(a.queue_name)}</td><td>${escapeHTML(a.app_name)}</td><td class="tag-${resultTag}">${escapeHTML(a.result_tag)}</td><td>${Number(a.max_allocated_mb || 0)}</td><td>${Number(a.max_allocated_vcores || 0)}</td><td>${escapeHTML(a.start_time)}</td></tr>`);
  });
  html.push('</tbody></table>');
  document.getElementById('appTable').innerHTML = html.join('');
}

async function renderPageNav() {
  const pages = await getRowsWithFallback(['/api/meta/pages']);
  const nav = document.getElementById('pageNav');
  nav.innerHTML = pages
    .map(p => `<button class="nav-btn ${p.key === 'dashboard' ? 'active' : ''}" data-page="${escapeHTML(p.key)}">${escapeHTML(p.title)}</button>`)
    .join('');

  nav.querySelectorAll('.nav-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      const key = btn.dataset.page;
      document.querySelectorAll('[data-page-section]').forEach(sec => {
        sec.style.display = sec.dataset.pageSection === key ? 'block' : 'none';
      });
      nav.querySelectorAll('.nav-btn').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
    });
  });
}

function bindControls() {
  document.querySelectorAll('[data-period]').forEach(btn => {
    btn.addEventListener('click', () => {
      state.period = btn.dataset.period;
      refreshDashboard();
    });
  });
  document.getElementById('daysSelect').addEventListener('change', e => { state.days = Number(e.target.value); });
  document.getElementById('topNSelect').addEventListener('change', e => { state.topN = Number(e.target.value); });
  document.getElementById('queueMetricSelect').addEventListener('change', e => { state.queueMetric = e.target.value; });
  document.getElementById('refreshBtn').addEventListener('click', refreshDashboard);
}

async function refreshDashboard() {
  const btn = document.getElementById('refreshBtn');
  btn.disabled = true;
  updateRefreshStatus('刷新中...');

  const [statsRes, overviewRes, dailyRes, queueSummaryRes, appsRes] = await Promise.allSettled([
    loadQueueStats(),
    loadQueueOverview(),
    loadDailyAppsSummary(),
    loadQueueAppSummary(),
    loadApps(),
  ]);

  const topQueues = statsRes.status === 'fulfilled' ? statsRes.value.topQueues : [];
  const queueOverviewRows = overviewRes.status === 'fulfilled' ? overviewRes.value : [];
  const dailySummary = dailyRes.status === 'fulfilled' ? dailyRes.value : [];

  if (statsRes.status === 'rejected') showEmpty('queueStats', `加载失败: ${statsRes.reason.message}`);
  if (overviewRes.status === 'rejected') showEmpty('queueOverview', `加载失败: ${overviewRes.reason.message}`);
  if (dailyRes.status === 'rejected') showEmpty('dailyApps', `加载失败: ${dailyRes.reason.message}`);
  if (queueSummaryRes.status === 'rejected') showEmpty('queueAppSummary', `加载失败: ${queueSummaryRes.reason.message}`);
  if (appsRes.status === 'rejected') showEmpty('appTable', `加载失败: ${appsRes.reason.message}`);

  try {
    const todayUsage = await loadTodayUsage(topQueues);
    renderSummaryCards(todayUsage, dailySummary, queueOverviewRows);
  } catch (err) {
    showEmpty('todayUsage', `加载失败: ${err.message}`);
    showEmpty('summaryCards', `加载失败: ${err.message}`);
  }

  const failedCount = [statsRes, overviewRes, dailyRes, queueSummaryRes, appsRes].filter(x => x.status === 'rejected').length;
  updateRefreshStatus(failedCount ? `刷新完成（${failedCount}个模块失败）` : `刷新成功 ${new Date().toLocaleTimeString()}`, failedCount ? 'warn' : 'ok');
  btn.disabled = false;
}

renderPageNav();
bindControls();
refreshDashboard();

window.addEventListener('resize', () => {
  queueChart.resize();
  usageChart.resize();
  dailyAppsChart.resize();
  queueOverviewChart.resize();
});
