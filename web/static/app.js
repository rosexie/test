async function getJSON(url) {
  const r = await fetch(url);
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}

async function getJSONWithFallback(urls) {
  let lastError = null;
  for (const url of urls) {
    try {
      return await getJSON(url);
    } catch (err) {
      lastError = err;
    }
  }
  throw lastError || new Error('no available endpoint');
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
  return ['success', 'failed', 'running'].includes(tag) ? tag : 'running';
}

function showEmpty(id, text = '暂无数据') {
  document.getElementById(id).innerHTML = `<div class="empty">${escapeHTML(text)}</div>`;
}

const state = {
  period: 'day',
  days: 14,
  topN: 8,
  queueMetric: 'max_used_memory_mb',
};

const queueChart = echarts.init(document.getElementById('queueStats'));
const usageChart = echarts.init(document.getElementById('todayUsage'));
const dailyAppsChart = echarts.init(document.getElementById('dailyApps'));
const queueOverviewChart = echarts.init(document.getElementById('queueOverview'));

function pickTopQueuesFromStats(data, topN) {
  return Object.entries(
    data.reduce((acc, row) => {
      acc[row.queue_path] = Math.max(acc[row.queue_path] || 0, row.max_used_memory_mb || 0);
      return acc;
    }, {})
  ).sort((a, b) => b[1] - a[1]).slice(0, topN).map(([queue]) => queue);
}

async function loadQueueStats() {
  const data = await getJSONWithFallback([
    `/api/dashboard/queue/stats?period=${state.period}`,
    `/api/queue/stats?period=${state.period}`,
  ]);
  if (!data.length) {
    queueChart.clear();
    return { topQueues: [] };
  }

  const buckets = [...new Set(data.map(d => d.bucket_time.slice(0, 10)))];
  const topQueues = pickTopQueuesFromStats(data, state.topN);
  const queueByBucket = {};
  topQueues.forEach(q => { queueByBucket[q] = Array(buckets.length).fill(0); });

  data.forEach(d => {
    if (!queueByBucket[d.queue_path]) return;
    const idx = buckets.indexOf(d.bucket_time.slice(0, 10));
    if (idx >= 0) queueByBucket[d.queue_path][idx] = d[state.queueMetric] || 0;
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

async function loadQueueOverview() {
  const rows = await getJSONWithFallback([
    `/api/dashboard/queue/overview?period=${state.period}&top_n=${state.topN}`,
    `/api/queue/overview?period=${state.period}&top_n=${state.topN}`,
  ]);
  if (!rows.length) {
    queueOverviewChart.clear();
    return [];
  }

  queueOverviewChart.setOption({
    tooltip: { trigger: 'axis' },
    legend: { type: 'scroll' },
    xAxis: { type: 'value', name: 'MB' },
    yAxis: { type: 'category', inverse: true, data: rows.map(r => r.queue_path) },
    series: [
      { name: '峰值', type: 'bar', data: rows.map(r => r.peak_used_memory_mb) },
      { name: 'P95', type: 'bar', data: rows.map(r => r.p95_used_memory_mb) },
      { name: '平均', type: 'bar', data: rows.map(r => r.avg_used_memory_mb) },
    ],
  });
  return rows;
}

async function loadTodayUsage(preferredTopQueues = []) {
  const data = await getJSONWithFallback(['/api/dashboard/usage/today', '/api/today/usage']);
  if (!data.cluster.length) {
    usageChart.clear();
    return data;
  }

  const x = data.cluster.map(d => d.ts.slice(11));
  const topQueues = preferredTopQueues.length ? preferredTopQueues : Object.keys(data.queues).slice(0, state.topN);
  const series = [{ name: '集群已分配MB', type: 'line', smooth: true, lineStyle: { width: 3 }, data: data.cluster.map(d => d.allocated_mb) }];
  topQueues.forEach(q => {
    series.push({ name: q, type: 'line', smooth: true, data: (data.queues[q] || []).map(p => p.used_memory_mb) });
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

async function loadDailyAppsSummary() {
  const rows = await getJSONWithFallback([
    `/api/dashboard/apps/daily-summary?days=${state.days}`,
    `/api/apps/daily-summary?days=${state.days}`,
  ]);
  if (!rows.length) {
    dailyAppsChart.clear();
    return rows;
  }

  const days = rows.map(d => d.bucket_day.slice(0, 10));
  const successRate = rows.map(d => (d.total_apps > 0 ? d.success_apps / d.total_apps : 0));
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
      { name: 'success', type: 'bar', stack: 'count', data: rows.map(d => d.success_apps) },
      { name: 'failed', type: 'bar', stack: 'count', data: rows.map(d => d.failed_apps) },
      { name: 'running', type: 'bar', stack: 'count', data: rows.map(d => d.running_apps) },
      { name: 'P95内存', type: 'line', yAxisIndex: 1, smooth: true, data: rows.map(d => d.p95_max_allocated_mb) },
      { name: '成功率', type: 'line', yAxisIndex: 2, smooth: true, data: successRate },
    ],
  });
  return rows;
}

async function loadQueueAppSummary() {
  const rows = await getJSONWithFallback([
    `/api/dashboard/apps/queue-summary?days=${Math.min(state.days, 30)}&top_n=${Math.max(state.topN, 8)}`,
    '/api/apps/by-queue',
  ]);
  if (!rows.length) return showEmpty('queueAppSummary', '当前窗口无队列任务汇总数据');

  const html = ['<table><thead><tr><th>queue</th><th>total</th><th>success_rate</th><th>failed</th><th>running</th><th>avg_mb</th><th>p95_mb</th></tr></thead><tbody>'];
  rows.forEach(r => {
    const rate = r.total_apps > 0 ? r.success_apps / r.total_apps : 0;
    html.push(`<tr><td>${escapeHTML(r.queue_name)}</td><td>${r.total_apps}</td><td>${toPercent(rate)}</td><td>${r.failed_apps}</td><td>${r.running_apps}</td><td>${Math.round(r.avg_max_allocated_mb)}</td><td>${Math.round(r.p95_max_allocated_mb)}</td></tr>`);
  });
  html.push('</tbody></table>');
  document.getElementById('queueAppSummary').innerHTML = html.join('');
}

function renderSummaryCards(todayUsage, dailySummary, queueOverviewRows) {
  const latestCluster = todayUsage.cluster[todayUsage.cluster.length - 1];
  const latestDaily = dailySummary[dailySummary.length - 1] || { total_apps: 0, success_apps: 0, failed_apps: 0, p95_max_allocated_mb: 0 };
  const successRate = latestDaily.total_apps > 0 ? latestDaily.success_apps / latestDaily.total_apps : 0;
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
  const rows = await getJSONWithFallback(['/api/dashboard/apps/recent', '/api/apps/by-queue']);
  if (!rows.length) return showEmpty('appTable', '暂无任务明细数据');

  const html = ['<table><thead><tr><th>app_id</th><th>queue</th><th>name</th><th>result</th><th>max_mb</th><th>max_vcores</th><th>time</th></tr></thead><tbody>'];
  rows.slice(0, 200).forEach(a => {
    const resultTag = normalizeResultTag(a.result_tag);
    html.push(`<tr><td>${escapeHTML(a.app_id)}</td><td>${escapeHTML(a.queue_name)}</td><td>${escapeHTML(a.app_name)}</td><td class="tag-${resultTag}">${escapeHTML(a.result_tag)}</td><td>${a.max_allocated_mb}</td><td>${a.max_allocated_vcores}</td><td>${escapeHTML(a.start_time)}</td></tr>`);
  });
  html.push('</tbody></table>');
  document.getElementById('appTable').innerHTML = html.join('');
}

async function renderPageNav() {
  const pages = await getJSON('/api/meta/pages');
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
