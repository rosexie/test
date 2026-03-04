async function getJSON(url) {
  const r = await fetch(url);
  if (!r.ok) throw new Error(await r.text());
  return r.json();
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
  )
    .sort((a, b) => b[1] - a[1])
    .slice(0, topN)
    .map(([queue]) => queue);
}

async function loadQueueStats() {
  const data = await getJSON(`/api/queue/stats?period=${state.period}`);
  const buckets = [...new Set(data.map(d => d.bucket_time.slice(0, 10)))];
  const topQueues = pickTopQueuesFromStats(data, state.topN);

  const queueByBucket = {};
  topQueues.forEach(queue => {
    queueByBucket[queue] = Array(buckets.length).fill(0);
  });

  data.forEach(d => {
    if (!queueByBucket[d.queue_path]) return;
    const idx = buckets.indexOf(d.bucket_time.slice(0, 10));
    if (idx < 0) return;
    queueByBucket[d.queue_path][idx] = d[state.queueMetric] || 0;
  });

  const metricLabel = {
    max_used_memory_mb: '峰值 Max',
    p95_used_memory_mb: 'P95',
    min_used_memory_mb: '最小值 Min',
  }[state.queueMetric];

  queueChart.setOption({
    tooltip: { trigger: 'axis' },
    legend: { type: 'scroll' },
    xAxis: { type: 'category', data: buckets },
    yAxis: { type: 'value', name: `${metricLabel} (MB)` },
    series: Object.entries(queueByBucket).map(([name, values]) => ({ type: 'line', smooth: true, name, data: values })),
  });

  return { data, topQueues };
}

async function loadQueueOverview() {
  const rows = await getJSON(`/api/queue/overview?period=${state.period}&top_n=${state.topN}`);
  const queues = rows.map(r => r.queue_path);

  queueOverviewChart.setOption({
    tooltip: { trigger: 'axis' },
    legend: { type: 'scroll' },
    xAxis: { type: 'value', name: 'MB' },
    yAxis: { type: 'category', data: queues, inverse: true },
    series: [
      { name: 'Peak', type: 'bar', data: rows.map(r => r.peak_used_memory_mb) },
      { name: 'P95', type: 'bar', data: rows.map(r => r.p95_used_memory_mb) },
      { name: 'Avg', type: 'bar', data: rows.map(r => r.avg_used_memory_mb) },
    ],
  });

  return rows;
}

async function loadTodayUsage(preferredTopQueues = []) {
  const data = await getJSON('/api/today/usage');
  const x = data.cluster.map(d => d.ts.slice(11));

  const topQueues = preferredTopQueues.length
    ? preferredTopQueues
    : Object.entries(data.queues)
      .map(([queue, points]) => [queue, points.reduce((sum, p) => sum + (p.used_memory_mb || 0), 0)])
      .sort((a, b) => b[1] - a[1])
      .slice(0, state.topN)
      .map(([queue]) => queue);

  const series = [
    { name: '集群已分配MB', type: 'line', smooth: true, lineStyle: { width: 3 }, data: data.cluster.map(d => d.allocated_mb) },
  ];
  topQueues.forEach(queue => {
    series.push({ name: queue, type: 'line', smooth: true, data: (data.queues[queue] || []).map(p => p.used_memory_mb) });
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
  const data = await getJSON(`/api/apps/daily-summary?days=${state.days}`);
  const days = data.map(d => d.bucket_day.slice(0, 10));
  const successRate = data.map(d => (d.total_apps > 0 ? d.success_apps / d.total_apps : 0));

  dailyAppsChart.setOption({
    tooltip: { trigger: 'axis' },
    legend: { type: 'scroll' },
    xAxis: [{ type: 'category', data: days }],
    yAxis: [{ type: 'value', name: '任务数' }, { type: 'value', name: '资源 MB' }, { type: 'value', name: '成功率', min: 0, max: 1, axisLabel: { formatter: value => `${Math.round(value * 100)}%` } }],
    series: [
      { name: 'success', type: 'bar', stack: 'count', data: data.map(d => d.success_apps) },
      { name: 'failed', type: 'bar', stack: 'count', data: data.map(d => d.failed_apps) },
      { name: 'running', type: 'bar', stack: 'count', data: data.map(d => d.running_apps) },
      { name: 'P95 max_allocated_mb', type: 'line', yAxisIndex: 1, smooth: true, data: data.map(d => d.p95_max_allocated_mb) },
      { name: 'success_rate', type: 'line', yAxisIndex: 2, smooth: true, data: successRate },
    ],
  });

  return data;
}

async function loadQueueAppSummary() {
  const rows = await getJSON(`/api/apps/queue-summary?days=${Math.min(state.days, 30)}&top_n=${Math.max(state.topN, 8)}`);
  const table = [
    '<table><thead><tr><th>queue</th><th>total</th><th>success</th><th>failed</th><th>running</th><th>success_rate</th><th>avg_mb</th><th>p95_mb</th></tr></thead><tbody>',
  ];

  rows.forEach(r => {
    const rate = r.total_apps > 0 ? r.success_apps / r.total_apps : 0;
    table.push(`<tr><td>${escapeHTML(r.queue_name)}</td><td>${r.total_apps}</td><td>${r.success_apps}</td><td>${r.failed_apps}</td><td>${r.running_apps}</td><td>${toPercent(rate)}</td><td>${Math.round(r.avg_max_allocated_mb)}</td><td>${Math.round(r.p95_max_allocated_mb)}</td></tr>`);
  });

  table.push('</tbody></table>');
  document.getElementById('queueAppSummary').innerHTML = table.join('');

  return rows;
}

function renderSummaryCards(todayUsage, dailySummary, queueOverviewRows) {
  const latestCluster = todayUsage.cluster[todayUsage.cluster.length - 1];
  const latestDaily = dailySummary[dailySummary.length - 1] || {
    total_apps: 0,
    success_apps: 0,
    failed_apps: 0,
    running_apps: 0,
    p95_max_allocated_mb: 0,
  };
  const successRate = latestDaily.total_apps > 0 ? latestDaily.success_apps / latestDaily.total_apps : 0;
  const hottestQueue = queueOverviewRows[0];

  const cards = [
    { label: '当前集群已分配', value: latestCluster ? toGB(latestCluster.allocated_mb) : '0 GB' },
    { label: '今日任务总数', value: String(latestDaily.total_apps || 0) },
    { label: '今日成功率', value: toPercent(successRate) },
    { label: '今日失败任务', value: String(latestDaily.failed_apps || 0) },
    { label: '任务P95峰值内存', value: toGB(latestDaily.p95_max_allocated_mb) },
    { label: '热点队列(峰值)', value: hottestQueue ? `${hottestQueue.queue_path} / ${toGB(hottestQueue.peak_used_memory_mb)}` : '-' },
  ];

  document.getElementById('summaryCards').innerHTML = cards
    .map(c => `<div class="summary-item"><div class="label">${escapeHTML(c.label)}</div><div class="value">${escapeHTML(c.value)}</div></div>`)
    .join('');
}

async function loadApps() {
  const apps = await getJSON('/api/apps/by-queue');
  const table = [
    '<table><thead><tr><th>app_id</th><th>queue</th><th>name</th><th>result</th><th>max_mb</th><th>max_vcores</th><th>time</th></tr></thead><tbody>',
  ];
  apps.slice(0, 200).forEach(a => {
    const appId = escapeHTML(a.app_id);
    const queueName = escapeHTML(a.queue_name);
    const appName = escapeHTML(a.app_name);
    const resultTag = normalizeResultTag(a.result_tag);
    const resultLabel = escapeHTML(a.result_tag);
    const startTime = escapeHTML(a.start_time);
    table.push(`<tr><td>${appId}</td><td>${queueName}</td><td>${appName}</td><td class="tag-${resultTag}">${resultLabel}</td><td>${a.max_allocated_mb}</td><td>${a.max_allocated_vcores}</td><td>${startTime}</td></tr>`);
  });
  table.push('</tbody></table>');
  document.getElementById('appTable').innerHTML = table.join('');
}

function bindControls() {
  document.querySelectorAll('[data-period]').forEach(btn => {
    btn.addEventListener('click', () => {
      state.period = btn.dataset.period;
      refreshDashboard();
    });
  });

  document.getElementById('daysSelect').addEventListener('change', event => {
    state.days = Number(event.target.value);
  });
  document.getElementById('topNSelect').addEventListener('change', event => {
    state.topN = Number(event.target.value);
  });
  document.getElementById('queueMetricSelect').addEventListener('change', event => {
    state.queueMetric = event.target.value;
  });

  document.getElementById('refreshBtn').addEventListener('click', () => {
    refreshDashboard();
  });
}

async function refreshDashboard() {
  const [{ topQueues }, queueOverviewRows, todayUsage, dailySummary] = await Promise.all([
    loadQueueStats(),
    loadQueueOverview(),
    loadTodayUsage(),
    loadDailyAppsSummary(),
    loadQueueAppSummary(),
    loadApps(),
  ]);

  if (topQueues.length > 0) {
    await loadTodayUsage(topQueues);
  }

  renderSummaryCards(todayUsage, dailySummary, queueOverviewRows);
}

bindControls();
refreshDashboard();
window.addEventListener('resize', () => {
  queueChart.resize();
  usageChart.resize();
  dailyAppsChart.resize();
  queueOverviewChart.resize();
});
