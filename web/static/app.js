async function getJSON(url) {
  const r = await fetch(url);
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}

const queueChart = echarts.init(document.getElementById('queueStats'));
const usageChart = echarts.init(document.getElementById('todayUsage'));

async function loadQueueStats(period = 'day') {
  const data = await getJSON(`/api/queue/stats?period=${period}`);
  const buckets = [...new Set(data.map(d => d.bucket_time.slice(0, 10)))];
  const queueByBucket = {};
  data.forEach(d => {
    const key = `${d.queue_path}#max`;
    queueByBucket[key] = queueByBucket[key] || Array(buckets.length).fill(0);
    queueByBucket[key][buckets.indexOf(d.bucket_time.slice(0, 10))] = d.max_used_memory_mb;
  });

  queueChart.setOption({
    tooltip: { trigger: 'axis' },
    legend: { type: 'scroll' },
    xAxis: { type: 'category', data: buckets },
    yAxis: { type: 'value', name: 'MB' },
    series: Object.entries(queueByBucket).map(([name, values]) => ({ type: 'bar', name, data: values })),
  });
}

async function loadTodayUsage() {
  const data = await getJSON('/api/today/usage');
  const x = data.cluster.map(d => d.ts.slice(11));
  const series = [
    { name: 'cluster_allocated_mb', type: 'line', smooth: true, data: data.cluster.map(d => d.allocated_mb) }
  ];
  Object.entries(data.queues).slice(0, 8).forEach(([queue, points]) => {
    series.push({ name: queue, type: 'line', smooth: true, data: points.map(p => p.used_memory_mb) });
  });

  usageChart.setOption({
    tooltip: { trigger: 'axis' },
    legend: { type: 'scroll' },
    xAxis: { type: 'category', data: x },
    yAxis: { type: 'value', name: 'MB' },
    series,
  });
}

async function loadApps() {
  const apps = await getJSON('/api/apps/by-queue');
  const table = [`<table><thead><tr><th>app_id</th><th>queue</th><th>name</th><th>result</th><th>max_mb</th><th>max_vcores</th><th>time</th></tr></thead><tbody>`];
  apps.slice(0, 200).forEach(a => {
    table.push(`<tr><td>${a.app_id}</td><td>${a.queue_name || ''}</td><td>${a.app_name || ''}</td><td class="tag-${a.result_tag}">${a.result_tag}</td><td>${a.max_allocated_mb}</td><td>${a.max_allocated_vcores}</td><td>${a.start_time || ''}</td></tr>`);
  });
  table.push('</tbody></table>');
  document.getElementById('appTable').innerHTML = table.join('');
}

document.querySelectorAll('[data-period]').forEach(btn => {
  btn.addEventListener('click', () => loadQueueStats(btn.dataset.period));
});

Promise.all([loadQueueStats(), loadTodayUsage(), loadApps()]);
window.addEventListener('resize', () => { queueChart.resize(); usageChart.resize(); });
