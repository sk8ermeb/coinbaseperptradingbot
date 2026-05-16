
let chart, candleSeries, markersPrimitive;
let chartindicators = {};
let indicatorColorMap = {};
let subPanes = [];
let candleslist = [];
let colors = ['#FF11FF','#11FFFF','#FFFF11','#AAAAFF','#AAFFAA','#FFAAAA','#FFFFFF','#AAAAAA'];
let pollTimer = null;
let pricePollTimer = null;
let idleBalanceTimer = null;
let isRunning = false;
let granularityLocked = false;
let currentProductId = '';
let _stopping = false;

// ------------------------------------------------------------------ chart init

chart = LightweightCharts.createChart(document.getElementById('chart'), {
  autoSize: true,
  layout: { background: { color: '#1a1a1a' }, textColor: '#d1d4dc' },
  grid: { vertLines: { color: '#2a2e39' }, horzLines: { color: '#2a2e39' } },
  crosshair: { mode: LightweightCharts.CrosshairMode.Normal },
  rightPriceScale: { borderColor: '#2a2e39' },
  timeScale: { borderColor: '#2a2e39', timeVisible: true, secondsVisible: false },
});

candleSeries = chart.addSeries(LightweightCharts.CandlestickSeries);
candleSeries.applyOptions({
  upColor: '#26a69a', downColor: '#ef5350',
  borderVisible: false, wickUpColor: '#26a69a', wickDownColor: '#ef5350',
});
markersPrimitive = LightweightCharts.createSeriesMarkers(candleSeries, []);
chart.timeScale().fitContent();

chart.subscribeCrosshairMove(param => {
  if (param.time === undefined) return;
  const hovered = new Date(param.time * 1000);
  for (const c of candleslist) {
    if (new Date(c.timestamp * 1000).getTime() === hovered.getTime()) {
      break;
    }
  }
});

// ------------------------------------------------------------------ script / granularity selection

async function onScriptChange() {
  if (granularityLocked) return;
  const scriptid = document.getElementById('scriptDropdown').value;
  if (scriptid < 0) return;
  localStorage.setItem('selectedScriptId', scriptid);
  try {
    const resp = await fetch(`/api/live/scriptgranularity?scriptid=${scriptid}`);
    if (!resp.ok) return;
    const data = await resp.json();
    currentProductId = data.product_id || '';
    const drop = document.getElementById('granularityDropdown');
    for (const opt of drop.options) {
      if (opt.value === data.granularity) { opt.selected = true; break; }
    }
    document.getElementById('acc_gran').textContent =
      document.getElementById('granularityDropdown').value || '—';
    document.getElementById('acc_pair').textContent = currentProductId || '—';
    if (!isRunning) await refreshAccountForProduct(currentProductId);
    loadCandles();
  } catch(e) {}
}

function onGranularityChange() {
  if (isRunning) return;
  document.getElementById('acc_gran').textContent =
    document.getElementById('granularityDropdown').value || '—';
  loadCandles();
}

// ------------------------------------------------------------------ start / stop

async function startTrading() {
  const scriptid = document.getElementById('scriptDropdown').value;
  if (scriptid < 0) { showMessage("Select a script first"); return; }

  const resp = await fetch('/api/live/start', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ scriptid: parseInt(scriptid) }),
  });
  if (resp.ok) {
    _stopping = false;
    lastTickTime = null;
    setRunningUI(true);
    loadCandles();
    startPolling();
  } else {
    const err = await resp.json();
    showMessage(err.detail || 'Failed to start');
  }
}

async function stopTrading() {
  _stopping = true;
  setRunningUI(false);
  stopPolling();
  try {
    await fetch('/api/live/stop', { method: 'POST' });
  } finally {
    _stopping = false;
  }
}

function setRunningUI(running) {
  isRunning = running;
  granularityLocked = running;
  document.getElementById('bstart').classList.toggle('d-none', running);
  document.getElementById('bstop').classList.toggle('d-none', !running);
  document.getElementById('scriptDropdown').disabled = running;
  document.getElementById('granularityDropdown').disabled = running;
  const badge = document.getElementById('statusbadge');
  badge.textContent = running ? 'Running' : 'Stopped';
  badge.className = 'badge ' + (running ? 'bg-success' : 'bg-secondary');
}

// ------------------------------------------------------------------ polling

let _fastPollCount = 0;

function startPolling() {
  if (pollTimer) clearInterval(pollTimer);
  if (pricePollTimer) clearInterval(pricePollTimer);
  if (idleBalanceTimer) { clearInterval(idleBalanceTimer); idleBalanceTimer = null; }

  // Poll every 3s for the first 30s so account data appears as soon as the
  // backend finishes reading balance/position (before history load completes).
  _fastPollCount = 0;
  pollStatus();
  pollTimer = setInterval(() => {
    pollStatus();
    _fastPollCount++;
    if (_fastPollCount >= 10) {
      clearInterval(pollTimer);
      pollTimer = setInterval(pollStatus, 15000);
    }
  }, 3000);

  pricePollTimer = setInterval(pollPrice, 5000);
}

function stopPolling() {
  if (pollTimer) { clearInterval(pollTimer); pollTimer = null; }
  if (pricePollTimer) { clearInterval(pricePollTimer); pricePollTimer = null; }
  // Resume idle account polling now that the bot isn't running.
  if (idleBalanceTimer) clearInterval(idleBalanceTimer);
  refreshAccountForProduct(currentProductId);
  idleBalanceTimer = setInterval(() => refreshAccountForProduct(currentProductId), 15000);
}

async function pollStatus() {
  try {
    const resp = await fetch('/api/live/status');
    if (!resp.ok) return;
    const data = await resp.json();
    updateAccountPanel(data);
    if (data.running !== isRunning && !(data.running && _stopping)) {
      setRunningUI(data.running);
    }
    document.getElementById('lastupdate').textContent =
      'Updated ' + new Date().toLocaleTimeString();

    if (data.running && data.last_tick_time && data.last_tick_time !== lastTickTime) {
      lastTickTime = data.last_tick_time;
      loadCandles(false);
    }
  } catch(e) {}
}

async function refreshAccountForProduct(productId) {
  // Pull a fresh snapshot for a specific product directly from Coinbase.
  // Used on page load, on script change, and on the idle polling timer.
  if (isRunning) return;
  if (!productId) return;
  try {
    const resp = await fetch('/api/live/account?product_id=' + encodeURIComponent(productId));
    if (!resp.ok) return;
    const data = await resp.json();
    const baseSym = (data.base_currency || '').toUpperCase();
    document.getElementById('acc_usd').textContent    = '$' + fmt(data.usd);
    document.getElementById('acc_equity').textContent = '$' + fmt(data.total_equity);
    const pos = data.realposition;
    const posEl = document.getElementById('acc_pos');
    posEl.textContent = fmt(pos, 6);
    posEl.style.color = pos > 0 ? '#26a69a' : pos < 0 ? '#ef5350' : '';
    document.getElementById('acc_cb').textContent    = pos ? '$' + fmt(data.costbasis) : '—';
    document.getElementById('acc_price').textContent = data.mark_price ? '$' + fmt(data.mark_price) : '—';
    const pnlEl = document.getElementById('acc_upnl');
    pnlEl.textContent = (data.unrealized_pnl >= 0 ? '+' : '') + '$' + fmt(data.unrealized_pnl);
    pnlEl.style.color = data.unrealized_pnl >= 0 ? '#26a69a' : '#ef5350';
    if (data.max_leverage) {
      document.getElementById('acc_lev').textContent = data.max_leverage + 'x';
    }
    document.getElementById('acc_contract_size').textContent =
      data.contract_size != null ? data.contract_size + (baseSym ? ' ' + baseSym : '') : '—';
    document.getElementById('acc_pair').textContent = data.product_id || productId;
    document.getElementById('acc_base').textContent = baseSym || '—';
    document.getElementById('lastupdate').textContent =
      'Account updated ' + new Date().toLocaleTimeString();
  } catch(e) {}
}

async function pollPrice() {
  try {
    const resp = await fetch('/api/live/price');
    if (!resp.ok) return;
    const data = await resp.json();
    if (data.price) {
      document.getElementById('acc_price').textContent = '$' + fmt(data.price);
      document.getElementById('lastupdate').textContent =
        'Price updated ' + new Date().toLocaleTimeString();
    }
  } catch(e) {}
}

// ------------------------------------------------------------------ account panel

function fmt(n, dec=2) {
  if (n === undefined || n === null) return '—';
  return parseFloat(n).toFixed(dec);
}

function updateAccountPanel(data) {
  document.getElementById('acc_usd').textContent    = '$' + fmt(data.usd);
  document.getElementById('acc_equity').textContent = '$' + fmt(data.total_equity);
  const pos = data.realposition;
  const posEl = document.getElementById('acc_pos');
  posEl.textContent = fmt(pos, 6);
  posEl.style.color = pos > 0 ? '#26a69a' : pos < 0 ? '#ef5350' : '';
  document.getElementById('acc_cb').textContent    = pos ? '$' + fmt(data.costbasis) : '—';
  document.getElementById('acc_price').textContent = '$' + fmt(data.close);
  const pnlEl = document.getElementById('acc_upnl');
  pnlEl.textContent = (data.unrealized_pnl >= 0 ? '+' : '') + '$' + fmt(data.unrealized_pnl);
  pnlEl.style.color = data.unrealized_pnl >= 0 ? '#26a69a' : '#ef5350';
  document.getElementById('acc_lev').textContent  = data.leverage + 'x';
  // data.pair holds the trading Product_ID from the script (e.g.
  // BIP-20DEC30-CDE). Base symbol comes from the account snapshot, not
  // the product_id string (which has no reliable base prefix for CDE).
  const productId = data.pair || '—';
  const baseSym = (data.base_currency || '').toUpperCase();
  document.getElementById('acc_contract_size').textContent =
    data.contract_size != null ? data.contract_size + (baseSym ? ' ' + baseSym : '') : '—';
  document.getElementById('acc_pair').textContent = productId;
  document.getElementById('acc_base').textContent = baseSym || '—';
  document.getElementById('acc_gran').textContent = data.granularity || '—';

  if (data.log && data.log.length) {
    const logEl = document.getElementById('livelog');
    logEl.textContent = data.log.join('\n');
    logEl.scrollTop = logEl.scrollHeight;
  }
}

// ------------------------------------------------------------------ chart

let lastTickTime = null;

async function loadCandles(fitView = true) {
  try {
    const gran = document.getElementById('granularityDropdown').value;
    let params = '';
    if (!isRunning) {
      if (!currentProductId) return;  // no script selected → nothing to chart
      params = `?product_id=${encodeURIComponent(currentProductId)}&granularity=${encodeURIComponent(gran)}`;
    }
    const resp = await fetch('/api/live/candles' + params);
    if (!resp.ok) return;
    const data = await resp.json();
    setChartCandles(data.candles || []);
    setChartIndicators(data.indicators || {});
    if (fitView) chart.timeScale().fitContent();
  } catch(e) {}
}

function setChartCandles(candles) {
  candleslist = candles;
  const bars = candles.map(c => ({
    time: c.timestamp, open: c.open, high: c.high, low: c.low, close: c.close,
  }));
  candleSeries.setData(bars);
}

function setChartIndicators(indicators) {
  for (const name in chartindicators) {
    chart.removeSeries(chartindicators[name]);
    delete chartindicators[name];
  }
  for (const pane of subPanes) {
    try { chart.removePane(pane.paneIndex()); } catch(e) {}
  }
  subPanes = [];
  indicatorColorMap = {};
  document.getElementById('indicator-legend').innerHTML = '';

  const lastClose = candleslist.length > 0 ? candleslist[candleslist.length - 1].close : 0;
  const chartTimes = new Set(candleslist.map(c => c.timestamp));
  let subPane = null;
  let j = 0;

  for (const name in indicators) {
    const color = colors[j % colors.length];
    const data = (indicators[name] || []).filter(e =>
      e.time != null && chartTimes.has(e.time) &&
      e.value !== null && e.value !== undefined && !isNaN(e.value)
    );

    let targetPaneIndex = undefined;
    if (lastClose > 0 && data.length > 0) {
      const vals = data.map(d => Math.abs(d.value)).filter(v => isFinite(v));
      if (vals.length > 0 && Math.max(...vals) < lastClose * 0.05) {
        if (!subPane) {
          subPane = chart.addPane();
          subPanes.push(subPane);
        }
        targetPaneIndex = subPane.paneIndex();
      }
    }

    const series = chart.addSeries(LightweightCharts.LineSeries, {
      color: color, lineWidth: 2,
      priceScaleId: 'right', title: name,
    }, targetPaneIndex);
    series.setData(data);
    chartindicators[name] = series;
    indicatorColorMap[name] = color;
    j++;
  }
  renderIndicatorLegend();
}

function renderIndicatorLegend() {
  const legend = document.getElementById('indicator-legend');
  legend.innerHTML = '';
  for (const name in chartindicators) {
    const color = indicatorColorMap[name] || '#ffffff';
    const label = document.createElement('label');
    label.style.cssText = 'display:flex;align-items:center;gap:4px;cursor:pointer;font-size:0.78rem;white-space:nowrap;';
    const cb = document.createElement('input');
    cb.type = 'checkbox';
    cb.checked = true;
    cb.onchange = () => toggleIndicator(name, cb);
    const swatch = document.createElement('span');
    swatch.style.cssText = `display:inline-block;width:10px;height:10px;background:${color};border-radius:2px;flex-shrink:0;`;
    label.appendChild(cb);
    label.appendChild(swatch);
    label.appendChild(document.createTextNode(name));
    legend.appendChild(label);
  }
}

function toggleIndicator(name, checkbox) {
  const series = chartindicators[name];
  if (series) {
    series.applyOptions({ visible: checkbox.checked });
    chart.timeScale().fitContent();
  }
}

// ------------------------------------------------------------------ helpers

function fmtUtc(unixSecs) {
  const d = new Date(unixSecs * 1000);
  return d.toISOString().replace('T', ' ').replace(/\.\d+Z$/, ' UTC');
}

// ------------------------------------------------------------------ history tab

let historyPage = 0;

document.querySelector('[data-bs-target="#historytab"]').addEventListener('shown.bs.tab', () => loadHistory(0));

async function loadHistory(page = 0) {
  historyPage = page;
  try {
    const resp = await fetch(`/api/live/history?page=${page}`);
    if (!resp.ok) return;
    const data = await resp.json();
    renderOrders(data.orders || []);
    renderEvents(data.events || [], page);
  } catch(e) {}
}

function renderOrders(orders) {
  const tbody = document.getElementById('orderbody');
  tbody.innerHTML = '';
  for (const o of orders) {
    const tr = document.createElement('tr');
    const dt = fmtUtc(o.time);
    tr.innerHTML = `<td>${dt}</td><td>${o.tradetype}</td><td>${(o.amount||0).toFixed(4)}</td>` +
                   `<td>${o.limitprice||'—'}</td><td>${o.stopprice||'—'}</td><td>${o.status}</td>`;
    tbody.appendChild(tr);
  }
}

function renderEvents(events, page) {
  const tbody = document.getElementById('eventbody');
  tbody.innerHTML = '';
  for (const e of events) {
    const tr = document.createElement('tr');
    tr.style.cursor = 'pointer';
    tr.title = 'Click to view tick detail';
    const dt = fmtUtc(e.time);
    let details = '';
    try {
      const d = JSON.parse(e.eventdata || '{}');
      details = Object.entries(d)
        .filter(([k]) => k !== 'time')
        .map(([k,v]) => `${k}:${typeof v==='number'?v.toFixed(4):v}`)
        .join(' | ');
    } catch(_) {}
    tr.innerHTML = `<td>${dt}</td><td>${e.eventtype}</td><td class="small text-muted">${details}</td>`;
    tr.dataset.eventId = e.id;
    tbody.appendChild(tr);
  }

  const paginationEl = document.getElementById('eventpagination');
  paginationEl.innerHTML = '';
  if (page > 0 || events.length === 300) {
    if (events.length === 300) {
      const btn = document.createElement('button');
      btn.className = 'btn btn-sm btn-outline-secondary me-2';
      btn.textContent = '← Older';
      btn.onclick = () => loadHistory(page + 1);
      paginationEl.appendChild(btn);
    }
    if (page > 0) {
      const btn = document.createElement('button');
      btn.className = 'btn btn-sm btn-outline-secondary';
      btn.textContent = 'Newer →';
      btn.onclick = () => loadHistory(page - 1);
      paginationEl.appendChild(btn);
    }
  }
}

// ------------------------------------------------------------------ tick detail modal

document.getElementById('eventbody').addEventListener('click', function(evt) {
  const tr = evt.target.closest('tr[data-event-id]');
  if (tr) openTickDetail(parseInt(tr.dataset.eventId));
});

async function openTickDetail(eventId) {
  console.log('[tick detail] opening for event', eventId);
  const contentEl = document.getElementById('tickDetailContent');
  if (!contentEl) { console.error('[tick detail] modal content element not found'); return; }
  contentEl.innerHTML = '<div class="text-center py-4"><div class="spinner-border spinner-border-sm" role="status"></div> Loading…</div>';
  try {
    const modal = bootstrap.Modal.getOrCreateInstance(document.getElementById('tickDetailModal'));
    modal.show();
  } catch(err) { console.error('[tick detail] modal error', err); return; }

  try {
    const resp = await fetch(`/api/live/tick_detail?event_id=${eventId}`);
    if (!resp.ok) { contentEl.innerHTML = '<p class="text-danger">Failed to load tick detail.</p>'; return; }
    const data = await resp.json();

    let html = '';

    // Events section
    html += '<h6 class="fw-bold mb-2">Events</h6>';
    if (data.events && data.events.length) {
      html += '<table class="table table-sm table-bordered table-striped mb-3"><thead><tr><th>Time</th><th>Type</th><th>Data</th></tr></thead><tbody>';
      for (const e of data.events) {
        const dt = fmtUtc(e.time);
        let dataStr = '';
        try {
          const d = JSON.parse(e.eventdata || '{}');
          dataStr = Object.entries(d)
            .map(([k,v]) => `<span class="text-muted">${k}:</span> ${typeof v==='number'?v.toFixed(6):v}`)
            .join('<br>');
        } catch(_) { dataStr = e.eventdata || ''; }
        html += `<tr><td class="text-nowrap small">${dt}</td><td class="text-nowrap">${e.eventtype}</td><td class="small">${dataStr}</td></tr>`;
      }
      html += '</tbody></table>';
    } else {
      html += '<p class="text-muted small">No events found.</p>';
    }

    // Simlog section
    html += '<h6 class="fw-bold mb-2">Simlog</h6>';
    if (data.simlog && data.simlog.length) {
      html += `<pre class="bg-dark text-light p-2 rounded small" style="white-space:pre-wrap;word-break:break-word;">${data.simlog.map(l => escHtml(l)).join('\n')}</pre>`;
    } else {
      html += '<p class="text-muted small">No log lines found for this tick.</p>';
    }

    contentEl.innerHTML = html;
  } catch(e) {
    contentEl.innerHTML = '<p class="text-danger">Error loading tick detail.</p>';
  }
}

function escHtml(s) {
  return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}

// ------------------------------------------------------------------ log modal

function openLogModal() {
  bootstrap.Modal.getOrCreateInstance(document.getElementById('logModal')).show();
  // Defer scroll until the modal is laid out
  setTimeout(() => {
    const el = document.getElementById('livelog');
    if (el) el.scrollTop = el.scrollHeight;
  }, 50);
}

// ------------------------------------------------------------------ open orders modal

let openOrdersTimer = null;

async function refreshOpenOrdersCount() {
  try {
    const resp = await fetch('/api/live/open_orders');
    if (!resp.ok) return;
    const data = await resp.json();
    const badge = document.getElementById('openOrdersCount');
    if (!badge) return;
    if (data.error) {
      badge.textContent = '!';
      badge.className = 'badge bg-danger ms-1';
      badge.title = data.error;
    } else {
      const n = data.count || 0;
      badge.textContent = n;
      badge.className = 'badge ms-1 ' + (n > 0 ? 'bg-warning text-dark' : 'bg-secondary');
      badge.title = '';
    }
  } catch(e) {}
}

function openOpenOrdersModal() {
  bootstrap.Modal.getOrCreateInstance(document.getElementById('openOrdersModal')).show();
  refreshOpenOrdersModal();
}

async function refreshOpenOrdersModal() {
  const contentEl = document.getElementById('openOrdersContent');
  contentEl.innerHTML = '<div class="text-center py-4"><div class="spinner-border spinner-border-sm" role="status"></div> Loading…</div>';
  try {
    const resp = await fetch('/api/live/open_orders');
    const data = await resp.json();
    if (data.error) {
      contentEl.innerHTML = `<div class="alert alert-danger small mb-0">Error from Coinbase: ${escHtml(data.error)}</div>`;
      return;
    }
    const orders = data.orders || [];
    // Refresh badge to keep them in sync
    const badge = document.getElementById('openOrdersCount');
    if (badge) {
      badge.textContent = orders.length;
      badge.className = 'badge ms-1 ' + (orders.length > 0 ? 'bg-warning text-dark' : 'bg-secondary');
    }
    if (orders.length === 0) {
      contentEl.innerHTML = '<p class="text-muted mb-0">No open orders on the exchange.</p>';
      return;
    }
    let html = '<table class="table table-sm table-bordered table-striped mb-0"><thead><tr>' +
      '<th>Created</th><th>Product</th><th>Side</th><th>Config</th>' +
      '<th>Size</th><th>Limit</th><th>Stop</th><th>Status</th><th class="small">Order ID</th>' +
      '</tr></thead><tbody>';
    for (const o of orders) {
      const cfg = o.order_configuration || {};
      const cfgKey = Object.keys(cfg)[0] || '';
      const cfgVal = cfg[cfgKey] || {};
      const limit = cfgVal.limit_price || '—';
      const stop = cfgVal.stop_price || '—';
      const size = cfgVal.base_size || cfgVal.quote_size || '—';
      let created = '—';
      if (o.created_time) {
        const t = Date.parse(o.created_time);
        if (!isNaN(t)) created = fmtUtc(Math.floor(t / 1000));
      }
      html += `<tr><td class="small text-nowrap">${escHtml(created)}</td>` +
        `<td>${escHtml(o.product_id || '')}</td>` +
        `<td>${escHtml(o.side || '')}</td>` +
        `<td class="small">${escHtml(cfgKey)}</td>` +
        `<td>${escHtml(String(size))}</td>` +
        `<td>${escHtml(String(limit))}</td>` +
        `<td>${escHtml(String(stop))}</td>` +
        `<td>${escHtml(o.status || '')}</td>` +
        `<td class="small font-monospace">${escHtml(o.order_id || '')}</td></tr>`;
    }
    html += '</tbody></table>';
    contentEl.innerHTML = html;
  } catch(e) {
    contentEl.innerHTML = `<div class="alert alert-danger small mb-0">Failed to load: ${escHtml(String(e))}</div>`;
  }
}

// ------------------------------------------------------------------ init on load

(async () => {
  // Restore last selected script from localStorage first
  const savedId = localStorage.getItem('selectedScriptId');
  if (savedId) {
    const drop = document.getElementById('scriptDropdown');
    for (const opt of drop.options) {
      if (opt.value === savedId) { opt.selected = true; break; }
    }
  }

  try {
    const resp = await fetch('/api/live/status');
    if (resp.ok) {
      const data = await resp.json();
      updateAccountPanel(data);
      if (data.running) {
        setRunningUI(true);
        startPolling();
        // Running script overrides localStorage selection
        if (data.scriptid) {
          const drop = document.getElementById('scriptDropdown');
          for (const opt of drop.options) {
            if (parseInt(opt.value) === data.scriptid) { opt.selected = true; break; }
          }
        }
        if (data.pair) currentProductId = data.pair;
        if (data.granularity) {
          const gdrop = document.getElementById('granularityDropdown');
          for (const opt of gdrop.options) {
            if (opt.value === data.granularity) { opt.selected = true; break; }
          }
        }
      }
    }
  } catch(e) {}

  // Fetch product_id/granularity from selected script, then load the
  // per-product account snapshot + chart.
  await onScriptChange();
  if (!isRunning && currentProductId) {
    await refreshAccountForProduct(currentProductId);
    if (idleBalanceTimer) clearInterval(idleBalanceTimer);
    idleBalanceTimer = setInterval(() => refreshAccountForProduct(currentProductId), 15000);
    loadCandles();
  }

  // Open-orders badge: refresh on load and every 30s
  refreshOpenOrdersCount();
  if (openOrdersTimer) clearInterval(openOrdersTimer);
  openOrdersTimer = setInterval(refreshOpenOrdersCount, 30000);
})();
