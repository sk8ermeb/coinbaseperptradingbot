
let chart, candleSeries, markersPrimitive;
let chartindicators = {};
let indicatorColorMap = {};
let indicatorMeta = {};
let subPane = null;
let candleslist = [];
let eventlist = [];
let orderPriceLines = [];
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
    refreshOpenOrdersCount();
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

  pricePollTimer = setInterval(() => { pollPrice(); pollBalance(); }, 5000);
  pollBalance();
}

function stopPolling() {
  if (pollTimer) { clearInterval(pollTimer); pollTimer = null; }
  if (pricePollTimer) { clearInterval(pricePollTimer); pricePollTimer = null; }
  // Resume idle account polling now that the bot isn't running.
  if (idleBalanceTimer) clearInterval(idleBalanceTimer);
  refreshAccountForProduct(currentProductId);
  idleBalanceTimer = setInterval(() => { refreshAccountForProduct(currentProductId); pollBalance(); }, 15000);
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
    document.getElementById('acc_initial_margin').textContent =
      data.initial_margin != null ? '$' + fmt(data.initial_margin) : '—';
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
    document.getElementById('acc_price_tick').textContent =
      data.price_increment != null ? '$' + data.price_increment : '—';
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

// PnL/equity/margin move with mark price. /api/live/status only refreshes
// every ~15s and only reflects what the trader cached at the last candle
// close (e.g. once an hour on ONE_HOUR), so we poll /api/live/balance
// (one cheap Coinbase call) directly on a faster cadence.
async function pollBalance() {
  try {
    const resp = await fetch('/api/live/balance');
    if (!resp.ok) return;
    const data = await resp.json();
    if (data.usd != null)
      document.getElementById('acc_usd').textContent = '$' + fmt(data.usd);
    if (data.total_equity != null)
      document.getElementById('acc_equity').textContent = '$' + fmt(data.total_equity);
    if (data.initial_margin != null)
      document.getElementById('acc_initial_margin').textContent = '$' + fmt(data.initial_margin);
    if (data.unrealized_pnl != null) {
      const pnlEl = document.getElementById('acc_upnl');
      pnlEl.textContent = (data.unrealized_pnl >= 0 ? '+' : '') + '$' + fmt(data.unrealized_pnl);
      pnlEl.style.color = data.unrealized_pnl >= 0 ? '#26a69a' : '#ef5350';
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
  document.getElementById('acc_initial_margin').textContent =
    data.initial_margin != null ? '$' + fmt(data.initial_margin) : '—';
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
  document.getElementById('acc_price_tick').textContent =
    data.price_increment != null ? '$' + data.price_increment : '—';
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
    const sid  = document.getElementById('scriptDropdown').value;
    // Always pass the selected scriptid so the event markers track the
    // script the user is viewing, not whichever one last called start().
    const sidParam = (sid && parseInt(sid) >= 0) ? `scriptid=${encodeURIComponent(sid)}` : '';
    let params = '';
    if (!isRunning) {
      if (!currentProductId) return;  // no script selected → nothing to chart
      params = `?product_id=${encodeURIComponent(currentProductId)}&granularity=${encodeURIComponent(gran)}`;
      if (sidParam) params += '&' + sidParam;
    } else if (sidParam) {
      params = '?' + sidParam;
    }
    const resp = await fetch('/api/live/candles' + params);
    if (!resp.ok) return;
    const data = await resp.json();
    setChartCandles(data.candles || []);
    setChartIndicators(data.indicators || {});
    eventlist = data.events || [];
    applyEventFilters();
    if (fitView) chart.timeScale().fitContent();
  } catch(e) {}
}

function eventCategory(eventtype) {
  if (eventtype.startsWith('user'))   return 'user';
  if (eventtype.startsWith('fill'))   return 'fill';
  if (eventtype.startsWith('create')) return 'create';
  if (eventtype.startsWith('cancel')) return 'cancel';
  return 'other';
}

function applyEventFilters() {
  if (!markersPrimitive) return;
  const showUser   = document.getElementById('chkUser').checked;
  const showCreate = document.getElementById('chkCreate').checked;
  const showFill   = document.getElementById('chkFill').checked;
  const showCancel = document.getElementById('chkCancel').checked;
  const chartTimes = new Set(candleslist.map(c => c.timestamp));
  const markers = [];
  for (const ev of eventlist) {
    if (!chartTimes.has(ev.time)) continue;  // candle not visible → skip
    const cat = eventCategory(ev.eventtype);
    if (cat === 'user'   && !showUser)   continue;
    if (cat === 'fill'   && !showFill)   continue;
    if (cat === 'create' && !showCreate) continue;
    if (cat === 'cancel' && !showCancel) continue;
    let color = '';
    if      (cat === 'user')   color = colors[3];
    else if (cat === 'fill')   color = colors[4];
    else if (cat === 'create') color = colors[5];
    else if (cat === 'cancel') color = colors[7];
    else                       color = colors[6];
    let shape = 'square';
    if      (ev.eventtype.includes('Buy'))         shape = 'arrowUp';
    else if (ev.eventtype.includes('Sell'))        shape = 'arrowDown';
    else if (ev.eventtype.includes('Liquidation')) shape = 'circle';
    else if (cat === 'cancel')                     shape = 'circle';
    markers.push({time: ev.time, position: 'aboveBar', color: color, shape: shape, text: ev.eventtype});
  }
  markersPrimitive.setMarkers(markers);
}

function onEventFilterChange() {
  localStorage.setItem('liveEventFilters', JSON.stringify({
    user:   document.getElementById('chkUser').checked,
    create: document.getElementById('chkCreate').checked,
    fill:   document.getElementById('chkFill').checked,
    cancel: document.getElementById('chkCancel').checked,
  }));
  applyEventFilters();
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
  if (subPane) {
    try { chart.removePane(subPane.paneIndex()); } catch(e) {}
    subPane = null;
  }
  indicatorColorMap = {};
  indicatorMeta = {};
  document.getElementById('indicator-legend').innerHTML = '';

  const lastClose = candleslist.length > 0 ? candleslist[candleslist.length - 1].close : 0;
  const chartTimes = new Set(candleslist.map(c => c.timestamp));
  let j = 0;

  for (const name in indicators) {
    const color = colors[j % colors.length];
    const data = (indicators[name] || []).filter(e =>
      e.time != null && chartTimes.has(e.time) &&
      e.value !== null && e.value !== undefined && !isNaN(e.value)
    );

    let onSubPane = false;
    let targetPaneIndex = undefined;
    if (lastClose > 0 && data.length > 0) {
      const vals = data.map(d => Math.abs(d.value)).filter(v => isFinite(v));
      if (vals.length > 0 && Math.max(...vals) < lastClose * 0.05) {
        if (!subPane) subPane = chart.addPane();
        targetPaneIndex = subPane.paneIndex();
        onSubPane = true;
      }
    }

    const series = chart.addSeries(LightweightCharts.LineSeries, {
      color: color, lineWidth: 2,
      priceScaleId: 'right', title: name,
    }, targetPaneIndex);
    series.setData(data);
    chartindicators[name] = series;
    indicatorColorMap[name] = color;
    indicatorMeta[name] = { data, color, onSubPane, visible: true };
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
  const meta = indicatorMeta[name];
  if (!meta) return;
  meta.visible = checkbox.checked;

  // Main-pane indicators just toggle visibility — no axis to drop.
  if (!meta.onSubPane) {
    const series = chartindicators[name];
    if (series) series.applyOptions({ visible: checkbox.checked });
    chart.timeScale().fitContent();
    return;
  }

  // Sub-pane indicators: remove/recreate the series so the pane (and its
  // axis) can drop away when the last visible series in it is unchecked,
  // and come back when any is re-checked.
  if (!checkbox.checked) {
    const series = chartindicators[name];
    if (series) {
      try { chart.removeSeries(series); } catch(e) {}
      delete chartindicators[name];
    }
    const anyVisible = Object.values(indicatorMeta).some(m => m.onSubPane && m.visible);
    if (!anyVisible && subPane) {
      try { chart.removePane(subPane.paneIndex()); } catch(e) {}
      subPane = null;
    }
  } else {
    if (!subPane) subPane = chart.addPane();
    const series = chart.addSeries(LightweightCharts.LineSeries, {
      color: meta.color, lineWidth: 2,
      priceScaleId: 'right', title: name,
    }, subPane.paneIndex());
    series.setData(meta.data);
    chartindicators[name] = series;
  }
  chart.timeScale().fitContent();
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
                   `<td>${o.limitprice||'—'}</td><td>${o.stopprice||'—'}</td>` +
                   `<td>${o.status}</td>`;
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

  // Always render pagination affordance at the bottom. Disable each button
  // when there's nothing to navigate to so the controls are still visible.
  const paginationEl = document.getElementById('eventpagination');
  paginationEl.innerHTML = '';
  paginationEl.className = 'mt-2 mb-3 d-flex align-items-center gap-2';

  const hasOlder = events.length === 300;
  const hasNewer = page > 0;

  const olderBtn = document.createElement('button');
  olderBtn.className = 'btn btn-sm btn-outline-secondary';
  olderBtn.textContent = '← Older';
  olderBtn.disabled = !hasOlder;
  olderBtn.onclick = () => loadHistory(page + 1);
  paginationEl.appendChild(olderBtn);

  const newerBtn = document.createElement('button');
  newerBtn.className = 'btn btn-sm btn-outline-secondary';
  newerBtn.textContent = 'Newer →';
  newerBtn.disabled = !hasNewer;
  newerBtn.onclick = () => loadHistory(page - 1);
  paginationEl.appendChild(newerBtn);

  const status = document.createElement('span');
  status.className = 'text-muted small ms-2';
  if (events.length === 0 && page === 0) {
    status.textContent = 'No events yet';
  } else {
    status.textContent = `Page ${page + 1} — ${events.length} event${events.length === 1 ? '' : 's'}`;
  }
  paginationEl.appendChild(status);
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

    // Trade Log section (per-tick excerpt of the live log)
    html += '<h6 class="fw-bold mb-2">Trade Log</h6>';
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
      drawOrderPriceLines([]);
    } else {
      const n = data.count || 0;
      badge.textContent = n;
      badge.className = 'badge ms-1 ' + (n > 0 ? 'bg-warning text-dark' : 'bg-secondary');
      badge.title = '';
      drawOrderPriceLines(data.orders || []);
    }
  } catch(e) {}
}

// Draw a horizontal price line on the candle chart for each open order leg.
// Green = limit price, red = stop trigger. Bracket orders get both.
function drawOrderPriceLines(orders) {
  if (!candleSeries) return;
  for (const line of orderPriceLines) {
    try { candleSeries.removePriceLine(line); } catch(e) {}
  }
  orderPriceLines = [];

  for (const o of orders) {
    if (currentProductId && o.product_id && o.product_id !== currentProductId) continue;
    const cfg = o.order_configuration || {};
    const cfgVal = cfg[Object.keys(cfg)[0] || ''] || {};
    const side = (o.side || '').toUpperCase();
    const limit = parseFloat(cfgVal.limit_price);
    const stop = parseFloat(cfgVal.stop_price || cfgVal.stop_trigger_price);

    if (!isNaN(limit) && limit > 0) {
      orderPriceLines.push(candleSeries.createPriceLine({
        price: limit,
        color: '#26a69a',
        lineWidth: 1,
        lineStyle: LightweightCharts.LineStyle.Dashed,
        axisLabelVisible: true,
        title: side ? side + ' limit' : 'limit',
      }));
    }
    if (!isNaN(stop) && stop > 0) {
      orderPriceLines.push(candleSeries.createPriceLine({
        price: stop,
        color: '#ef5350',
        lineWidth: 1,
        lineStyle: LightweightCharts.LineStyle.Dashed,
        axisLabelVisible: true,
        title: side ? side + ' stop' : 'stop',
      }));
    }
  }
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
    const orders = data.orders || [];
    const internal = data.internal || [];

    // Keep the navbar badge in sync with the exchange count.
    const badge = document.getElementById('openOrdersCount');
    if (badge) {
      badge.textContent = orders.length;
      badge.className = 'badge ms-1 ' + (orders.length > 0 ? 'bg-warning text-dark' : 'bg-secondary');
    }
    drawOrderPriceLines(orders);

    // Friendly names for Coinbase order-configuration keys + a hint about
    // what the "Limit" / "Trigger" cells mean for each type.
    const cfgTypeName = {
      'limit_limit_gtc':            'Limit',
      'limit_limit_gtd':            'Limit (GTD)',
      'market_market_ioc':          'Market',
      'stop_limit_stop_limit_gtc':  'Stop Loss (stop-limit)',
      'stop_limit_stop_limit_gtd':  'Stop Loss (GTD)',
      'trigger_bracket_gtc':        'Bracket (TP + SL)',
      'trigger_bracket_gtd':        'Bracket (GTD)',
    };

    let html = '';

    // === Section 1: live on the exchange ===
    html += '<h6 class="fw-bold mb-2">On Exchange (Coinbase)</h6>';
    if (data.error) {
      html += `<div class="alert alert-danger small mb-3">Error from Coinbase: ${escHtml(data.error)}</div>`;
    } else if (orders.length === 0) {
      html += '<p class="text-muted small mb-3">No open orders on the exchange.</p>';
    } else {
      html += '<table class="table table-sm table-bordered table-striped mb-3"><thead><tr>' +
        '<th>Created</th><th>Product</th><th>Side</th><th>Type</th>' +
        '<th>Size</th>' +
        '<th title="For Limit: fill price. For Bracket: take-profit price. For Stop Loss (stop-limit): post-trigger slip cap.">Limit</th>' +
        '<th title="Stop-loss trigger price (or bracket SL trigger).">Trigger</th>' +
        '<th>Status</th><th class="small">Order ID</th>' +
        '</tr></thead><tbody>';
      for (const o of orders) {
        const cfg = o.order_configuration || {};
        const cfgKey = Object.keys(cfg)[0] || '';
        const cfgVal = cfg[cfgKey] || {};
        const limit = cfgVal.limit_price || '—';
        // trigger_bracket_gtc/gtd uses stop_trigger_price; stop_limit_* uses stop_price
        const stop = cfgVal.stop_price || cfgVal.stop_trigger_price || '—';
        const size = cfgVal.base_size || cfgVal.quote_size || '—';
        const typeName = cfgTypeName[cfgKey] || cfgKey;
        let created = '—';
        if (o.created_time) {
          const t = Date.parse(o.created_time);
          if (!isNaN(t)) created = fmtUtc(Math.floor(t / 1000));
        }
        html += `<tr><td class="small text-nowrap">${escHtml(created)}</td>` +
          `<td>${escHtml(o.product_id || '')}</td>` +
          `<td>${escHtml(o.side || '')}</td>` +
          `<td class="small" title="${escHtml(cfgKey)}">${escHtml(typeName)}</td>` +
          `<td>${escHtml(String(size))}</td>` +
          `<td>${escHtml(String(limit))}</td>` +
          `<td>${escHtml(String(stop))}</td>` +
          `<td>${escHtml(o.status || '')}</td>` +
          `<td class="small font-monospace">${escHtml(o.order_id || '')}</td></tr>`;
      }
      html += '</tbody></table>';
    }

    // === Section 2: internal-only state ===
    // Coinbase has no concept of trail %, activation, peak, or hard stop —
    // those live in the local liveorder rows. Show them here so the user
    // can see the trail layered on top of the exchange order.
    html += '<h6 class="fw-bold mb-2 mt-3">Internal Tracking <span class="text-muted small">(trail state — not on the exchange)</span></h6>';
    html += '<p class="text-muted small mb-2">' +
      'Trailing orders live here until activation. The <b>Limit / Activation</b> ' +
      'column is the threshold the market has to cross before the system places ' +
      'a trailing stop on Coinbase. If a <b>Hard Stop</b> is set on a trailing ' +
      'Exit, only that hard stop sits on the exchange pre-activation as initial ' +
      'protection — it gets cancelled and replaced by the trailing stop once ' +
      'activation fires. Entries (Buy/Sell) with a trail have nothing on the ' +
      'exchange pre-activation.' +
      '</p>';
    if (internal.length === 0) {
      html += '<p class="text-muted small mb-0">No internal open orders.</p>';
    } else {
      const pct = (v) => (v && v > 0) ? (v * 100).toFixed(2) + '%' : '—';
      const num = (v, d=2) => (v && v > 0) ? Number(v).toFixed(d) : '—';
      const exchangeIds = new Set(orders.map(o => o.order_id));
      html += '<table class="table table-sm table-bordered table-striped mb-0"><thead><tr>' +
        '<th>Time</th><th>Type</th><th>Amount</th>' +
        '<th title="For trailing Exits this is the activation threshold (not a fill price). For other order types it is the limit price.">Limit / Activation</th>' +
        '<th title="Current stop price (moves up as the trail follows the peak).">Current Stop</th>' +
        '<th>Limit Trail %</th>' +
        '<th>Activated</th><th>Peak</th>' +
        '<th title="Floor for the trailing stop — the stop never moves below this.">Hard Stop</th>' +
        '<th>On Exch</th><th class="small">CB Order ID</th>' +
        '</tr></thead><tbody>';
      for (const o of internal) {
        const dt = fmtUtc(o.time);
        const onExch = exchangeIds.has(o.coinbase_order_id);
        // For trailing Exits, the limitprice field is an activation threshold,
        // not a Coinbase limit — label it so the value isn't mistaken for a TP.
        const trailing = (o.tradetype === 'Exit') && (o.limittrailpercent > 0);
        const limitLabel = trailing && o.limitprice > 0
          ? `<span class="text-muted small">act @</span> ${num(o.limitprice)}`
          : num(o.limitprice);
        html += `<tr><td class="small text-nowrap">${escHtml(dt)}</td>` +
          `<td>${escHtml(o.tradetype || '')}</td>` +
          `<td>${(o.amount||0).toFixed(6)}</td>` +
          `<td>${limitLabel}</td>` +
          `<td>${num(o.stopprice)}</td>` +
          `<td>${pct(o.limittrailpercent)}</td>` +
          `<td>${o.activated ? 'yes' : 'no'}</td>` +
          `<td>${num(o.peak_price)}</td>` +
          `<td>${num(o.hard_stopprice)}</td>` +
          `<td>${onExch ? '<span class="text-success">yes</span>' : '<span class="text-warning">no</span>'}</td>` +
          `<td class="small font-monospace">${escHtml(o.coinbase_order_id || '')}</td></tr>`;
      }
      html += '</tbody></table>';
    }

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

  // Restore event-filter checkbox state.
  const savedFilters = localStorage.getItem('liveEventFilters');
  if (savedFilters) {
    try {
      const f = JSON.parse(savedFilters);
      document.getElementById('chkUser').checked   = f.user   !== false;
      document.getElementById('chkCreate').checked = f.create !== false;
      document.getElementById('chkFill').checked   = f.fill   !== false;
      document.getElementById('chkCancel').checked = f.cancel !== false;
    } catch(e) {}
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
    idleBalanceTimer = setInterval(() => { refreshAccountForProduct(currentProductId); pollBalance(); }, 15000);
    loadCandles();
  }

  // Open-orders badge: refresh on load and every 30s
  refreshOpenOrdersCount();
  if (openOrdersTimer) clearInterval(openOrdersTimer);
  openOrdersTimer = setInterval(refreshOpenOrdersCount, 30000);
})();
