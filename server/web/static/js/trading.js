
let chart, candleSeries, markersPrimitive;
let chartindicators = {};
let indicatorColorMap = {};
let subPanes = [];
let candleslist = [];
let colors = ['#FF11FF','#11FFFF','#FFFF11','#AAAAFF','#AAFFAA','#FFAAAA','#FFFFFF','#AAAAAA'];
let pollTimer = null;
let pricePollTimer = null;
let isRunning = false;
let granularityLocked = false;
let currentPair = 'btc';
let _stopping = false;

const PRODUCT_SPECS = {
  btc:  { label: 'BTC',  maxLev: 3.3,  contractSize: 0.01 },
  eth:  { label: 'ETH',  maxLev: 3.0,  contractSize: 0.1 },
  sol:  { label: 'SOL',  maxLev: 1.8,  contractSize: 5.0 },
  xrp:  { label: 'XRP',  maxLev: 1.8,  contractSize: 500.0 },
  doge: { label: 'DOGE', maxLev: 1.1,  contractSize: 5000.0 },
  ada:  { label: 'ADA',  maxLev: 2.4,  contractSize: 1000.0 },
  paxg: { label: 'PAXG', maxLev: 12.1, contractSize: 1.0 },
  zec:  { label: 'ZEC',  maxLev: 2.0,  contractSize: 1.0 },
  xlm:  { label: 'XLM',  maxLev: 2.6,  contractSize: 5000.0 },
  link: { label: 'LINK', maxLev: 2.3,  contractSize: 50.0 },
  sui:  { label: 'SUI',  maxLev: 1.8,  contractSize: 500.0 },
  aave: { label: 'AAVE', maxLev: 1.5,  contractSize: 5.0 },
};

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

// ------------------------------------------------------------------ script / product / granularity dropdowns

function syncProductDropdown(pair) {
  const pdrop = document.getElementById('productDropdown');
  for (const opt of pdrop.options) {
    if (opt.value === (pair || '').toLowerCase()) { opt.selected = true; return; }
  }
}

function updateProductPanel() {
  if (isRunning) return;
  const specs = PRODUCT_SPECS[currentPair];
  if (!specs) return;
  document.getElementById('acc_pair').textContent = specs.label + '-PERP-INTX';
  document.getElementById('acc_lev').textContent  = specs.maxLev + 'x';
  document.getElementById('acc_contract_size').textContent = specs.contractSize + ' ' + specs.label;
  document.getElementById('acc_gran').textContent =
    document.getElementById('granularityDropdown').value || '—';
}

async function onScriptChange() {
  if (granularityLocked) return;
  const scriptid = document.getElementById('scriptDropdown').value;
  if (scriptid < 0) return;
  localStorage.setItem('selectedScriptId', scriptid);
  try {
    const resp = await fetch(`/api/live/scriptgranularity?scriptid=${scriptid}`);
    if (resp.ok) {
      const data = await resp.json();
      currentPair = data.pair || 'btc';
      syncProductDropdown(currentPair);
      const drop = document.getElementById('granularityDropdown');
      for (const opt of drop.options) {
        if (opt.value === data.granularity) { opt.selected = true; break; }
      }
      updateProductPanel();
      loadCandles();
    }
  } catch(e) {}
}

function onProductChange() {
  if (isRunning) return;
  currentPair = document.getElementById('productDropdown').value;
  updateProductPanel();
  loadCandles();
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
  document.getElementById('productDropdown').disabled = running;
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
  const pairBase = (data.pair || '').toUpperCase();
  document.getElementById('acc_contract_size').textContent =
    data.contract_size ? data.contract_size + ' ' + pairBase : '—';
  document.getElementById('acc_pair').textContent = pairBase + '-PERP-INTX';
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
    const params = isRunning ? '' : `?pair=${encodeURIComponent(currentPair)}&granularity=${encodeURIComponent(gran)}`;
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
    const dt = new Date(o.time * 1000).toLocaleString();
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
    const dt = new Date(e.time * 1000).toLocaleString();
    let details = '';
    try {
      const d = JSON.parse(e.eventdata || '{}');
      details = Object.entries(d)
        .filter(([k]) => k !== 'time')
        .map(([k,v]) => `${k}:${typeof v==='number'?v.toFixed(4):v}`)
        .join(' | ');
    } catch(_) {}
    tr.innerHTML = `<td>${dt}</td><td>${e.eventtype}</td><td class="small text-muted">${details}</td>`;
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
        if (data.pair) syncProductDropdown(data.pair);
        if (data.granularity) {
          const gdrop = document.getElementById('granularityDropdown');
          for (const opt of gdrop.options) {
            if (opt.value === data.granularity) { opt.selected = true; break; }
          }
        }
      }
    }
  } catch(e) {}

  if (!isRunning) {
    updateProductPanel();
    try {
      const balResp = await fetch('/api/live/balance');
      if (balResp.ok) {
        const bal = await balResp.json();
        document.getElementById('acc_usd').textContent    = '$' + fmt(bal.usd);
        document.getElementById('acc_equity').textContent = '$' + fmt(bal.total_equity);
        const pnlEl = document.getElementById('acc_upnl');
        pnlEl.textContent = (bal.unrealized_pnl >= 0 ? '+' : '') + '$' + fmt(bal.unrealized_pnl);
        pnlEl.style.color = bal.unrealized_pnl >= 0 ? '#26a69a' : '#ef5350';
      }
    } catch(e) {}
  }

  // Fetch pair/granularity from selected script, then load chart
  await onScriptChange();
  if (!isRunning) loadCandles();
})();
