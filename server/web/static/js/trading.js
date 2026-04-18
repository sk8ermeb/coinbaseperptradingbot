
let chart, candleSeries, markersPrimitive;
let chartindicators = {};
let candleslist = [];
let colors = ['#FF11FF','#11FFFF','#FFFF11','#AAAAFF','#AAFFAA','#FFAAAA','#FFFFFF','#AAAAAA'];
let pollTimer = null;
let isRunning = false;
let granularityLocked = false;

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
  let html = '';
  for (const c of candleslist) {
    if (new Date(c.timestamp * 1000).getTime() === hovered.getTime()) {
      html += `<b>Candle</b><br>O:${c.open} H:${c.high} L:${c.low} C:${c.close}<br>`;
    }
  }
  // no eventsout panel on trading page, but could be added
});

// ------------------------------------------------------------------ script dropdown

async function onScriptChange() {
  if (granularityLocked) return;
  const scriptid = document.getElementById('scriptDropdown').value;
  if (scriptid < 0) return;
  try {
    const resp = await fetch(`/api/live/scriptgranularity?scriptid=${scriptid}`);
    if (resp.ok) {
      const data = await resp.json();
      const drop = document.getElementById('granularityDropdown');
      for (const opt of drop.options) {
        if (opt.value === data.granularity) { opt.selected = true; break; }
      }
    }
  } catch(e) {}
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
    setRunningUI(true);
    loadCandles();
    startPolling();
  } else {
    const err = await resp.json();
    showMessage(err.detail || 'Failed to start');
  }
}

async function stopTrading() {
  await fetch('/api/live/stop', { method: 'POST' });
  setRunningUI(false);
  stopPolling();
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

function startPolling() {
  if (pollTimer) clearInterval(pollTimer);
  pollStatus();
  pollTimer = setInterval(pollStatus, 15000);
}

function stopPolling() {
  if (pollTimer) { clearInterval(pollTimer); pollTimer = null; }
}

async function pollStatus() {
  try {
    const resp = await fetch('/api/live/status');
    if (!resp.ok) return;
    const data = await resp.json();
    updateAccountPanel(data);
    if (data.running !== isRunning) {
      setRunningUI(data.running);
    }
    document.getElementById('lastupdate').textContent =
      'Updated ' + new Date().toLocaleTimeString();

    // Refresh candles every poll cycle when running
    if (data.running) loadCandles();
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
  document.getElementById('acc_pair').textContent = (data.pair || '').toUpperCase() + '-PERP-INTX';
  document.getElementById('acc_gran').textContent = data.granularity || '—';

  // Log
  if (data.log && data.log.length) {
    const logEl = document.getElementById('livelog');
    logEl.textContent = data.log.join('\n');
    logEl.scrollTop = logEl.scrollHeight;
  }
}

// ------------------------------------------------------------------ chart

async function loadCandles() {
  try {
    const resp = await fetch('/api/live/candles');
    if (!resp.ok) return;
    const data = await resp.json();
    setChartCandles(data.candles || []);
    setChartIndicators(data.indicators || {});
    chart.timeScale().fitContent();
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
  let j = 0;
  for (const name in indicators) {
    const series = chart.addSeries(LightweightCharts.LineSeries, {
      color: colors[j % colors.length], lineWidth: 2,
      priceScaleId: 'right', title: name,
    });
    // entries are [{time, value}] objects
    const data = (indicators[name] || []).filter(e => e.value !== null && !isNaN(e.value));
    series.setData(data);
    chartindicators[name] = series;
    j++;
  }
}

// ------------------------------------------------------------------ history tab

document.querySelector('[data-bs-target="#historytab"]').addEventListener('shown.bs.tab', loadHistory);

async function loadHistory() {
  try {
    const resp = await fetch('/api/live/history');
    if (!resp.ok) return;
    const data = await resp.json();
    renderOrders(data.orders || []);
    renderEvents(data.events || []);
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

function renderEvents(events) {
  const tbody = document.getElementById('eventbody');
  tbody.innerHTML = '';
  for (const e of events) {
    const tr = document.createElement('tr');
    const dt = new Date(e.time * 1000).toLocaleString();
    let details = '';
    try {
      const d = JSON.parse(e.eventdata || '{}');
      details = Object.entries(d).map(([k,v]) => `${k}:${typeof v==='number'?v.toFixed(4):v}`).join(' | ');
    } catch(_) {}
    tr.innerHTML = `<td>${dt}</td><td>${e.eventtype}</td><td class="small text-muted">${details}</td>`;
    tbody.appendChild(tr);
  }
}

// ------------------------------------------------------------------ init on load

(async () => {
  // Read current status to restore UI state
  try {
    const resp = await fetch('/api/live/status');
    if (resp.ok) {
      const data = await resp.json();
      updateAccountPanel(data);
      if (data.running) {
        setRunningUI(true);
        startPolling();
      }
      // Set dropdowns to match running script/granularity
      if (data.scriptid) {
        const drop = document.getElementById('scriptDropdown');
        for (const opt of drop.options) {
          if (parseInt(opt.value) === data.scriptid) { opt.selected = true; break; }
        }
      }
      if (data.granularity) {
        const gdrop = document.getElementById('granularityDropdown');
        for (const opt of gdrop.options) {
          if (opt.value === data.granularity) { opt.selected = true; break; }
        }
      }
    }
  } catch(e) {}
  loadCandles();
  // On script change when not running, update granularity to script's default
  if (!isRunning) onScriptChange();
})();
