
// Shift a UTC epoch so that toLocaleString() displays it as UTC time.
// e.g. for UTC-5, 05:00 UTC → date that shows "05:00" in local display
function utcDate(unixSeconds) {
  const d = new Date(unixSeconds * 1000);
  return new Date(d.getTime() + d.getTimezoneOffset() * 60000);
}
// Reverse: a date picked by TempusDominus (which added the offset) back to real UTC epoch.
function pickerToUtc(date) {
  return Math.floor((date.getTime() - date.getTimezoneOffset() * 60000) / 1000);
}

let candleSeries;
let chart;
let markersPrimitive;
let picker1;
let picker2;
let chartindicators = {}
let candleslist = [];
let subPanes = [];
let eventlist = [];
let colors = ['#FF11FF', '#11FFFF', '#FFFF11', '#AAAAFF', '#AAFFAA', '#FFAAAA', '#FFFFFF', '#AAAAAA', '#AAFF11' ]
let initcandles = [
      { timestamp: 1766656800, open: 100, high: 110, low: 95,  close: 108 },
      { timestamp: 1766660400, open: 108, high: 115, low: 105, close: 112 },
      { timestamp: 1766664000, open: 112, high: 118, low: 108, close: 110 },
      // add more...
    ];

async function runScript() {
  const select = document.getElementById('scriptDropdown');
  const scriptid = select.value;  
  if(scriptid > -1){
    document.getElementById('prunsim').classList.remove('d-none');
    document.getElementById('bstopsim').classList.remove('d-none');
    document.getElementById('bstartsim').classList.add('d-none');
    if (!picker1 || picker1.dates.picked.length === 0) return null;
    let dt1 = picker1.dates.picked[0];
    //let start = Date.UTC(dt1.year, dt1.month, dt1.date, dt1.hours, dt1.minutes)/1000;
    let dt2 = picker2.dates.picked[0];
    //let stop = Date.UTC(dt2.year, dt2.month, dt2.date, dt2.hours, dt2.minutes)/1000;
    let start = pickerToUtc(picker1.dates.picked[0]);
    let stop  = pickerToUtc(picker2.dates.picked[0]);
    const response = await fetch('/api/startsim', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ 'scriptid': scriptid, 'start':start, 'stop':stop }),
    });

    if (response.ok) {
      const data = await response.json();
      const simid = data['simid']
      const simresponse = await fetch('/api/fetchsim?simid=' + simid, {
        method: 'GET',
        headers: { 'Content-Type': 'application/json' },
      });

      if (simresponse.ok) {
        const data = await simresponse.json();
        const candles = data['candles']
        const simassets = data['assets']
        const events = data['events']
        const indicators = data['indicators']
        const simlog = data['log']
        document.getElementById('simlog').innerHTML = simlog || '<i>Simulation produced no log entries.</i>';
        clearseries()
        setseries(candles);
        addindicators(indicators);
        setevents(events);
        displayStats(computeStats(candles, data['leverage']));
        chart.timeScale().fitContent();
        loadSimHistory(scriptid);
      } else {
        showMessage("Failed to load sim");
      }
    }
    else{
      const error = await response.json();  // { "detail": "my detail" }
      showMessage(error.detail);
    }

  } else {
    showMessage("You must first create a script to run in the algorithms tab (can be blank)");
    
  }
  setTimeout(() => {
    document.getElementById('prunsim').classList.add('d-none');
    document.getElementById('bstopsim').classList.add('d-none');
    document.getElementById('bstartsim').classList.remove('d-none');
  }, 1000);

}

function eventCategory(eventtype) {
  if (eventtype.startsWith('user'))   return 'user';
  if (eventtype.startsWith('fill'))   return 'fill';
  if (eventtype.startsWith('create')) return 'create';
  if (eventtype.startsWith('cancel')) return 'cancel';
  return 'other';
}

function applyEventFilters() {
  const showUser   = document.getElementById('chkUser').checked;
  const showCreate = document.getElementById('chkCreate').checked;
  const showFill   = document.getElementById('chkFill').checked;
  const showCancel = document.getElementById('chkCancel').checked;
  const markers = [];
  for (const myev of eventlist) {
    const cat = eventCategory(myev['eventtype']);
    if (cat === 'user'   && !showUser)   continue;
    if (cat === 'fill'   && !showFill)   continue;
    if (cat === 'create' && !showCreate) continue;
    if (cat === 'cancel' && !showCancel) continue;
    let Color = '';
    let Shape = '';
    if      (cat === 'user')   Color = colors[3];
    else if (cat === 'fill')   Color = colors[4];
    else if (cat === 'create') Color = colors[5];
    else if (cat === 'cancel') Color = colors[7];
    else                       Color = colors[6];
    if      (myev['eventtype'].includes('Buy'))         Shape = 'arrowUp';
    else if (myev['eventtype'].includes('Sell'))        Shape = 'arrowDown';
    else if (myev['eventtype'].includes('Liquidation')) Shape = 'circle';
    else if (cat === 'cancel')                          Shape = 'circle';
    else                                                Shape = 'square';
    markers.push({time: myev['time'], position: 'aboveBar', color: Color, shape: Shape, text: myev['eventtype']});
  }
  markersPrimitive.setMarkers(markers);
}

function onEventFilterChange() {
  localStorage.setItem('eventFilters', JSON.stringify({
    user:   document.getElementById('chkUser').checked,
    create: document.getElementById('chkCreate').checked,
    fill:   document.getElementById('chkFill').checked,
    cancel: document.getElementById('chkCancel').checked,
  }));
  applyEventFilters();
}

function setevents(events){
  eventlist = events;
  applyEventFilters();
}

function computeStats(candles, leverage) {
  if (!candles || candles.length === 0) return null;
  const first = candles[0];
  const last  = candles[candles.length - 1];
  const startEquity = first.sim_total_equity || 0;
  const finalEquity = last.sim_total_equity  || 0;
  const finalUSD    = last.sim_usd || 0;
  const EPS = 1e-8;
  let trades = [];
  let prev = 0;
  let openEquity = 0;
  for (const c of candles) {
    const cur    = c.sim_contracts || 0;
    const equity = c.sim_total_equity || 0;
    const wasIn  = Math.abs(prev) > EPS;
    const isIn   = Math.abs(cur)  > EPS;
    const flip   = wasIn && isIn && ((prev > 0) !== (cur > 0));
    if (!wasIn && isIn) {
      openEquity = equity;
    } else if (wasIn && !isIn) {
      trades.push({ open: openEquity, close: equity });
    } else if (flip) {
      trades.push({ open: openEquity, close: equity });
      openEquity = equity;
    }
    prev = cur;
  }
  const totalTrades      = trades.length;
  const profitableTrades = trades.filter(t => t.close > t.open).length;
  const pcts = trades.map(t => (t.close - t.open) / Math.abs(t.open) * 100);
  const avgPct = pcts.length > 0 ? pcts.reduce((a, b) => a + b, 0) / pcts.length : 0;
  const totalPct = startEquity > 0 ? (finalEquity - startEquity) / startEquity * 100 : 0;
  return { startEquity, finalEquity, finalUSD, leverage: leverage || '—', totalTrades, profitableTrades, avgPct, totalPct };
}

function displayStats(stats) {
  const el = document.getElementById('simstats');
  if (!stats) { el.innerHTML = '<i class="text-muted">No data.</i>'; return; }
  const profitPct = stats.totalTrades > 0
    ? ((stats.profitableTrades / stats.totalTrades) * 100).toFixed(1) + '%'
    : '—';
  const avgColor   = stats.avgPct   >= 0 ? 'text-success' : 'text-danger';
  const totalColor = stats.totalPct >= 0 ? 'text-success' : 'text-danger';
  el.innerHTML = `
    <table class="table table-sm table-borderless mb-0">
      <tbody>
        <tr><td class="text-muted">Starting Margin</td>
            <td class="text-end fw-bold">$${stats.startEquity.toFixed(2)}</td></tr>
        <tr><td class="text-muted">Leverage</td>
            <td class="text-end fw-bold">${stats.leverage}x</td></tr>
        <tr><td colspan="2"><hr class="my-1"></td></tr>
        <tr><td class="text-muted">Total Equity</td>
            <td class="text-end fw-bold">$${stats.finalEquity.toFixed(2)}</td></tr>
        <tr><td class="text-muted">Free USD</td>
            <td class="text-end fw-bold">$${stats.finalUSD.toFixed(2)}</td></tr>
        <tr><td class="text-muted">Total Trades</td>
            <td class="text-end fw-bold">${stats.totalTrades}</td></tr>
        <tr><td class="text-muted">Profitable</td>
            <td class="text-end fw-bold">${stats.profitableTrades} <span class="text-muted fw-normal">(${profitPct})</span></td></tr>
        <tr><td class="text-muted">Avg Gain/Trade</td>
            <td class="text-end fw-bold ${avgColor}">${stats.avgPct.toFixed(2)}%</td></tr>
        <tr><td colspan="2"><hr class="my-1"></td></tr>
        <tr><td class="text-muted">Total Return</td>
            <td class="text-end fw-bold ${totalColor}">${stats.totalPct.toFixed(2)}%</td></tr>
      </tbody>
    </table>`;
}

function clearseries(){
  candleSeries.setData([]);
  markersPrimitive.setMarkers([])
  for(inds in chartindicators){
    chart.removeSeries(chartindicators[inds]);
    delete chartindicators[inds];
  }
  for (const pane of subPanes) {
    try { chart.removePane(pane.paneIndex()); } catch(e) {}
  }
  subPanes = [];
}
function setseries(candles){
  let bars = [];
  candleslist = candles;
  for (const candle of candles) {
    bars.push({ time: candle['timestamp'], open: candle['open'], high: candle['high'], low: candle['low'], close: candle['close'] });
  }
  candleSeries.setData(bars);
}
function addcandle(candle){
  candleSeries.update(candle);
}

function chartmouseposition(param)
{
  //on some browsers, adding in this console logs makes it actually work real time instead of when you click
  //console.log('1');
  if(param.time === undefined){
    return;
  }
  let hout = '';
  hoveredtime = new Date(param.time * 1000);
  for (const candle of candleslist) {
    myts = new Date(candle['timestamp'] * 1000);
    if (myts.getTime() === hoveredtime.getTime()){
      const isostr = myts.toLocaleString('en-US', {
        timeZone: 'UTC',
        year: 'numeric',
        month: 'numeric',
        day: 'numeric',
        hour: 'numeric',
        minute: '2-digit',
        hour12: true
      }).replace(',', '');
      const simUsd = candle['sim_usd'] !== undefined ? '$'+candle['sim_usd'].toFixed(2) : '—';
      const simEquity = candle['sim_total_equity'] !== undefined ? '$'+candle['sim_total_equity'].toFixed(2) : '—';
      const simContracts = candle['sim_contracts'] !== undefined ? candle['sim_contracts'].toFixed(6) : '—';
      hout += "<b>Candle</b></br>Time:"+isostr+
        "</br>Open: "+candle['open']+
        "</br>Close:"+candle['close']+
        "</br>High: "+candle['high']+
        "</br>Low:  "+candle['low']+
        "</br><hr style='margin:4px 0'>"+
        "</br><b>Free Margin:</b> "+simUsd+
        "</br><b>Total Equity:</b> "+simEquity+
        "</br><b>Contracts:</b> "+simContracts;
    }
  }
  hout +="</br>";
  for (const cevent of eventlist){
    myts = new Date(cevent['time'] * 1000);
    //console.log(myts);
    if (myts.getTime() === hoveredtime.getTime()){
      hout += "---<b>"+cevent['eventtype']+"</b>---</br>"; 
      cdata = JSON.parse(cevent['eventdata']);
      for (const [key, value] of Object.entries(cdata)) {
        hout += key+": "+value+"</br>"; 
      }
    }
  }
  document.getElementById('eventsout').innerHTML = hout;
}

chart = LightweightCharts.createChart(document.getElementById('chart'), {
    autoSize: true,
    layout: {
      background: { color: '#1a1a1a' },
      textColor: '#d1d4dc',
    },
    grid: {
      vertLines: { color: '#2a2e39' },
      horzLines: { color: '#2a2e39' },
    },
    crosshair: {
      mode: LightweightCharts.CrosshairMode.Normal,
    },
    rightPriceScale: {
      borderColor: '#2a2e39',
    },
    timeScale: {
      borderColor: '#2a2e39',
      timeVisible: true,
      secondsVisible: false,
    },
  });
  //chart.applyOptions({ width: 800, height: 500 });
  candleSeries = chart.addSeries(LightweightCharts.CandlestickSeries);
  //candleSeries = chart.addCandlestickSeries();
  // Optional: nicer candles
  candleSeries.applyOptions({
    upColor: '#26a69a',
    downColor: '#ef5350',
    borderVisible: false,
    wickUpColor: '#26a69a',
    wickDownColor: '#ef5350',
  });
  markersPrimitive = LightweightCharts.createSeriesMarkers(candleSeries, []);
  chart.timeScale().fitContent();

var dt1 = document.getElementById('dt1hid').dataset.hidden;
var dt2 = document.getElementById('dt2hid').dataset.hidden;
let start = new Date(dt1.year, dt1.month, dt1.date, dt1.hours, dt1.minutes).getTime() / 1000;
let stop = new Date(dt2.year, dt2.month, dt2.date, dt2.hours, dt2.minutes).getTime() / 1000;

const pickerMaxDate = new Date();
const pickerMinDate = new Date(Date.now() - Math.floor(5 * 365.25 * 24 * 3600 * 1000));

picker2 = new tempusDominus.TempusDominus(document.getElementById('datetimepicker2'), {
  defaultDate: utcDate(parseInt(dt2)),
  useCurrent: false,
  restrictions: { minDate: pickerMinDate, maxDate: pickerMaxDate },
  display: {
    icons: { time: 'bi bi-clock', date: 'bi bi-calendar', up: 'bi bi-arrow-up', down: 'bi bi-arrow-down' },
    components: { clock: true, hours: true, minutes: true}
  }
});
chart.subscribeCrosshairMove(chartmouseposition);

setseries(initcandles);

picker1 = new tempusDominus.TempusDominus(document.getElementById('datetimepicker1'), {
  defaultDate: utcDate(parseInt(dt1)),
  useCurrent: false,
  restrictions: { minDate: pickerMinDate, maxDate: pickerMaxDate },
  display: {
    icons: { time: 'bi bi-clock', date: 'bi bi-calendar', up: 'bi bi-arrow-up', down: 'bi bi-arrow-down' },
    components: { clock: true, hours: true, minutes: true}
  }
});

function addindicators(indicators){
  let j = 0;

  // Detect price scale so we can tell oscillators apart from price-scale indicators.
  // Any indicator whose values are all < 5% of the current close belongs in a sub-pane.
  const lastClose = candleslist.length > 0 ? candleslist[candleslist.length - 1].close : 0;
  let subPane = null;

  for (const ind in indicators){
    const data = indicators[ind];

    // Auto-detect oscillator: all non-null values are tiny compared to price
    let isOscillator = false;
    if (lastClose > 0 && data.length > 0) {
      const vals = data.map(d => Math.abs(d.value)).filter(v => v != null && isFinite(v));
      if (vals.length > 0 && Math.max(...vals) < lastClose * 0.05) {
        isOscillator = true;
      }
    }

    let targetPaneIndex = undefined; // undefined → main (candle) pane
    if (isOscillator) {
      if (!subPane) {
        subPane = chart.addPane();
        subPanes.push(subPane);
      }
      targetPaneIndex = subPane.paneIndex();
    }

    let indicatorseries = chart.addSeries(LightweightCharts.LineSeries, {
      color: colors[j],
      lineWidth: 2,
      priceScaleId: 'right',
      title: ind
    }, targetPaneIndex);

    indicatorseries.setData(data);
    chartindicators[ind] = indicatorseries;
    j++;
    if(j >= colors.length) j = 0;
  }
}
document.getElementById('simlog').innerHTML = "Log<br>Files";

function onScriptChange(select) {
  if (select && select.value > -1) {
    localStorage.setItem('selectedScriptId', select.value);
    loadSimHistory(select.value);
  }
}

async function loadSimHistory(scriptid) {
  const dropdown = document.getElementById('historyDropdown');
  dropdown.innerHTML = '<option value="-1">History</option>';
  if (!scriptid || scriptid < 0) return;
  const response = await fetch('/api/simhistory?scriptid=' + scriptid);
  if (!response.ok) return;
  const data = await response.json();
  for (const run of data.runs) {
    let label;
    if (run.runat && run.runat > 0) {
      const dt = new Date(run.runat * 1000);
      label = dt.toLocaleString('en-US', {
        month: 'numeric', day: 'numeric', year: 'numeric',
        hour: 'numeric', minute: '2-digit', hour12: true
      });
    } else {
      const d1 = new Date(run.start * 1000).toLocaleDateString('en-US');
      const d2 = new Date(run.stop * 1000).toLocaleDateString('en-US');
      label = d1 + ' → ' + d2;
    }
    const opt = document.createElement('option');
    opt.value = run.id;
    opt.textContent = label;
    dropdown.appendChild(opt);
  }
}

async function onHistoryChange(select) {
  const simid = select.value;
  if (simid < 0) return;
  const simresponse = await fetch('/api/fetchsim?simid=' + simid);
  if (!simresponse.ok) { showMessage('Failed to load sim'); return; }
  const data = await simresponse.json();
  document.getElementById('simlog').innerHTML = data.log || '<i>Simulation produced no log entries.</i>';
  clearseries();
  setseries(data.candles);
  addindicators(data.indicators);
  setevents(data.events);
  displayStats(computeStats(data.candles, data.leverage));
  chart.timeScale().fitContent();
}

document.addEventListener('DOMContentLoaded', () => {
  const lastId = localStorage.getItem('selectedScriptId');
  const select = document.getElementById('scriptDropdown');
  if (lastId) {
    for (const opt of select.options) {
      if (opt.value === lastId) { opt.selected = true; break; }
    }
  }
  if (select.value > -1) loadSimHistory(select.value);

  const saved = localStorage.getItem('eventFilters');
  if (saved) {
    const f = JSON.parse(saved);
    document.getElementById('chkUser').checked   = f.user   !== false;
    document.getElementById('chkCreate').checked = f.create !== false;
    document.getElementById('chkFill').checked   = f.fill   !== false;
    document.getElementById('chkCancel').checked = f.cancel !== false;
  }
});
