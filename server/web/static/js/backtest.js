let candleSeries;
let chart;
let picker1;
let picker2;

async function runScript() {
  const select = document.getElementById('scriptDropdown');
  const scriptid = select.value;  
  if(scriptid > -1){
    if (!picker1 || picker1.dates.picked.length === 0) return null;
    let dt1 = picker1.dates.picked[0];
    let start = Date.UTC(dt1.year, dt1.month, dt1.date, dt1.hours, dt1.minutes)/1000;
    //const start = Math.floor(dt.toJSDate().getTime() / 1000);
    //const start = picker1.dates.picked[0].toUnixInteger();
    let dt2 = picker2.dates.picked[0];
    let stop = Date.UTC(dt2.year, dt2.month, dt2.date, dt2.hours, dt2.minutes)/1000;
    const response = await fetch('/api/startsim', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ 'scriptid': scriptid, 'start':start, 'stop':stop }),
    });

    if (response.ok) {
      document.getElementById('prunsim').classList.remove('d-none');
      document.getElementById('bstopsim').classList.remove('d-none');
      document.getElementById('bstartsim').classList.add('d-none');
      const data = await response.json();
      const simid = data['simid']
      const simresponse = await fetch('/api/fetchsim?simid=' + simid, {
        method: 'GET',
        headers: { 'Content-Type': 'application/json' },
      });

      if (simresponse.ok) {
        const data = await simresponse.json();
        const candles = data['candles']
        const simassets = data['simassets']
        clearseries()
        setseries(candles);
        chart.timeScale().fitContent();
      } else {
        showMessage("Failed to load sim");
      }
      setTimeout(() => {
        document.getElementById('prunsim').classList.add('d-none');
        document.getElementById('bstopsim').classList.add('d-none');
        document.getElementById('bstartsim').classList.remove('d-none');
      }, 1000);
    }
    else{
    }

  } else {
    showMessage("Failed to save");
    
  }

}



function clearseries(){
  candleSeries.setData([]);
}
function setseries(candles){
  let bars = [];
  for (const candle of candles) { 
    bars.push({ time: candle['timestamp'], open: candle['open'], high: candle['high'], low: candle['low'], close: candle['close'] });
  }
  candleSeries.setData(bars);

}
function addcandle(candle){

candleSeries.update(candle);
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

  // Optional: nicer candles
  candleSeries.applyOptions({
    upColor: '#26a69a',
    downColor: '#ef5350',
    borderVisible: false,
    wickUpColor: '#26a69a',
    wickDownColor: '#ef5350',
  });
candleSeries.setData([
      { time: '2025-01-01', open: 100, high: 110, low: 95,  close: 108 },
      { time: '2025-01-02', open: 108, high: 115, low: 105, close: 112 },
      { time: '2025-01-03', open: 112, high: 118, low: 108, close: 110 },
      // add more...
    ]);
  //candleSeries.setData([/* your data */]);
  chart.timeScale().fitContent();

picker2 = new tempusDominus.TempusDominus(document.getElementById('datetimepicker2'), {
  defaultDate: new Date(new Date().getFullYear(), new Date().getMonth(), new Date().getDate() - 1),
  useCurrent: false,           // ← stops jumping to today
  display: {
    icons: { time: 'bi bi-clock', date: 'bi bi-calendar', up: 'bi bi-arrow-up', down: 'bi bi-arrow-down' },
    components: { clock: true, hours: true, minutes: true}
  }
});

picker1 = new tempusDominus.TempusDominus(document.getElementById('datetimepicker1'), {
  defaultDate: new Date(new Date().getFullYear(), new Date().getMonth(), new Date().getDate() - 2),
  useCurrent: false,           // ← stops jumping to today
  display: {
    icons: { time: 'bi bi-clock', date: 'bi bi-calendar', up: 'bi bi-arrow-up', down: 'bi bi-arrow-down' },
    components: { clock: true, hours: true, minutes: true}
  }
});

