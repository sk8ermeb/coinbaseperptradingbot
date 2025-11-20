  const chart = LightweightCharts.createChart(document.getElementById('chart'), {
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
  const candleSeries = chart.addSeries(LightweightCharts.CandlestickSeries);

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



    new tempusDominus.TempusDominus(document.getElementById('datetimepicker1'), {
      display: {
        icons: { time: 'bi bi-clock', date: 'bi bi-calendar', up: 'bi bi-arrow-up', down: 'bi bi-arrow-down' },
        components: { clock: true, hours: true, minutes: true }
      }
    });
    new tempusDominus.TempusDominus(document.getElementById('datetimepicker2'), {
      display: {
        icons: { time: 'bi bi-clock', date: 'bi bi-calendar', up: 'bi bi-arrow-up', down: 'bi bi-arrow-down' },
        components: { clock: true, hours: true, minutes: true }
      }
    });
  
