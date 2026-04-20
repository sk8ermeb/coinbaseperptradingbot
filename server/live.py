import util
import talib
import numpy
import traceback
import json
import time
import math
import threading
import uuid
from enum import Enum
from datetime import datetime

lutil = util.util()

GRAN_SECONDS = {
    'ONE_MINUTE': 60, 'FIVE_MINUTE': 300, 'FIFTEEN_MINUTE': 900,
    'ONE_HOUR': 3600, 'SIX_HOUR': 21600, 'ONE_DAY': 86400
}
GRANULARITIES = list(GRAN_SECONDS.keys())


class LiveTrader:
    def __init__(self, scriptid):
        self.scriptid = scriptid
        self.running = False
        self.thread = None
        self.namespace = {}
        self.historysize = 100
        self.pair = 'btc'
        self.granularity = 'ONE_HOUR'
        self.candle_history = []
        self._ind_history = {}

    # ------------------------------------------------------------------ startup

    def start(self):
        self.running = True
        lutil.setkeyval('live_scriptid', str(self.scriptid))
        lutil.setkeyval('live_running', 'true')
        self.thread = threading.Thread(target=self._run_with_restart, daemon=True)
        self.thread.start()

    def stop(self):
        self.running = False
        lutil.setkeyval('live_running', 'false')
        self._livelog("Live trader stopped by user")

    # ------------------------------------------------------------------ logging

    def _livelog(self, msg):
        ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
        entry = f"{ts}: {msg}"
        print(f"[LIVE] {entry}")
        existing = lutil.getkeyval('live_log') or ''
        lines = existing.split('\n') if existing else []
        lines.append(entry)
        lutil.setkeyval('live_log', '\n'.join(lines[-500:]))

    def _log_event(self, event_type, data):
        try:
            lutil.runinsert(
                "INSERT INTO liveevent (scriptid, eventtype, eventdata, time) VALUES(?,?,?,?)",
                (self.scriptid, event_type, json.dumps(data), int(time.time()))
            )
        except Exception:
            pass

    # ------------------------------------------------------------------ main loop

    def _run_with_restart(self):
        while self.running:
            try:
                self._run_loop()
            except Exception:
                self._livelog(f"CRASH:\n{traceback.format_exc()}")
                if self.running:
                    self._livelog("Auto-restarting in 60 seconds...")
                    time.sleep(60)

    def _run_loop(self):
        self._livelog(f"Starting — script {self.scriptid}")
        scripts = lutil.runselect("SELECT * FROM scripts WHERE id=?", (self.scriptid,))
        if not scripts:
            self._livelog("Script not found — stopping")
            self.running = False
            return

        self._init_namespace()
        try:
            exec(scripts[0]['script'], self.namespace)
        except Exception:
            self._livelog(f"Script init error:\n{traceback.format_exc()}")
            self.running = False
            return

        self.pair = self.namespace.get('pair', 'btc')
        self.granularity = self.namespace.get('granularity', 'ONE_HOUR')
        product_id = self.pair.upper() + '-PERP-INTX'
        gran_secs = GRAN_SECONDS.get(self.granularity, 3600)

        lutil.setkeyval('live_pair', self.pair)
        lutil.setkeyval('live_granularity', self.granularity)
        self._livelog(f"Pair: {product_id} | Granularity: {self.granularity}")

        self._load_history(product_id)
        self._livelog("History loaded — waiting for next candle close")

        while self.running:
            now = time.time()
            next_close = math.ceil(now / gran_secs) * gran_secs
            wait = next_close - now
            self._livelog(f"Next candle close in {wait:.0f}s")

            deadline = time.time() + wait
            while time.time() < deadline and self.running:
                time.sleep(min(10, deadline - time.time()))
            if not self.running:
                break

            # Give exchange a moment to settle the candle
            time.sleep(5)

            try:
                candle = self._fetch_closed_candle(product_id, next_close, gran_secs)
            except Exception:
                self._livelog(f"Candle fetch error:\n{traceback.format_exc()}")
                time.sleep(30)
                continue

            if candle is None:
                self._livelog("Candle not available yet — retrying in 30s")
                time.sleep(30)
                continue

            self._livelog(f"Candle O:{candle['open']} H:{candle['high']} L:{candle['low']} C:{candle['close']}")

            # Update trailing orders (cancel+replace on Coinbase) before reading state
            self._update_trailing_orders(product_id, float(candle['close']))

            # Read account state from Coinbase
            try:
                self._read_account_state(product_id)
            except Exception:
                self._livelog(f"Account read error:\n{traceback.format_exc()}")

            # Update namespace arrays
            self._update_namespace_candle(candle)

            # Run indicators
            self._run_indicators(candle)

            # Call tick()
            orders = self._run_tick()
            self._livelog(f"tick() returned {len(orders)} order(s)")

            # Execute orders
            for order in orders:
                self._execute_order(order, product_id, float(candle['close']))

            self._log_event('tick', {
                'time': candle['timestamp'],
                'close': candle['close'],
                'orders': len(orders),
                'usd': self.namespace.get('usd', 0),
                'realposition': self.namespace.get('realposition', 0),
                'costbasis': self.namespace.get('costbasis', 0),
            })

    # ------------------------------------------------------------------ namespace

    def _init_namespace(self):
        self.namespace = {
            'talib': talib, 'numpy': numpy, 'Enum': Enum, 'nan': numpy.nan,
            'TradeType': util.TradeType, 'TradeOrder': util.TradeOrder,
            'calcinds': {}, 'granularity': 'ONE_HOUR', 'pair': 'btc',
            'N': self.historysize - 1,
            'opens': numpy.full(self.historysize, numpy.nan),
            'closes': numpy.full(self.historysize, numpy.nan),
            'highs': numpy.full(self.historysize, numpy.nan),
            'lows': numpy.full(self.historysize, numpy.nan),
            'volumes': numpy.full(self.historysize, numpy.nan),
            'candle': {}, 'high': 0, 'low': 0, 'open': 0, 'close': 0,
            'volume': 0, 'time': 0, 'maxpositions': 1,
            'pendingpositions': [], 'realposition': 0.0, 'costbasis': 0.0,
            'usd': 0.0, 'leverage': 10, 'makerfee': 0.0, 'takerfee': 0.0003,
            'cancel_order': self._cancel_order,
        }
        self.candle_history = []
        self._ind_history = {}

    def _update_namespace_candle(self, candle):
        self.candle_history = (self.candle_history + [candle])[-self.historysize:]
        hist = self.candle_history
        pad = self.historysize - len(hist)

        def _arr(key):
            a = numpy.array([float(c[key]) for c in hist], dtype=float)
            return numpy.pad(a, (pad, 0), constant_values=numpy.nan) if pad > 0 else a

        self.namespace.update({
            'opens': _arr('open'), 'closes': _arr('close'),
            'highs': _arr('high'), 'lows': _arr('low'), 'volumes': _arr('volume'),
            'candle': candle,
            'open': float(candle['open']), 'close': float(candle['close']),
            'high': float(candle['high']), 'low': float(candle['low']),
            'volume': float(candle['volume']), 'time': candle['timestamp'],
            'N': min(len(hist) - 1, self.historysize - 1),
        })

    # ------------------------------------------------------------------ history preload

    def _load_history(self, product_id):
        now = int(time.time())
        gran_secs = GRAN_SECONDS.get(self.granularity, 3600)
        start = now - (self.historysize + 20) * gran_secs
        candles = lutil.gethistoricledata(self.granularity, product_id, start, now)
        candles = candles[-(self.historysize):]
        for candle in candles:
            self._update_namespace_candle(candle)
            if 'indicators' in self.namespace:
                try:
                    inds = self.namespace['indicators']()
                    self._store_ind_history(candle['timestamp'], inds)
                except Exception:
                    pass
        self._rebuild_calcinds()
        self._livelog(f"Preloaded {len(candles)} candles")

    def _store_ind_history(self, ts, inds):
        if not inds:
            return
        for name, val in inds.items():
            arr = numpy.array(val, dtype=float) if hasattr(val, '__iter__') else numpy.array([val], dtype=float)
            last = float(arr[-1]) if len(arr) > 0 else numpy.nan
            self._ind_history.setdefault(name, []).append({'time': ts, 'value': last})

    def _rebuild_calcinds(self):
        calcinds = {}
        for name, entries in self._ind_history.items():
            vals = numpy.array([e['value'] for e in entries], dtype=float)
            pad = self.historysize - len(vals)
            if pad > 0:
                vals = numpy.pad(vals, (pad, 0), constant_values=numpy.nan)
            calcinds[name] = vals[-self.historysize:]
        self.namespace['calcinds'] = calcinds

    def _run_indicators(self, candle):
        if 'indicators' not in self.namespace:
            return
        try:
            inds = self.namespace['indicators']()
            self._store_ind_history(candle['timestamp'], inds)
            self._rebuild_calcinds()
        except Exception:
            self._livelog(f"Indicator error:\n{traceback.format_exc()}")

    # ------------------------------------------------------------------ tick

    def _run_tick(self):
        if 'tick' not in self.namespace:
            return []
        try:
            events = self.namespace['tick']()
            return events or []
        except Exception:
            self._livelog(f"tick() error:\n{traceback.format_exc()}")
            return []

    # ------------------------------------------------------------------ Coinbase account state

    def _read_account_state(self, product_id):
        client = lutil.getclient()
        if client is None:
            self._livelog("No Coinbase client — check credentials in Settings")
            return

        # Portfolio summary (buying power / free margin)
        try:
            ports = client.list_portfolios().to_dict().get('portfolios', [])
            intx = next((p for p in ports if p.get('type') == 'INTX'), None)
            if intx:
                port_uuid = intx['uuid']
                summary = client.get_portfolio_summary(port_uuid).to_dict()
                bp = summary.get('buying_power', {})
                self.namespace['usd'] = float(bp.get('value', 0)) if isinstance(bp, dict) else 0.0

                # Open positions
                pos_resp = client.get_portfolio_balances(port_uuid).to_dict()
                for pos in pos_resp.get('balances', []):
                    if product_id in str(pos.get('asset', '')):
                        self.namespace['realposition'] = float(pos.get('quantity', 0))
                        self.namespace['costbasis'] = float(pos.get('cost_basis', {}).get('value', 0)
                                                             if isinstance(pos.get('cost_basis'), dict)
                                                             else pos.get('cost_basis', 0))
                        self._livelog(f"Position: {self.namespace['realposition']} contracts "
                                      f"@ {self.namespace['costbasis']:.2f} | "
                                      f"Free margin: ${self.namespace['usd']:.2f}")
                        break
                else:
                    self.namespace['realposition'] = 0.0
                    self.namespace['costbasis'] = 0.0
        except Exception:
            self._livelog(f"Portfolio read error:\n{traceback.format_exc()}")

        # Open orders → pendingpositions for script
        try:
            open_orders = client.list_orders(product_id=product_id, order_status=['OPEN', 'PENDING']).to_dict()
            orders_list = open_orders.get('orders', [])

            # Reconcile liveorder table: mark anything no longer open on Coinbase as filled
            open_cb_ids = {o['order_id'] for o in orders_list if o.get('order_id')}
            tracked = lutil.runselect(
                "SELECT id, coinbase_order_id FROM liveorder WHERE scriptid=? AND status='open'",
                (self.scriptid,))
            for row in tracked:
                if row['coinbase_order_id'] not in open_cb_ids:
                    lutil.runupdate(
                        "UPDATE liveorder SET status='filled' WHERE id=?", (row['id'],))
                    self._livelog(f"Order {row['coinbase_order_id']} marked filled")

            pending = []
            for o in orders_list:
                cfg = o.get('order_configuration', {})
                limit_cfg = cfg.get('limit_limit_gtc', {})
                lp = float(limit_cfg.get('limit_price', 0) or 0)
                pending.append({
                    'id': o.get('order_id', ''),
                    'ordertype': 'Limit' if lp > 0 else 'Market',
                    'price': lp, 'amount': float(o.get('base_size', 0) or 0),
                    'stopprice': 0, 'limitprice': lp,
                    'limittrailpercent': 0, 'stoptrailpercent': 0,
                    'tradetype': 'Buy' if o.get('side') == 'BUY' else 'Sell',
                })
            self.namespace['pendingpositions'] = pending
        except Exception:
            self._livelog(f"Open orders read error:\n{traceback.format_exc()}")

    # ------------------------------------------------------------------ trailing order management

    def _update_trailing_orders(self, product_id, close_price):
        """For each tracked liveorder with a trail percent, cancel+replace if price moved enough."""
        rows = lutil.runselect(
            "SELECT * FROM liveorder WHERE scriptid=? AND status='open'", (self.scriptid,))
        client = lutil.getclient()
        if client is None or not rows:
            return

        for row in rows:
            ltp = row['limittrailpercent']
            stp = row['stoptrailpercent']
            tradetype = row['tradetype']
            updated = False
            new_limit = row['limitprice']
            new_stop = row['stopprice']

            if ltp and ltp > 0 and row['limitprice'] > 0:
                cur_limit = row['limitprice']
                if tradetype == 'Buy':
                    candidate = close_price * (1.0 - ltp)
                    if candidate > cur_limit:
                        new_limit = candidate
                        updated = True
                elif tradetype == 'Sell':
                    candidate = close_price * (1.0 + ltp)
                    if candidate < cur_limit:
                        new_limit = candidate
                        updated = True
                elif tradetype == 'Exit':
                    pos = self.namespace.get('realposition', 0)
                    activated = bool(row.get('activated', 0))
                    peak = float(row.get('peak_price', 0))
                    hard_stop = float(row.get('hard_stopprice', 0))

                    if pos > 0:
                        if not activated and close_price >= cur_limit:
                            activated = True
                            peak = close_price
                            self._livelog(f"Trailing stop activated (long) at {close_price:.2f}")
                            lutil.runupdate(
                                "UPDATE liveorder SET activated=1, peak_price=? WHERE id=?",
                                (peak, row['id']))
                        if activated:
                            if close_price > peak:
                                peak = close_price
                                lutil.runupdate("UPDATE liveorder SET peak_price=? WHERE id=?", (peak, row['id']))
                            trail_stop = peak * (1.0 - ltp)
                            if hard_stop > 0:
                                trail_stop = max(trail_stop, hard_stop)
                            if trail_stop != new_stop:
                                new_stop = trail_stop
                                updated = True
                    elif pos < 0:
                        if not activated and close_price <= cur_limit:
                            activated = True
                            peak = close_price
                            self._livelog(f"Trailing stop activated (short) at {close_price:.2f}")
                            lutil.runupdate(
                                "UPDATE liveorder SET activated=1, peak_price=? WHERE id=?",
                                (peak, row['id']))
                        if activated:
                            if peak == 0 or close_price < peak:
                                peak = close_price
                                lutil.runupdate("UPDATE liveorder SET peak_price=? WHERE id=?", (peak, row['id']))
                            trail_stop = peak * (1.0 + ltp)
                            if hard_stop > 0:
                                trail_stop = min(trail_stop, hard_stop)
                            if trail_stop != new_stop:
                                new_stop = trail_stop
                                updated = True

            if stp and stp > 0 and row['stopprice'] > 0:
                cur = row['stopprice']
                if tradetype == 'Exit':
                    pos = self.namespace.get('realposition', 0)
                    if pos > 0:
                        candidate = close_price * (1.0 - stp)
                        if candidate > cur:
                            new_stop = candidate
                            updated = True
                    elif pos < 0:
                        candidate = close_price * (1.0 + stp)
                        if candidate < cur:
                            new_stop = candidate
                            updated = True

            if updated:
                try:
                    if row['coinbase_order_id']:
                        client.cancel_orders([row['coinbase_order_id']])
                    new_cb_id = str(uuid.uuid4())
                    base_size = str(row['amount'])
                    pos = self.namespace.get('realposition', 0)
                    # Place a stop order at the new trailing stop price
                    if new_stop > 0:
                        limit_price_for_stop = round(new_stop * (0.998 if pos > 0 else 1.002), 2)
                        stop_direction = 'STOP_DIRECTION_STOP_DOWN' if pos > 0 else 'STOP_DIRECTION_STOP_UP'
                        side = 'SELL' if pos > 0 else 'BUY'
                        resp = client.create_order(
                            client_order_id=new_cb_id, product_id=product_id, side=side,
                            order_configuration={'stop_limit_stop_limit_gtc': {
                                'base_size': base_size,
                                'limit_price': str(limit_price_for_stop),
                                'stop_price': str(round(new_stop, 2)),
                                'stop_direction': stop_direction,
                            }})
                        new_cb_id = resp.to_dict().get('order_id', new_cb_id)
                    elif tradetype == 'Buy':
                        resp = client.limit_order_gtc_buy(new_cb_id, product_id, base_size, str(round(new_limit, 2)))
                        new_cb_id = resp.to_dict().get('order_id', new_cb_id)
                    elif tradetype in ('Sell', 'Exit'):
                        resp = client.limit_order_gtc_sell(new_cb_id, product_id, base_size, str(round(new_limit, 2)))
                        new_cb_id = resp.to_dict().get('order_id', new_cb_id)
                    lutil.runupdate(
                        "UPDATE liveorder SET coinbase_order_id=?, limitprice=?, stopprice=? WHERE id=?",
                        (new_cb_id, new_limit, new_stop, row['id']))
                    self._livelog(f"Trailing update [{tradetype}]: stop {row['stopprice']:.2f}→{new_stop:.2f}")
                except Exception:
                    self._livelog(f"Trailing cancel/replace error:\n{traceback.format_exc()}")

    # ------------------------------------------------------------------ cancel order

    def _cancel_order(self, order_id):
        client = lutil.getclient()
        if client:
            try:
                client.cancel_orders([order_id])
                self._livelog(f"Cancelled order {order_id}")
            except Exception:
                self._livelog(f"cancel_order error:\n{traceback.format_exc()}")
        positions = self.namespace.get('pendingpositions', [])
        self.namespace['pendingpositions'] = [p for p in positions if p['id'] != order_id]
        lutil.runupdate(
            "UPDATE liveorder SET status='cancelled' WHERE coinbase_order_id=? AND scriptid=?",
            (order_id, self.scriptid))

    # ------------------------------------------------------------------ order execution

    def _execute_order(self, trade_order, product_id, close_price):
        client = lutil.getclient()
        if client is None:
            self._livelog("No client — cannot execute order")
            return

        tradetype = trade_order.tradetype
        amount = trade_order.amount
        limitprice = trade_order.limitprice
        stopprice = trade_order.stopprice
        ltp = trade_order.limittrailpercent
        stp = trade_order.stoptrailpercent

        leverage = self.namespace.get('leverage', 10)
        realposition = self.namespace.get('realposition', 0.0)

        # Auto-size: total equity × leverage × 0.99
        if amount == 0:
            usd = self.namespace.get('usd', 0)
            costbasis = self.namespace.get('costbasis', 0)
            locked = abs(realposition) * costbasis / leverage if realposition and costbasis else 0
            upnl = (close_price - costbasis) * realposition if realposition > 0 else \
                   (costbasis - close_price) * abs(realposition) if realposition < 0 else 0
            total_eq = usd + locked + upnl
            amount_notional = total_eq * leverage * 0.99
        else:
            amount_notional = amount

        order_id = str(uuid.uuid4())

        try:
            cb_order_id = None

            if tradetype == util.TradeType.Exit:
                pos = realposition
                if pos == 0:
                    self._livelog("Exit requested but no open position")
                    return
                close_qty = abs(pos) if amount == 0 else amount
                base_size = str(round(close_qty, 8))
                if limitprice > 0:
                    if pos > 0:
                        resp = client.limit_order_gtc_sell(order_id, product_id, base_size, str(round(limitprice, 2)))
                    else:
                        resp = client.limit_order_gtc_buy(order_id, product_id, base_size, str(round(limitprice, 2)))
                    cb_order_id = resp.to_dict().get('order_id', order_id)
                    self._livelog(f"EXIT limit {'sell' if pos>0 else 'buy'} {base_size} @ {limitprice}")
                else:
                    if pos > 0:
                        resp = client.market_order_sell(order_id, product_id, base_size)
                    else:
                        resp = client.market_order_buy(order_id, product_id, base_size)
                    cb_order_id = resp.to_dict().get('order_id', order_id)
                    self._livelog(f"EXIT market {'sell' if pos>0 else 'buy'} {base_size}")

            elif tradetype == util.TradeType.Buy:
                if realposition < 0:
                    close_id = str(uuid.uuid4())
                    base_size = str(round(abs(realposition), 8))
                    client.market_order_buy(close_id, product_id, base_size)
                    self._livelog(f"BUY: closed short {base_size} at market")
                    realposition = 0.0

                if limitprice > 0:
                    base_size = str(round(amount_notional / limitprice, 8))
                    resp = client.limit_order_gtc_buy(order_id, product_id, base_size, str(round(limitprice, 2)))
                    cb_order_id = resp.to_dict().get('order_id', order_id)
                    self._livelog(f"BUY limit {base_size} @ {limitprice}")
                elif stopprice > 0:
                    base_size = str(round(amount_notional / stopprice, 8))
                    resp = client.create_order(
                        client_order_id=order_id, product_id=product_id, side='BUY',
                        order_configuration={'stop_limit_stop_limit_gtc': {
                            'base_size': base_size,
                            'limit_price': str(round(stopprice * 1.002, 2)),
                            'stop_price': str(round(stopprice, 2)),
                            'stop_direction': 'STOP_DIRECTION_STOP_UP'
                        }})
                    cb_order_id = resp.to_dict().get('order_id', order_id)
                    self._livelog(f"BUY stop {base_size} @ {stopprice}")
                else:
                    quote_size = str(round(amount_notional, 2))
                    resp = client.market_order_buy(order_id, product_id, quote_size=quote_size)
                    cb_order_id = resp.to_dict().get('order_id', order_id)
                    self._livelog(f"BUY market ${quote_size} notional")

            elif tradetype == util.TradeType.Sell:
                if realposition > 0:
                    close_id = str(uuid.uuid4())
                    base_size = str(round(realposition, 8))
                    client.market_order_sell(close_id, product_id, base_size)
                    self._livelog(f"SELL: closed long {base_size} at market")
                    realposition = 0.0

                if limitprice > 0:
                    base_size = str(round(amount_notional / limitprice, 8))
                    resp = client.limit_order_gtc_sell(order_id, product_id, base_size, str(round(limitprice, 2)))
                    cb_order_id = resp.to_dict().get('order_id', order_id)
                    self._livelog(f"SELL limit {base_size} @ {limitprice}")
                elif stopprice > 0:
                    base_size = str(round(amount_notional / stopprice, 8))
                    resp = client.create_order(
                        client_order_id=order_id, product_id=product_id, side='SELL',
                        order_configuration={'stop_limit_stop_limit_gtc': {
                            'base_size': base_size,
                            'limit_price': str(round(stopprice * 0.998, 2)),
                            'stop_price': str(round(stopprice, 2)),
                            'stop_direction': 'STOP_DIRECTION_STOP_DOWN'
                        }})
                    cb_order_id = resp.to_dict().get('order_id', order_id)
                    self._livelog(f"SELL stop {base_size} @ {stopprice}")
                else:
                    quote_size = str(round(amount_notional, 2))
                    resp = client.market_order_sell(order_id, product_id, quote_size=quote_size)
                    cb_order_id = resp.to_dict().get('order_id', order_id)
                    self._livelog(f"SELL market ${quote_size} notional")

            # Track order in DB for trailing management
            if cb_order_id and (ltp > 0 or stp > 0):
                base_size_f = float(amount_notional / (limitprice or stopprice or close_price or 1))
                lutil.runinsert(
                    "INSERT OR IGNORE INTO liveorder "
                    "(scriptid, coinbase_order_id, internal_id, tradetype, limitprice, stopprice, "
                    "amount, limittrailpercent, stoptrailpercent, status, time, "
                    "activated, peak_price, hard_stopprice) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (self.scriptid, cb_order_id, order_id, tradetype.name,
                     limitprice, stopprice, base_size_f, ltp, stp, 'open', int(time.time()),
                     0, 0.0, float(stopprice)))

            self._log_event('order', {
                'tradetype': tradetype.name, 'amount': amount,
                'limitprice': limitprice, 'stopprice': stopprice,
                'coinbase_order_id': cb_order_id,
            })

        except Exception:
            self._livelog(f"Order error:\n{traceback.format_exc()}")

    # ------------------------------------------------------------------ candle fetch

    def _fetch_closed_candle(self, product_id, close_time, gran_secs):
        client = lutil.getclient()
        candle_open_ts = int(close_time) - gran_secs
        start = candle_open_ts - gran_secs
        end = int(close_time) + gran_secs

        resp = client.get_candles(product_id, start=str(start), end=str(end),
                                  granularity=self.granularity)
        candles = resp.to_dict().get('candles', [])
        best = min(candles, key=lambda c: abs(int(c.get('start', 0)) - candle_open_ts), default=None)
        if best is None or abs(int(best.get('start', 0)) - candle_open_ts) > gran_secs:
            return None

        # Persist to candle table
        try:
            cid = lutil.runinsert(
                "INSERT INTO candle (pair, open, close, high, low, volume, timestamp, duration) "
                "VALUES(?,?,?,?,?,?,?,?)",
                (product_id, best['open'], best['close'], best['high'],
                 best['low'], best['volume'], best['start'], self.granularity))
        except Exception:
            row = lutil.runselect("SELECT id FROM candle WHERE pair=? AND timestamp=? AND duration=?",
                                  (product_id, best['start'], self.granularity))
            cid = row[0]['id'] if row else None

        return {
            'id': cid, 'timestamp': int(best['start']),
            'open': float(best['open']), 'high': float(best['high']),
            'low': float(best['low']), 'close': float(best['close']),
            'volume': float(best['volume']),
        }

    # ------------------------------------------------------------------ status

    def get_status(self):
        close = self.namespace.get('close', 0)
        usd = self.namespace.get('usd', 0)
        pos = self.namespace.get('realposition', 0)
        cb = self.namespace.get('costbasis', 0)
        lev = self.namespace.get('leverage', 10)
        locked = abs(pos) * cb / lev if pos and cb else 0
        upnl = (close - cb) * pos if pos > 0 else (cb - close) * abs(pos) if pos < 0 else 0
        return {
            'running': self.running,
            'scriptid': self.scriptid,
            'pair': self.pair,
            'granularity': self.granularity,
            'usd': round(usd, 2),
            'realposition': pos,
            'costbasis': round(cb, 2),
            'close': close,
            'unrealized_pnl': round(upnl, 2),
            'total_equity': round(usd + locked + upnl, 2),
            'leverage': lev,
            'log': (lutil.getkeyval('live_log') or '').split('\n')[-100:],
        }


# ------------------------------------------------------------------ module-level singleton

_trader: LiveTrader = None
_lock = threading.Lock()


def get_trader():
    return _trader


def start_trader(scriptid: int) -> LiveTrader:
    global _trader
    with _lock:
        if _trader and _trader.running:
            _trader.stop()
            time.sleep(1)
        _trader = LiveTrader(scriptid)
        _trader.start()
        return _trader


def stop_trader():
    global _trader
    with _lock:
        if _trader:
            _trader.stop()


def maybe_autoresume():
    """Called at server startup — resume if a trader was running before crash."""
    running = lutil.getkeyval('live_running')
    scriptid = lutil.getkeyval('live_scriptid')
    if running == 'true' and scriptid:
        print(f"[LIVE] Auto-resuming script {scriptid} after restart")
        start_trader(int(scriptid))
