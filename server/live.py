import util
from coinbase_http import CoinbaseHTTP
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
        self.historysize = 1000
        self.pair = 'btc'
        self.granularity = 'ONE_HOUR'
        self.candle_history = []
        self._ind_history = {}
        self._max_base_size = None
        self._min_base_size = None
        self._base_increment = None
        self._contract_size = None
        self._product_venue = None  # 'FCM'/'CDE' (base_size = contract count) vs 'INTX' (base_size = base asset qty)

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
        try:
            import ntfy_util
            ntfy_util.send_notification(event_type, data)
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
                    for _ in range(60):
                        if not self.running:
                            break
                        time.sleep(1)

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

        product_id = (self.namespace.get('Product_ID') or '').strip()
        if not product_id:
            # Legacy fallback: scripts still using pair="btc"
            legacy = self.namespace.get('pair')
            if legacy:
                product_id = legacy.upper() + '-PERP-INTX'
        self.pair = product_id  # kept for downstream code that referenced self.pair
        self.granularity = self.namespace.get('granularity', 'ONE_HOUR')
        gran_secs = GRAN_SECONDS.get(self.granularity, 3600)

        if not product_id:
            self._livelog("Product_ID not set in script — stopping. "
                          "Set Product_ID='BIP-20DEC30-CDE' or similar (use 'Browse Products' on the backtest page).")
            self.running = False
            return

        lutil.setkeyval('live_pair', product_id)
        lutil.setkeyval('live_granularity', self.granularity)
        self._livelog(f"Product_ID: {product_id} | Granularity: {self.granularity}")

        # Read account data first so the UI is populated immediately
        self._load_product_limits(product_id)
        self._read_account_state(product_id)
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

            # Update trailing orders before reading state
            self._update_trailing_orders(product_id, float(candle['close']))

            try:
                self._read_account_state(product_id)
            except Exception:
                self._livelog(f"Account read error:\n{traceback.format_exc()}")

            self._update_namespace_candle(candle)
            self._run_indicators(candle)
            orders = self._run_tick()
            self._livelog(f"tick() returned {len(orders)} order(s)")

            for order in orders:
                self._log_event('user:' + order.tradetype.name, {
                    'tradetype': order.tradetype.name,
                    'amount': order.amount,
                    'limitprice': order.limitprice,
                    'stopprice': order.stopprice,
                })
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
            'simlog': self._livelog,
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
        ind_errors = 0
        first_ind_error = None
        for candle in candles:
            self._update_namespace_candle(candle)
            if 'indicators' in self.namespace:
                try:
                    inds = self.namespace['indicators']()
                    self._store_ind_history(candle['timestamp'], inds)
                except Exception as e:
                    ind_errors += 1
                    if first_ind_error is None:
                        first_ind_error = traceback.format_exc()
        self._rebuild_calcinds()
        ind_count = sum(len(v) for v in self._ind_history.values())
        self._livelog(
            f"Preloaded {len(candles)} candles | "
            f"indicators: {list(self._ind_history.keys())} ({ind_count} points, {ind_errors} errors)"
        )
        if first_ind_error:
            self._livelog(f"First indicator error:\n{first_ind_error}")

    def _store_ind_history(self, ts, inds):
        if not inds:
            return
        for name, val in inds.items():
            if not hasattr(val, '__iter__'):
                val = [val]
            arr = numpy.array(val, dtype=float)
            flat = arr.flatten()
            last = float(flat[-1]) if len(flat) > 0 else numpy.nan
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
        cb = CoinbaseHTTP()

        # Balance summary — derive free margin from atomic fields (total - initial - holds)
        # because Coinbase's `available_margin` has been observed to lag for INTX-opened
        # positions. Fall back to available_margin / buying_power if the math yields 0.
        try:
            data = cb.get_balance_summary()
            bal = data.get('balance_summary', {})

            def _amount(key):
                v = bal.get(key, {})
                return float(v.get('value', 0) if isinstance(v, dict) else v or 0)

            total          = _amount('total_usd_balance')
            initial_margin = _amount('initial_margin')
            hold           = _amount('total_open_orders_hold_amount')
            available      = _amount('available_margin')
            buying_power   = _amount('futures_buying_power')
            unrealized     = _amount('unrealized_pnl')

            usd_computed = total - initial_margin - hold
            if usd_computed > 0:
                self.namespace['usd'] = usd_computed
            elif available > 0:
                self.namespace['usd'] = available
            else:
                self.namespace['usd'] = buying_power

            # Store Coinbase-reported values so get_status() can return them
            # directly instead of recomputing (the local locked/upnl math used
            # the script's leverage and ignored contract_size — wildly wrong).
            self.namespace['unrealized_pnl'] = unrealized
            self.namespace['total_equity'] = total
            self.namespace['initial_margin'] = initial_margin

            self._livelog(
                f"Free margin: ${self.namespace['usd']:.2f} "
                f"(total ${total:.2f} − initial_margin ${initial_margin:.2f} − holds ${hold:.2f}) | "
                f"Coinbase available_margin: ${available:.2f} | Unrealized PnL: ${unrealized:.2f}"
            )
        except Exception:
            self._livelog(f"Futures balance read error:\n{traceback.format_exc()}")

        # Open position
        try:
            pos_resp = cb.get_position(product_id)
            pos = pos_resp.get('position') or {}
            if not pos or not pos.get('number_of_contracts'):
                self.namespace['realposition'] = 0.0
                self.namespace['costbasis'] = 0.0
                self._livelog(f"No open position | Free margin: ${self.namespace['usd']:.2f}")
            else:
                contracts = float(pos.get('number_of_contracts', 0) or 0)
                side = pos.get('side', '')
                if side == 'SHORT':
                    contracts = -contracts
                avg_entry = float(pos.get('avg_entry_price', 0) or 0)
                self.namespace['realposition'] = contracts
                self.namespace['costbasis'] = avg_entry
                self._livelog(
                    f"Position: {contracts} contracts @ {avg_entry:.2f} | "
                    f"Free margin: ${self.namespace['usd']:.2f}"
                )
        except Exception:
            self._livelog(f"Position read error:\n{traceback.format_exc()}")
            self.namespace['realposition'] = 0.0
            self.namespace['costbasis'] = 0.0

        # Open orders → pendingpositions
        try:
            open_orders = cb.list_orders(product_id=product_id, order_status=['OPEN'])
            orders_list = open_orders.get('orders', [])

            # Reconcile every tracked open order by asking Coinbase for its current
            # state. This distinguishes fills, cancels, expirations, and failures
            # (the old "missing from OPEN list = filled" assumption misclassified
            # exchange-side cancels and expirations as fills).
            tracked = lutil.runselect(
                "SELECT id, coinbase_order_id, tradetype, amount, limitprice, stopprice "
                "FROM liveorder WHERE scriptid=? AND status='open'",
                (self.scriptid,))
            for row in tracked:
                cb_id = row['coinbase_order_id']
                if not cb_id:
                    continue
                try:
                    order = cb.get_order(cb_id).get('order', {}) or {}
                except Exception:
                    self._livelog(f"get_order failed for {cb_id}:\n{traceback.format_exc()}")
                    continue
                status = (order.get('status') or '').upper()
                if status in ('OPEN', 'PENDING', 'QUEUED', ''):
                    continue  # still working — leave the DB row alone
                tt = row['tradetype'] or 'order'
                evt = {
                    'coinbase_order_id': cb_id,
                    'tradetype': tt,
                    'amount': row['amount'] or 0,
                    'limitprice': row['limitprice'] or 0,
                    'stopprice': row['stopprice'] or 0,
                    'status': status,
                    'filled_size': float(order.get('filled_size', 0) or 0),
                    'average_filled_price': float(order.get('average_filled_price', 0) or 0),
                    'completion_percentage': float(order.get('completion_percentage', 0) or 0),
                }
                if status == 'FILLED':
                    lutil.runupdate("UPDATE liveorder SET status='filled' WHERE id=?", (row['id'],))
                    self._livelog(
                        f"Order {cb_id} FILLED {evt['filled_size']} @ {evt['average_filled_price']:.2f}"
                    )
                    self._log_event('fill:' + tt, evt)
                elif status in ('CANCELLED', 'EXPIRED'):
                    lutil.runupdate("UPDATE liveorder SET status='cancelled' WHERE id=?", (row['id'],))
                    self._livelog(f"Order {cb_id} {status} on exchange (filled {evt['filled_size']})")
                    self._log_event('cancel:' + tt, evt)
                elif status == 'FAILED':
                    lutil.runupdate("UPDATE liveorder SET status='failed' WHERE id=?", (row['id'],))
                    self._livelog(f"Order {cb_id} FAILED on exchange")
                    self._log_event('fail:' + tt, evt)
                else:
                    self._livelog(f"Order {cb_id} unknown status '{status}' — leaving open")

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
        if not rows:
            return
        cb = CoinbaseHTTP()

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
                # Step 1: cancel existing order — abort replacement if cancel fails
                if row['coinbase_order_id']:
                    try:
                        cb.cancel_orders([row['coinbase_order_id']])
                    except Exception:
                        self._livelog(f"Trailing cancel failed for {row['coinbase_order_id']} — skipping replace:\n{traceback.format_exc()}")
                        continue

                # Step 2: place replacement order
                try:
                    new_cb_id = str(uuid.uuid4())
                    base_size = self._format_base_size(self._round_to_increment(float(row['amount'])))
                    pos = self.namespace.get('realposition', 0)
                    if new_stop > 0:
                        limit_price_for_stop = round(new_stop * (0.999 if pos > 0 else 1.001), 2)
                        stop_direction = 'STOP_DIRECTION_STOP_DOWN' if pos > 0 else 'STOP_DIRECTION_STOP_UP'
                        side = 'SELL' if pos > 0 else 'BUY'
                        intent = f"TRAILING {side} stop {base_size} @ {new_stop:.2f}"
                        resp = self._cb_create_order(cb, new_cb_id, product_id, side, {
                            'stop_limit_stop_limit_gtc': {
                                'base_size': base_size,
                                'limit_price': str(limit_price_for_stop),
                                'stop_price': str(round(new_stop, 2)),
                                'stop_direction': stop_direction,
                            }
                        })
                        if not self._check_order_response(resp, intent): continue
                        new_cb_id = self._get_cb_order_id(resp, new_cb_id, product_id) or new_cb_id
                    elif tradetype == 'Buy':
                        intent = f"TRAILING BUY limit {base_size} @ {new_limit:.2f}"
                        resp = self._cb_create_order(cb, new_cb_id, product_id, 'BUY', {
                            'limit_limit_gtc': {'base_size': base_size, 'limit_price': str(round(new_limit, 2))}
                        })
                        if not self._check_order_response(resp, intent): continue
                        new_cb_id = self._get_cb_order_id(resp, new_cb_id, product_id) or new_cb_id
                    elif tradetype in ('Sell', 'Exit'):
                        intent = f"TRAILING SELL limit {base_size} @ {new_limit:.2f}"
                        resp = self._cb_create_order(cb, new_cb_id, product_id, 'SELL', {
                            'limit_limit_gtc': {'base_size': base_size, 'limit_price': str(round(new_limit, 2))}
                        })
                        if not self._check_order_response(resp, intent): continue
                        new_cb_id = self._get_cb_order_id(resp, new_cb_id, product_id) or new_cb_id
                    lutil.runupdate(
                        "UPDATE liveorder SET coinbase_order_id=?, limitprice=?, stopprice=? WHERE id=?",
                        (new_cb_id, new_limit, new_stop, row['id']))
                    self._livelog(f"Trailing update [{tradetype}]: stop {row['stopprice']:.2f}→{new_stop:.2f}")
                except Exception:
                    self._livelog(f"Trailing replace error:\n{traceback.format_exc()}")

    # ------------------------------------------------------------------ product limits

    def _load_product_limits(self, product_id):
        from coinbase_http import KNOWN_CONTRACT_SIZES
        cb = CoinbaseHTTP()
        try:
            product = cb.get_product(product_id)
            # Refresh the cache row from this same fetch so the rest of the app sees fresh data.
            try:
                if isinstance(product, dict) and product.get('product_id'):
                    lutil.upsert_futures_product(product)
            except Exception:
                pass

            self._max_base_size = float(product.get('base_max_size') or 0) or None
            self._min_base_size = float(product.get('base_min_size') or 0) or None
            self._base_increment = float(product.get('base_increment') or 0) or None
            self._product_venue = (product.get('product_venue') or '').upper()

            future_details = product.get('future_product_details') or {}
            api_contract_size = float(future_details.get('contract_size') or 0)
            perp_details = future_details.get('perpetual_details') or {}
            max_leverage = float(perp_details.get('max_leverage') or 0)

            # INTX: API's contract_size field is unreliable post-merger — use hardcoded specs.
            # FCM/CDE: API's contract_size is correct (e.g. 0.01 BTC for BIP-20DEC30-CDE).
            known = KNOWN_CONTRACT_SIZES.get(product_id)
            if self._product_venue == 'INTX' and known is not None:
                self._contract_size = known
                if known != api_contract_size:
                    self._livelog(
                        f"contract_size: API returned {api_contract_size}, "
                        f"using hardcoded {known} for {product_id}"
                    )
            else:
                self._contract_size = api_contract_size if api_contract_size > 0 else (known or None)

            self._livelog(
                f"Product limits — venue:{self._product_venue} min:{self._min_base_size} "
                f"max:{self._max_base_size} increment:{self._base_increment} | "
                f"contract_size:{self._contract_size} max_leverage:{max_leverage}x"
            )
        except Exception:
            self._livelog(f"Could not load product limits:\n{traceback.format_exc()}")

    def _round_to_increment(self, size: float) -> float:
        """Round a base-asset (BTC) quantity DOWN to the nearest whole contract.
        Whole-contract granularity = contract_size, regardless of venue (the base_size
        on the wire differs by venue but the underlying-asset granularity is the same)."""
        cs = self._contract_size or 0
        if cs <= 0:
            return round(size, 8)
        factor = round(1 / cs)
        contracts = math.floor(abs(size) * factor)
        return math.copysign(contracts / factor, size) if size else 0.0

    def _format_base_size(self, btc_qty: float) -> str:
        """Convert a (signed) base-asset quantity into the venue-correct base_size string.
        - FCM/CDE: base_size is integer CONTRACT COUNT (1, 2, 3 ...)
        - INTX:    base_size is BASE-ASSET amount (e.g. '0.01' BTC = 1 contract)
        Caller is responsible for sign; this returns a non-negative string.
        """
        cs = self._contract_size or 0.01
        qty = abs(btc_qty)
        if (self._product_venue or '').upper() in ('FCM', 'CDE'):
            contracts = int(round(qty / cs)) if cs > 0 else int(qty)
            return str(contracts)
        # INTX (or unknown): wire format is base-asset amount
        return str(round(qty, 8))

    def _cb_create_order(self, cb, client_order_id, product_id, side, order_configuration):
        """All perp orders go through here so leverage + margin_type are always
        set. Per Coinbase docs:
          - leverage defaults to "1.0" if omitted (wrong for perp strategies).
          - margin_type defaults to CROSS, explicit is safer.
          - retail_portfolio_id is deprecated for CDP keys, do NOT send."""
        leverage = self.namespace.get('leverage', 1)
        try:
            lev_str = f"{float(leverage):.1f}"
        except Exception:
            lev_str = str(leverage)
        return cb.create_order(
            client_order_id, product_id, side, order_configuration,
            leverage=lev_str, margin_type='CROSS',
        )

    def _cap_base_size(self, base_size_f):
        base_size_f = self._round_to_increment(base_size_f)
        if self._max_base_size and base_size_f > self._max_base_size:
            self._livelog(f"base_size {base_size_f} capped to max {self._max_base_size}")
            base_size_f = self._round_to_increment(self._max_base_size)
        if self._min_base_size and base_size_f < self._min_base_size:
            self._livelog(f"base_size {base_size_f} below min {self._min_base_size} — order skipped")
            return 0.0
        return base_size_f

    def _check_order_response(self, resp, intent_label):
        """Inspect a Coinbase create_order response. On rejection, log a clear
        error line and fire an `error:order_rejected` event (for push). Returns
        True on success, False on failure — callers should bail on False so the
        success-style log and `create:*` event do not fire for a phantom order."""
        if not isinstance(resp, dict):
            self._notify_order_error(intent_label, 'INVALID_RESPONSE', str(resp)[:300])
            return False
        # API-level error (auth, permission, malformed request) — top-level
        # 'error' key with no success_response.
        if resp.get('error') and not resp.get('success_response'):
            reason = resp.get('error') or 'UNKNOWN'
            message = resp.get('message') or resp.get('error_details') or ''
            self._notify_order_error(intent_label, reason, message)
            return False
        # Order-level rejection — success: false with error_response populated.
        if resp.get('success') is False:
            err = resp.get('error_response') or {}
            reason = (err.get('error')
                      or resp.get('failure_reason')
                      or err.get('preview_failure_reason')
                      or 'UNKNOWN')
            message = err.get('message') or err.get('error_details') or ''
            self._notify_order_error(intent_label, reason, message)
            return False
        return True

    def _notify_order_error(self, intent_label, reason, message):
        self._livelog(f"ORDER REJECTED [{reason}] {intent_label}: {message}")
        self._log_event('error:order_rejected', {
            'intent': intent_label, 'reason': reason, 'message': message,
        })

    def _get_cb_order_id(self, resp, client_order_id, product_id):
        """Extract Coinbase order_id from response dict; fall back to open-order lookup."""
        cb_id = resp.get('success_response', {}).get('order_id') or resp.get('order_id')
        if cb_id:
            return cb_id
        try:
            cb = CoinbaseHTTP()
            recent = cb.list_orders(product_id=product_id, order_status=['OPEN'])
            for o in recent.get('orders', []):
                if o.get('client_order_id') == client_order_id:
                    return o.get('order_id')
        except Exception:
            pass
        self._livelog(
            f"Warning: could not resolve Coinbase order_id for client_order_id {client_order_id}. "
            f"Exchange response: {json.dumps(resp)[:800]}"
        )
        return None

    # ------------------------------------------------------------------ cancel order

    def _cancel_order(self, order_id):
        cb = CoinbaseHTTP()
        row = lutil.runselect(
            "SELECT tradetype FROM liveorder WHERE coinbase_order_id=? AND scriptid=?",
            (order_id, self.scriptid))
        tradetype_name = row[0]['tradetype'] if row else 'order'
        try:
            cb.cancel_orders([order_id])
            self._livelog(f"Cancelled order {order_id}")
        except Exception:
            self._livelog(f"cancel_order error:\n{traceback.format_exc()}")
        positions = self.namespace.get('pendingpositions', [])
        self.namespace['pendingpositions'] = [p for p in positions if p['id'] != order_id]
        lutil.runupdate(
            "UPDATE liveorder SET status='cancelled' WHERE coinbase_order_id=? AND scriptid=?",
            (order_id, self.scriptid))
        self._log_event('cancel:' + tradetype_name, {'coinbase_order_id': order_id})

    # ------------------------------------------------------------------ order execution

    def _execute_order(self, trade_order, product_id, close_price):
        cb = CoinbaseHTTP()

        tradetype = trade_order.tradetype
        amount = trade_order.amount
        limitprice = trade_order.limitprice
        stopprice = trade_order.stopprice
        ltp = trade_order.limittrailpercent
        stp = trade_order.stoptrailpercent

        leverage = self.namespace.get('leverage', 10)
        realposition = self.namespace.get('realposition', 0.0)

        # Auto-size: total equity × leverage × 0.99 (use Coinbase-reported
        # equity so we don't recompute locked/upnl with wrong contract_size).
        if amount == 0:
            usd = self.namespace.get('usd', 0)
            upnl = self.namespace.get('unrealized_pnl', 0.0)
            total_eq = self.namespace.get('total_equity', 0.0) or (usd + upnl)
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
                close_qty = abs(pos) if amount == 0 else min(amount, abs(pos))
                close_qty = self._round_to_increment(close_qty)
                if close_qty <= 0:
                    self._livelog(f"Exit skipped — close_qty {close_qty} below whole-contract granularity")
                    return
                base_size = self._format_base_size(close_qty)
                if limitprice > 0:
                    side = 'SELL' if pos > 0 else 'BUY'
                    intent = f"EXIT limit {'sell' if pos>0 else 'buy'} {base_size} @ {limitprice}"
                    resp = self._cb_create_order(cb, order_id, product_id, side, {
                        'limit_limit_gtc': {'base_size': base_size, 'limit_price': str(round(limitprice, 2))}
                    })
                    if not self._check_order_response(resp, intent): return
                    cb_order_id = self._get_cb_order_id(resp, order_id, product_id)
                    self._livelog(intent)
                else:
                    side = 'SELL' if pos > 0 else 'BUY'
                    intent = f"EXIT market {'sell' if pos>0 else 'buy'} {base_size}"
                    resp = self._cb_create_order(cb, order_id, product_id, side, {
                        'market_market_ioc': {'base_size': base_size}
                    })
                    if not self._check_order_response(resp, intent): return
                    cb_order_id = self._get_cb_order_id(resp, order_id, product_id)
                    self._livelog(intent)

            elif tradetype == util.TradeType.Buy:
                if realposition < 0:
                    close_id = str(uuid.uuid4())
                    base_size = self._format_base_size(self._round_to_increment(abs(realposition)))
                    intent = f"BUY: close short {base_size} at market"
                    resp = self._cb_create_order(cb, close_id, product_id, 'BUY', {
                        'market_market_ioc': {'base_size': base_size}
                    })
                    if not self._check_order_response(resp, intent): return
                    self._livelog(f"BUY: closed short {base_size} at market")
                    realposition = 0.0

                if limitprice > 0:
                    bs = self._cap_base_size(round(amount_notional / limitprice, 8))
                    if bs <= 0: return
                    base_size = self._format_base_size(bs)
                    intent = f"BUY limit {base_size} @ {limitprice}"
                    resp = self._cb_create_order(cb, order_id, product_id, 'BUY', {
                        'limit_limit_gtc': {'base_size': base_size, 'limit_price': str(round(limitprice, 2))}
                    })
                    if not self._check_order_response(resp, intent): return
                    cb_order_id = self._get_cb_order_id(resp, order_id, product_id)
                    self._livelog(intent)
                elif stopprice > 0:
                    bs = self._cap_base_size(round(amount_notional / stopprice, 8))
                    if bs <= 0: return
                    base_size = self._format_base_size(bs)
                    intent = f"BUY stop {base_size} @ {stopprice}"
                    resp = self._cb_create_order(cb, order_id, product_id, 'BUY', {
                        'stop_limit_stop_limit_gtc': {
                            'base_size': base_size,
                            'limit_price': str(round(stopprice * 1.001, 2)),
                            'stop_price': str(round(stopprice, 2)),
                            'stop_direction': 'STOP_DIRECTION_STOP_UP',
                        }
                    })
                    if not self._check_order_response(resp, intent): return
                    cb_order_id = self._get_cb_order_id(resp, order_id, product_id)
                    self._livelog(intent)
                else:
                    bs = self._cap_base_size(round(amount_notional / close_price, 8))
                    if bs <= 0: return
                    base_size = self._format_base_size(bs)
                    intent = f"BUY market {base_size} contracts"
                    resp = self._cb_create_order(cb, order_id, product_id, 'BUY', {
                        'market_market_ioc': {'base_size': base_size}
                    })
                    if not self._check_order_response(resp, intent): return
                    cb_order_id = self._get_cb_order_id(resp, order_id, product_id)
                    self._livelog(intent)

            elif tradetype == util.TradeType.Sell:
                if realposition > 0:
                    close_id = str(uuid.uuid4())
                    base_size = self._format_base_size(self._round_to_increment(realposition))
                    intent = f"SELL: close long {base_size} at market"
                    resp = self._cb_create_order(cb, close_id, product_id, 'SELL', {
                        'market_market_ioc': {'base_size': base_size}
                    })
                    if not self._check_order_response(resp, intent): return
                    self._livelog(f"SELL: closed long {base_size} at market")
                    realposition = 0.0

                if limitprice > 0:
                    bs = self._cap_base_size(round(amount_notional / limitprice, 8))
                    if bs <= 0: return
                    base_size = self._format_base_size(bs)
                    intent = f"SELL limit {base_size} @ {limitprice}"
                    resp = self._cb_create_order(cb, order_id, product_id, 'SELL', {
                        'limit_limit_gtc': {'base_size': base_size, 'limit_price': str(round(limitprice, 2))}
                    })
                    if not self._check_order_response(resp, intent): return
                    cb_order_id = self._get_cb_order_id(resp, order_id, product_id)
                    self._livelog(intent)
                elif stopprice > 0:
                    bs = self._cap_base_size(round(amount_notional / stopprice, 8))
                    if bs <= 0: return
                    base_size = self._format_base_size(bs)
                    intent = f"SELL stop {base_size} @ {stopprice}"
                    resp = self._cb_create_order(cb, order_id, product_id, 'SELL', {
                        'stop_limit_stop_limit_gtc': {
                            'base_size': base_size,
                            'limit_price': str(round(stopprice * 0.999, 2)),
                            'stop_price': str(round(stopprice, 2)),
                            'stop_direction': 'STOP_DIRECTION_STOP_DOWN',
                        }
                    })
                    if not self._check_order_response(resp, intent): return
                    cb_order_id = self._get_cb_order_id(resp, order_id, product_id)
                    self._livelog(intent)
                else:
                    bs = self._cap_base_size(round(amount_notional / close_price, 8))
                    if bs <= 0: return
                    base_size = self._format_base_size(bs)
                    intent = f"SELL market {base_size} contracts"
                    resp = self._cb_create_order(cb, order_id, product_id, 'SELL', {
                        'market_market_ioc': {'base_size': base_size}
                    })
                    if not self._check_order_response(resp, intent): return
                    cb_order_id = self._get_cb_order_id(resp, order_id, product_id)
                    self._livelog(intent)

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

            self._log_event('create:' + tradetype.name, {
                'tradetype': tradetype.name, 'amount': amount,
                'limitprice': limitprice, 'stopprice': stopprice,
                'coinbase_order_id': cb_order_id,
            })

        except Exception:
            self._livelog(f"Order error:\n{traceback.format_exc()}")

    # ------------------------------------------------------------------ candle fetch

    def _fetch_closed_candle(self, product_id, close_time, gran_secs):
        cb = CoinbaseHTTP()
        candle_open_ts = int(close_time) - gran_secs
        start = candle_open_ts - gran_secs
        end = int(close_time) + gran_secs

        resp = cb.get_candles(product_id, start=str(start), end=str(end), granularity=self.granularity)
        candles = resp.get('candles', [])
        best = min(candles, key=lambda c: abs(int(c.get('start', 0)) - candle_open_ts), default=None)
        if best is None or abs(int(best.get('start', 0)) - candle_open_ts) > gran_secs:
            return None

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
        # Prefer Coinbase-reported equity/PnL — the local locked/upnl math
        # used script leverage and ignored contract_size, which is wrong on
        # INTX (e.g. BTC contract = 0.01 BTC, max 3.3x not 10x).
        upnl = self.namespace.get('unrealized_pnl', 0.0)
        total_equity = self.namespace.get('total_equity', 0.0) or (usd + upnl)
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
            'total_equity': round(total_equity, 2),
            'initial_margin': round(self.namespace.get('initial_margin', 0.0), 2),
            'leverage': lev,
            'contract_size': self._contract_size,
            'base_increment': self._base_increment,
            'last_tick_time': int(self.namespace.get('time', 0)),
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
        sep = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC") + ": ── session started ──"
        existing = lutil.getkeyval('live_log') or ''
        lines = existing.split('\n') if existing else []
        lines.append(sep)
        lutil.setkeyval('live_log', '\n'.join(lines[-500:]))
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
