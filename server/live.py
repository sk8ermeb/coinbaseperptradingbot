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
        # Serializes trail updates triggered from the API price-poll thread
        # so they don't race with each other (or with any internal callers).
        self._trail_lock = threading.Lock()
        # Order ids cancelled by the trail loop as part of a reposition.
        # The WS `cancel` event and the reconciliation `cancel:<tt>` event
        # consult this set so trail churn doesn't spam push notifications.
        self._trail_canceling_ids = set()
        # Latest market mid cached by the backend's 5s poll. The /api/live/price
        # endpoint reads from here so the frontend doesn't have to hit Coinbase
        # itself when the trader thread is already polling.
        self._last_price = 0.0
        self._last_price_time = 0.0
        self.granularity = 'ONE_HOUR'
        self.candle_history = []
        self._ind_history = {}
        self._max_base_size = None
        self._min_base_size = None
        self._base_increment = None
        self._price_increment = None  # price tick (from API price_increment, e.g. 5.0 for CDE BTC futures)
        self._contract_size = None
        self._max_leverage = None   # exchange-side cap from get_product (INTX) or KNOWN_MAX_LEVERAGES; None if unknown
        self._product_venue = None  # 'FCM'/'CDE' (base_size = contract count) vs 'INTX' (base_size = base asset qty)
        self._base_currency = ''    # e.g. 'BTC' — for the status panel's "Base" field
        self._ws_client = None
        self._ws_product_id = None
        self._seen_order_states = {}  # order_id -> last status (dedupes repeat WS messages)

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
        self._stop_ws()
        self._livelog("Live trader stopped by user")

    # ------------------------------------------------------------------ logging

    def _livelog(self, msg):
        ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
        entry = f"{ts}: {msg}"
        print(f"[LIVE] {entry}")
        # Scope the log per-script so switching algorithms doesn't bleed history
        key = f'live_log_{self.scriptid}'
        existing = lutil.getkeyval(key) or ''
        lines = existing.split('\n') if existing else []
        lines.append(entry)
        lutil.setkeyval(key, '\n'.join(lines[-500:]))

    def _log_event(self, event_type, data, notify=True):
        try:
            lutil.runinsert(
                "INSERT INTO liveevent (scriptid, eventtype, eventdata, time) VALUES(?,?,?,?)",
                (self.scriptid, event_type, json.dumps(data), int(time.time()))
            )
        except Exception:
            pass
        if not notify:
            return
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
        self._start_ws(product_id)
        self._read_account_state(product_id)
        self._load_history(product_id)
        self._livelog("History loaded — waiting for next candle close")

        while self.running:
            now = time.time()
            next_close = math.ceil(now / gran_secs) * gran_secs
            wait = next_close - now
            self._livelog(f"Next candle close in {wait:.0f}s")

            # Inter-candle wait. While we sleep, poll the ticker on a 5s
            # cadence and run trailing-stop updates against fresh market
            # prices — Coinbase has no native trailing support, so this
            # thread is what keeps the trail moving between candle closes
            # (and it runs whether or not anyone has the trading page open).
            # self.running flips False on stop(), so the loop exits within
            # one chunk.
            deadline = time.time() + wait
            poll_interval = 5
            next_poll = time.time()
            while time.time() < deadline and self.running:
                now = time.time()
                if now >= next_poll:
                    self._poll_market_tick(product_id)
                    next_poll = time.time() + poll_interval
                chunk = min(poll_interval, deadline - time.time())
                if chunk > 0:
                    time.sleep(chunk)
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
                    'limittrailpercent': order.limittrailpercent,
                    'stoptrailpercent': order.stoptrailpercent,
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

    def refresh_balance_position(self, product_id, silent=False):
        """Lightweight refresh: balance_summary + position only. No order
        reconciliation, no DB writes. Safe to call on every status poll.
        Set silent=True to skip the live log lines (avoids spam at fast cadence)."""
        cb = CoinbaseHTTP()

        # Balance summary — `futures_buying_power` is the actual free margin
        # the Coinbase UI shows ("what you can open new positions with").
        # `available_margin` sounds right but empirically returns ~total equity
        # on FCM/CDE accounts (e.g. $295 when free is $96) — DO NOT use it as
        # the primary source. Order of preference: futures_buying_power →
        # available_margin → derived (total − initial − hold).
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
            daily_realized = _amount('daily_realized_pnl')

            usd_computed = total - initial_margin - hold
            if buying_power > 0:
                self.namespace['usd'] = buying_power
            elif available > 0:
                self.namespace['usd'] = available
            else:
                self.namespace['usd'] = max(usd_computed, 0)

            # Store Coinbase-reported values so get_status() can return them
            # directly instead of recomputing (the local locked/upnl math used
            # the script's leverage and ignored contract_size — wildly wrong).
            # total_usd_balance is a static snapshot of (spot + futures pool)
            # cash; it doesn't include today's realized P&L until end-of-day
            # settlement. Add daily_realized_pnl + unrealized_pnl so the
            # displayed equity reflects mark-to-market reality immediately.
            self.namespace['unrealized_pnl'] = unrealized
            self.namespace['daily_realized_pnl'] = daily_realized
            self.namespace['total_equity'] = total + daily_realized + unrealized
            self.namespace['initial_margin'] = initial_margin

            if not silent:
                self._livelog(
                    f"Free margin: ${self.namespace['usd']:.2f} "
                    f"(futures_buying_power ${buying_power:.2f}; available_margin ${available:.2f}; "
                    f"derived total ${total:.2f} − initial_margin ${initial_margin:.2f} − holds ${hold:.2f} = ${usd_computed:.2f}) "
                    f"| Unrealized PnL: ${unrealized:.2f} | Daily Realized PnL: ${daily_realized:.2f}"
                )
        except Exception:
            if not silent:
                self._livelog(f"Futures balance read error:\n{traceback.format_exc()}")

        # Open position
        try:
            pos_resp = cb.get_position(product_id)
            pos = pos_resp.get('position') or {}
            if not pos or not pos.get('number_of_contracts'):
                self.namespace['realposition'] = 0.0
                self.namespace['costbasis'] = 0.0
                if not silent:
                    self._livelog(f"No open position | Free margin: ${self.namespace['usd']:.2f}")
            else:
                contracts = float(pos.get('number_of_contracts', 0) or 0)
                side = pos.get('side', '')
                if side == 'SHORT':
                    contracts = -contracts
                avg_entry = float(pos.get('avg_entry_price', 0) or 0)
                self.namespace['realposition'] = contracts
                self.namespace['costbasis'] = avg_entry
                if not silent:
                    self._livelog(
                        f"Position: {contracts} contracts @ {avg_entry:.2f} | "
                        f"Free margin: ${self.namespace['usd']:.2f}"
                    )
        except Exception:
            if not silent:
                self._livelog(f"Position read error:\n{traceback.format_exc()}")
            self.namespace['realposition'] = 0.0
            self.namespace['costbasis'] = 0.0

    def _emit_order_terminal_event(self, row, status, order_payload):
        """Mark the local liveorder `row` as terminal and emit the canonical
        suffixed event (fill:<tt>, cancel:<tt>, fail:<tt>). Idempotent —
        if the row was already terminal (the other path got there first), this
        is a no-op so WS + reconciliation don't double-emit. `row` is a row from
        the liveorder table; `status` is the upper-case Coinbase status;
        `order_payload` is the get_order/WS order dict (used to surface
        filled_size / average_filled_price in the event)."""
        if (row['status'] or '').lower() in ('filled', 'cancelled', 'failed'):
            return True
        cb_id = row['coinbase_order_id'] or ''
        tt = row['tradetype'] or 'order'
        filled_size = float(
            order_payload.get('filled_size',
                              order_payload.get('cumulative_quantity', 0)) or 0)
        avg_price = float(
            order_payload.get('average_filled_price',
                              order_payload.get('avg_price', 0)) or 0)
        # Coinbase returns total_fees as a string on FILLED orders (both the
        # REST get_order body and the WS user-channel push). When the order
        # isn't yet filled / is cancelled with no fills, it's "0" or absent.
        total_fees = float(order_payload.get('total_fees', 0) or 0)
        total_value_after_fees = float(
            order_payload.get('total_value_after_fees', 0) or 0)
        evt = {
            'coinbase_order_id': cb_id,
            'tradetype': tt,
            'amount': row['amount'] or 0,
            'limitprice': row['limitprice'] or 0,
            'stopprice': row['stopprice'] or 0,
            'limittrailpercent': row['limittrailpercent'] or 0,
            'stoptrailpercent': row['stoptrailpercent'] or 0,
            'status': status,
            'filled_size': filled_size,
            'average_filled_price': avg_price,
            'completion_percentage': float(order_payload.get('completion_percentage', 0) or 0),
            'total_fees': total_fees,
            'total_value_after_fees': total_value_after_fees,
        }
        if status == 'FILLED':
            lutil.runupdate("UPDATE liveorder SET status='filled' WHERE id=?", (row['id'],))
            self._livelog(
                f"Order {cb_id} FILLED {filled_size} @ {avg_price:.2f} "
                f"(fee ${total_fees:.4f})"
            )
            self._log_event('fill:' + tt, evt)
        elif status in ('CANCELLED', 'EXPIRED'):
            lutil.runupdate("UPDATE liveorder SET status='cancelled' WHERE id=?", (row['id'],))
            self._livelog(f"Order {cb_id} {status} on exchange (filled {filled_size})")
            # Trail-initiated cancels: log but don't notify, and drop the id
            # from the set now that we've seen its terminal state.
            trail_owned = cb_id in self._trail_canceling_ids
            self._trail_canceling_ids.discard(cb_id)
            self._log_event('cancel:' + tt, evt, notify=not trail_owned)
        elif status == 'FAILED':
            lutil.runupdate("UPDATE liveorder SET status='failed' WHERE id=?", (row['id'],))
            self._livelog(f"Order {cb_id} FAILED on exchange")
            self._log_event('fail:' + tt, evt)
        else:
            self._livelog(f"Order {cb_id} unknown status '{status}' — leaving open")
            return False
        return True

    def _read_account_state(self, product_id):
        self.refresh_balance_position(product_id, silent=False)
        cb = CoinbaseHTTP()

        # Open orders → pendingpositions
        try:
            open_orders = cb.list_orders(product_id=product_id, order_status=['OPEN'])
            orders_list = open_orders.get('orders', [])

            # Reconcile every tracked open order by asking Coinbase for its current
            # state. This distinguishes fills, cancels, expirations, and failures
            # (the old "missing from OPEN list = filled" assumption misclassified
            # exchange-side cancels and expirations as fills).
            tracked = lutil.runselect(
                "SELECT * FROM liveorder WHERE scriptid=? AND status='open'",
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
                self._emit_order_terminal_event(row, status, order)

            # Look up the local liveorder rows so we can surface trail
            # percents in pendingpositions AND include pending-trail rows
            # (no Coinbase order yet) so the script's `not pendingpositions`
            # gate doesn't double-issue them on the next tick.
            local_rows = lutil.runselect(
                "SELECT * FROM liveorder WHERE scriptid=? AND status='open'",
                (self.scriptid,))
            local_by_id = {r['coinbase_order_id']: r
                           for r in local_rows if r['coinbase_order_id']}

            pending = []
            for o in orders_list:
                cfg = o.get('order_configuration', {}) or {}
                # Pull limit and stop from whichever config block is present.
                # limit_limit_gtc/gtd: limit_price
                # stop_limit_stop_limit_gtc/gtd: limit_price + stop_price
                # trigger_bracket_gtc/gtd: limit_price + stop_trigger_price
                lp = 0.0
                sp = 0.0
                for v in cfg.values():
                    if not isinstance(v, dict):
                        continue
                    if v.get('limit_price'):
                        lp = float(v.get('limit_price') or 0)
                    if v.get('stop_price'):
                        sp = float(v.get('stop_price') or 0)
                    elif v.get('stop_trigger_price'):
                        sp = float(v.get('stop_trigger_price') or 0)
                if lp > 0 and sp > 0:
                    ordertype = 'Bracket'
                elif sp > 0:
                    ordertype = 'Stop'
                elif lp > 0:
                    ordertype = 'Limit'
                else:
                    ordertype = 'Market'
                local = local_by_id.get(o.get('order_id', ''))
                ltp_local = float(local['limittrailpercent'] or 0) if local else 0.0
                stp_local = float(local['stoptrailpercent'] or 0) if local else 0.0
                # For a trailing Exit, Coinbase only has the hard stop on file
                # (the limit is a local activation threshold) — surface the
                # threshold to the script so it can see the full picture.
                if local and local['tradetype'] == 'Exit' and ltp_local > 0 and local['limitprice']:
                    lp = float(local['limitprice'])
                # Prefer the local tradetype (Buy/Sell/Exit) over the side-
                # based guess so Exit orders aren't mis-labeled as Sell/Buy.
                tradetype = local['tradetype'] if local else (
                    'Buy' if o.get('side') == 'BUY' else 'Sell')
                pending.append({
                    'id': o.get('order_id', ''),
                    'ordertype': ordertype,
                    'price': lp, 'amount': float(o.get('base_size', 0) or 0),
                    'stopprice': sp, 'limitprice': lp,
                    'limittrailpercent': ltp_local, 'stoptrailpercent': stp_local,
                    'tradetype': tradetype,
                })

            # Also include pending-trail rows that aren't on the exchange
            # yet. Without this, the script sees pendingpositions empty
            # and would issue another Exit on the next tick.
            exchange_ids = {o.get('order_id', '') for o in orders_list}
            for r in local_rows:
                cb_id = r['coinbase_order_id'] or ''
                if cb_id and cb_id in exchange_ids:
                    continue  # already represented above
                ltp_l = float(r['limittrailpercent'] or 0)
                lp_l = float(r['limitprice'] or 0)
                sp_l = float(r['stopprice'] or 0)
                ordertype = ('TrailPending' if ltp_l > 0 and lp_l > 0
                             else 'Internal')
                pending.append({
                    'id': r['internal_id'] or cb_id,
                    'ordertype': ordertype,
                    'price': lp_l, 'amount': float(r['amount'] or 0),
                    'stopprice': sp_l, 'limitprice': lp_l,
                    'limittrailpercent': ltp_l,
                    'stoptrailpercent': float(r['stoptrailpercent'] or 0),
                    'tradetype': r['tradetype'],
                })

            self.namespace['pendingpositions'] = pending
        except Exception:
            self._livelog(f"Open orders read error:\n{traceback.format_exc()}")

    # ------------------------------------------------------------------ trailing order management

    def _poll_market_tick(self, product_id):
        """Background market poll run by the LiveTrader thread between candle
        closes. Fetches the latest mid from Coinbase, caches it for the
        frontend's /api/live/price endpoint, and runs any pending trail
        updates. Errors are deduped so a flaky ticker doesn't spam the log
        every 5 seconds."""
        try:
            cb = CoinbaseHTTP()
            product = cb.get_product(product_id)
            bid = float(product.get('best_bid_price') or 0)
            ask = float(product.get('best_ask_price') or 0)
            if bid > 0 and ask > 0:
                price = (bid + ask) / 2
            elif bid > 0:
                price = bid
            elif ask > 0:
                price = ask
            else:
                price = float(product.get('price') or 0)
            if price > 0:
                # Publish to the shared cache before doing the trail work so
                # /api/live/price can return immediately even mid-trail.
                self._last_price = price
                self._last_price_time = time.time()
                # Cheap pre-check: skip the trail lock dance entirely when
                # no rows have a trail percent set.
                rows = lutil.runselect(
                    "SELECT 1 FROM liveorder WHERE scriptid=? AND status='open' "
                    "AND limittrailpercent > 0 LIMIT 1",
                    (self.scriptid,))
                if rows:
                    self.update_trailing(price)
            self._last_market_poll_error = None
        except Exception as e:
            # Suppress repeated identical errors so one bad ticker call
            # doesn't fill the log with 720 lines/hour.
            msg = str(e)
            if getattr(self, '_last_market_poll_error', None) != msg:
                self._livelog(f"Market poll fetch failed: {msg}")
                self._last_market_poll_error = msg

    def update_trailing(self, price):
        """Re-evaluate trailing orders against a fresh market price. Called
        from the price-poll API path on every market update so trails track
        intra-candle moves instead of only updating at candle close. Skips
        cleanly if the trader isn't fully started or another update is
        already in flight (the next poll picks up the slack)."""
        if not self.running or not self.pair:
            return
        try:
            price = float(price)
        except (TypeError, ValueError):
            return
        if price <= 0:
            return
        if not self._trail_lock.acquire(blocking=False):
            return
        try:
            self._update_trailing_orders(self.pair, price)
        except Exception:
            self._livelog(f"Trail update error:\n{traceback.format_exc()}")
        finally:
            self._trail_lock.release()

    def _trail_direction(self, tradetype, pos):
        """Resolve a trail row to one of two symmetric mechanics.

        Returns (kind, cb_side, stop_dir):
          kind='upside'   → peak tracks the HIGH, stop = peak*(1-ltp),
                            placed as a SELL stop_limit / STOP_DOWN.
                            Used for Long Exit and Sell Entry.
          kind='downside' → peak tracks the LOW, stop = peak*(1+ltp),
                            placed as a BUY  stop_limit / STOP_UP.
                            Used for Short Exit and Buy Entry.
        Returns None if the (tradetype, pos) combo can't run a trail
        (Exit with no position is the only realistic case)."""
        if tradetype == 'Exit':
            if pos > 0:
                return ('upside', 'SELL', 'STOP_DIRECTION_STOP_DOWN')
            if pos < 0:
                return ('downside', 'BUY', 'STOP_DIRECTION_STOP_UP')
            return None
        if tradetype == 'Sell':
            return ('upside', 'SELL', 'STOP_DIRECTION_STOP_DOWN')
        if tradetype == 'Buy':
            return ('downside', 'BUY', 'STOP_DIRECTION_STOP_UP')
        return None

    def _update_trailing_orders(self, product_id, price):
        """Re-evaluate every open trailing liveorder against the current
        market `price` and cancel+replace the exchange order whenever the
        trail moves. Runs on each backend market poll (5s) and on the
        frontend /api/live/price path.

        Trail design (see _trail_direction for the symmetric pairing):
          • Pre-activation: do nothing on the exchange side (any initial
            hard stop placed by _execute_order just sits there).
          • Activation fires when price crosses the stored `limitprice`
            in the favorable direction.
          • Post-activation: track `peak` (HIGH for upside, LOW for
            downside), set stop = peak*(1∓ltp), and only move the stop
            monotonically (up for upside, down for downside). The original
            hard stop is intentionally NOT used as a floor after
            activation."""
        rows = lutil.runselect(
            "SELECT * FROM liveorder WHERE scriptid=? AND status='open' "
            "AND limittrailpercent > 0",
            (self.scriptid,))
        if not rows:
            return
        cb = CoinbaseHTTP()
        pos = self.namespace.get('realposition', 0)

        for row in rows:
            ltp = row['limittrailpercent'] or 0
            tradetype = row['tradetype']
            activation = float(row['limitprice'] or 0)
            if ltp <= 0 or activation <= 0:
                continue

            direction = self._trail_direction(tradetype, pos)
            if direction is None:
                continue
            kind, cb_side, stop_dir = direction
            upside = (kind == 'upside')

            activated = bool(row.get('activated', 0))
            peak = float(row.get('peak_price', 0))
            cur_stop = float(row['stopprice'] or 0)

            # Activation check.
            if not activated:
                triggered = (upside and price >= activation) or \
                            (not upside and price <= activation)
                if not triggered:
                    continue
                activated = True
                peak = price
                lutil.runupdate(
                    "UPDATE liveorder SET activated=1, peak_price=? WHERE id=?",
                    (peak, row['id']))
                self._livelog(
                    f"Trail activated [{tradetype}] at {price:.2f} "
                    f"(activation@{activation:.2f}, trail:{ltp*100:.2f}%)"
                )
                # One-shot notification: subsequent trail re-places are
                # suppressed (notify=False at the end of this function), so
                # this is the user's only ping between create and fill.
                self._log_event('create:trail-activated', {
                    'tradetype': tradetype,
                    'coinbase_order_id': row['coinbase_order_id'] or '',
                    'activation_price': float(activation),
                    'market_price': float(price),
                    'limittrailpercent': float(ltp),
                })

            # Peak update (monotonic in the favorable direction).
            if upside:
                if price > peak:
                    peak = price
                    lutil.runupdate("UPDATE liveorder SET peak_price=? WHERE id=?",
                                    (peak, row['id']))
                new_stop = peak * (1.0 - ltp)
            else:
                if peak == 0 or price < peak:
                    peak = price
                    lutil.runupdate("UPDATE liveorder SET peak_price=? WHERE id=?",
                                    (peak, row['id']))
                new_stop = peak * (1.0 + ltp)

            # Decide whether the exchange order needs to be (re)placed.
            # Compare on tick-quantized prices so a sub-tick drift in `peak`
            # doesn't churn cancel+replace cycles that wouldn't change the
            # order on Coinbase.
            first_placement = not bool(row['coinbase_order_id'])
            pi = self._price_increment or 0.01
            new_ticks = round(new_stop / pi)
            cur_ticks = round(cur_stop / pi) if cur_stop > 0 else 0
            if first_placement:
                needs_place = True
            elif upside:
                needs_place = new_ticks > cur_ticks
            else:
                needs_place = new_ticks < cur_ticks
            if not needs_place:
                continue

            # Cancel existing exchange order (skipped on first placement).
            # If the existing order is the case-4 hard stop, this cancel
            # leaves the position briefly unprotected for ~2-5s before
            # the new trailing stop lands — accepted tradeoff.
            #
            # The cancel must be CONFIRMED gone from the book before we place
            # the replacement: a second near-identical resting stop-limit while
            # the old one is still OPEN is what Coinbase rejects with the
            # generic UNKNOWN_FAILURE_REASON (the old fire-and-forget cancel
            # raced the new placement and lost). If we can't confirm the cancel,
            # skip the replace this cycle and retry on the next poll — never
            # place a duplicate.
            if row['coinbase_order_id']:
                # Register the id so the cancel WS + reconciliation events
                # it triggers don't fire push notifications (trail churn).
                self._trail_canceling_ids.add(row['coinbase_order_id'])
                if not self._cancel_and_confirm(cb, row['coinbase_order_id']):
                    self._livelog(
                        f"Trail: cancel of {row['coinbase_order_id']} not confirmed "
                        f"— skipping replace this cycle (will retry next poll)"
                    )
                    continue

            # Place the new stop_limit.
            new_cb_id = str(uuid.uuid4())
            # row['amount'] is contract count; convert to base-asset units
            # for _format_base_size, which expects BTC and re-divides by
            # contract_size for the wire string.
            cs = self._contract_size or 1.0
            amount_btc = float(row['amount']) * cs
            base_size = self._format_base_size(self._round_to_increment(amount_btc))
            intent = (f"TRAIL {cb_side} stop_limit {base_size} @ {new_stop:.2f} "
                      f"[{tradetype} peak {peak:.2f} trail:{ltp*100:.2f}%]")
            try:
                resp = self._cb_create_order(cb, new_cb_id, product_id, cb_side, {
                    'stop_limit_stop_limit_gtc': {
                        'base_size': base_size,
                        'limit_price': self._format_price(new_stop),
                        'stop_price': self._format_stop_trigger_price(new_stop, cb_side),
                        'stop_direction': stop_dir,
                    }
                })
            except Exception:
                self._livelog(f"Trail place error:\n{traceback.format_exc()}")
                continue
            if not self._check_order_response(resp, intent):
                continue
            new_cb_id = self._get_cb_order_id(resp, new_cb_id, product_id) or new_cb_id

            lutil.runupdate(
                "UPDATE liveorder SET coinbase_order_id=?, stopprice=?, limitprice=? WHERE id=?",
                (new_cb_id, new_stop, new_stop, row['id']))
            self._livelog(intent)
            self._log_event('trail:' + tradetype, {
                'coinbase_order_id': new_cb_id,
                'tradetype': tradetype,
                'old_stopprice': cur_stop,
                'new_stopprice': float(new_stop),
                'peak_price': float(peak),
                'limittrailpercent': float(ltp),
                'market_price': float(price),
                'first_placement': first_placement,
            }, notify=False)

    # ------------------------------------------------------------------ product limits

    def _load_product_limits(self, product_id):
        from coinbase_http import KNOWN_CONTRACT_SIZES, KNOWN_MAX_LEVERAGES
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
            # `price_increment` is the ACTUAL tick (e.g. $5 for BIP-20DEC30-CDE).
            # `quote_increment` is only the USD currency precision ($0.01) and is
            # misleading on coarse-tick products — Coinbase rejects with
            # INVALID_PRICE_PRECISION if you round to quote_increment instead.
            self._price_increment = (float(product.get('price_increment') or 0) or
                                     float(product.get('quote_increment') or 0) or None)
            self._product_venue = (product.get('product_venue') or '').upper()

            future_details = product.get('future_product_details') or {}
            # For futures, base_currency_id / base_name are usually blank — the
            # root unit lives on future_product_details.contract_root_unit.
            self._base_currency = (
                product.get('base_currency_id') or product.get('base_name') or
                future_details.get('contract_root_unit') or ''
            ).upper()
            api_contract_size = float(future_details.get('contract_size') or 0)
            perp_details = future_details.get('perpetual_details') or {}
            api_max_leverage = float(perp_details.get('max_leverage') or 0)

            # Exchange-side leverage cap. INTX: API value is unreliable post-merger,
            # mirror KNOWN_CONTRACT_SIZES treatment. FCM/CDE: API returns 0 (no
            # perpetual_details), so we leave it None and let the broker reject
            # an over-leveraged order rather than guess.
            known_max_lev = KNOWN_MAX_LEVERAGES.get(product_id)
            if self._product_venue == 'INTX' and known_max_lev is not None:
                self._max_leverage = known_max_lev
            elif api_max_leverage > 0:
                self._max_leverage = api_max_leverage
            else:
                self._max_leverage = None

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

            # Coinbase reports base_min_size / base_max_size in CONTRACTS for FCM/CDE
            # products (e.g. min=1.0 contract for BIP-20DEC30-CDE) but our internal
            # sizing logic operates in BASE-ASSET units (BTC). Convert at load so
            # _cap_base_size can compare apples-to-apples.
            if self._product_venue in ('FCM', 'CDE') and self._contract_size:
                if self._min_base_size:
                    self._min_base_size = self._min_base_size * self._contract_size
                if self._max_base_size:
                    self._max_base_size = self._max_base_size * self._contract_size

            self._livelog(
                f"Product limits — venue:{self._product_venue} min:{self._min_base_size} "
                f"max:{self._max_base_size} (base units) base_increment:{self._base_increment} "
                f"price_increment:{self._price_increment} | "
                f"contract_size:{self._contract_size} "
                f"max_leverage:{self._max_leverage if self._max_leverage is not None else 'unknown'}x"
            )
        except Exception:
            self._livelog(f"Could not load product limits:\n{traceback.format_exc()}")

        # Last-resort fallback: pull the root unit from the local cache so the
        # status panel's "Base" field never goes blank when the product is known.
        if not self._base_currency:
            try:
                row = lutil.get_futures_product(product_id) or {}
                self._base_currency = (row.get('contract_root_unit') or '').upper()
            except Exception:
                pass

    # ------------------------------------------------------------------ websocket: fill/cancel notifications

    def _start_ws(self, product_id):
        """Open a WSClient on Coinbase's 'user' channel for this product so we
        get pushed FILLED/CANCELLED order events in real time instead of waiting
        for the next candle tick. The SDK runs its own listener thread."""
        # Clean up any prior client first (e.g. restart from _run_with_restart).
        self._stop_ws()
        try:
            from coinbase.websocket import WSClient
            key_name = lutil.getkeyval('cbkey')
            key_secret = lutil.getkeyval('cbsecret')
            if not key_name or not key_secret:
                self._livelog("WS: skipping fill subscription — no credentials")
                return
            key_secret = key_secret.replace('\\n', '\n').strip()
            self._ws_client = WSClient(
                api_key=key_name,
                api_secret=key_secret,
                on_message=self._on_ws_message,
                on_open=lambda: self._livelog(f"WS: user channel connected for {product_id}"),
                on_close=lambda: self._livelog("WS: user channel disconnected"),
                retry=True,
                verbose=False,
            )
            self._ws_client.open()
            self._ws_client.user(product_ids=[product_id])
            self._ws_product_id = product_id
        except Exception:
            self._livelog(f"WS: failed to start:\n{traceback.format_exc()}")
            self._ws_client = None

    def _stop_ws(self):
        if self._ws_client:
            try:
                self._ws_client.close()
            except Exception:
                pass
        self._ws_client = None
        self._seen_order_states = {}

    def _on_ws_message(self, raw_msg):
        try:
            data = json.loads(raw_msg) if isinstance(raw_msg, str) else raw_msg
            if data.get('channel') != 'user':
                return
            for ev in (data.get('events') or []):
                for order in (ev.get('orders') or []):
                    self._handle_order_update(order)
        except Exception:
            self._livelog(f"WS: message handler error:\n{traceback.format_exc()}")

    def _handle_order_update(self, order):
        """Dedupe by (order_id, status); only emit on FILLED/CANCELLED-class
        transitions. Triggers _read_account_state so position/equity refresh
        immediately rather than at the next candle close."""
        order_id = order.get('order_id') or ''
        status = (order.get('status') or '').upper()
        if not order_id or not status:
            return
        if self._seen_order_states.get(order_id) == status:
            return
        self._seen_order_states[order_id] = status
        if status not in ('FILLED', 'CANCELLED', 'EXPIRED', 'FAILED'):
            return
        side = order.get('order_side') or order.get('side') or ''
        filled = order.get('cumulative_quantity') or order.get('filled_size') or '0'
        avg_price = order.get('avg_price') or order.get('average_filled_price') or '0'
        product = order.get('product_id') or self._ws_product_id or ''
        self._livelog(f"WS {status}: {side} {filled} {product} @ {avg_price}")
        # Prefer the suffixed fill:<tt>/cancel:<tt> event so the chart and
        # Recent Orders table always get an entry — reconciliation in
        # _read_account_state used to be the sole emitter of those, but it
        # skips rows that are no longer status='open' (e.g. the trail loop
        # marked them cancelled in a cancel+replace that raced with this
        # fill), which is how exit fills went missing from the UI. If we
        # can't find a matching local row (foreign/manual order), fall back
        # to the bare fill/cancel event so the user still sees something.
        rows = lutil.runselect(
            "SELECT * FROM liveorder WHERE scriptid=? AND coinbase_order_id=?",
            (self.scriptid, order_id))
        if rows:
            self._emit_order_terminal_event(rows[0], status, order)
        else:
            event_type = 'fill' if status == 'FILLED' else 'cancel'
            # Suppress notification when the cancel was initiated by the
            # trail loop — the user doesn't want a ping every trail step.
            notify = not (event_type == 'cancel' and order_id in self._trail_canceling_ids)
            self._log_event(event_type, {
                'order_id': order_id, 'product_id': product, 'side': side,
                'status': status, 'filled': str(filled), 'avg_price': str(avg_price),
            }, notify=notify)
        try:
            if self._ws_product_id:
                self._read_account_state(self._ws_product_id)
        except Exception:
            self._livelog(f"WS: account refresh after {status} failed:\n{traceback.format_exc()}")

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

    def _base_size_to_contracts(self, base_size_str: str) -> float:
        """Inverse of _format_base_size: turn the wire base_size string back
        into a contract count for reporting. CDE/FCM already wire it as
        contracts; INTX wires it as base-asset amount, so divide by contract_size."""
        try:
            bs = float(base_size_str)
        except (TypeError, ValueError):
            return 0.0
        if (self._product_venue or '').upper() in ('FCM', 'CDE'):
            return bs
        cs = self._contract_size or 0.0
        return bs / cs if cs > 0 else bs

    def _effective_leverage(self):
        """Script's leverage, clamped to the exchange's max if known. Callers
        get a silent clamp; _execute_order logs once per order intent when it
        kicks in so the user knows their script asked for more than allowed."""
        try:
            lev = float(self.namespace.get('leverage', 1))
        except (TypeError, ValueError):
            lev = 1.0
        if self._max_leverage and lev > self._max_leverage:
            return self._max_leverage
        return lev

    def _price_decimals(self) -> int:
        """Decimal places implied by price_increment (e.g. 5.0 → 0, 0.01 → 2)."""
        pi = self._price_increment or 0.01
        s = f"{pi:.10f}".rstrip('0').rstrip('.')
        return len(s.split('.')[1]) if '.' in s else 0

    def _format_price(self, price: float) -> str:
        """Round to the product's price_increment and emit a clean fixed-decimal
        string. Coinbase rejects with INVALID_PRICE_PRECISION when the price
        isn't a multiple of price_increment (e.g. $5 tick on BIP-20DEC30-CDE).
        Naive round(price, 2) and even rounding to quote_increment are wrong
        on coarse-tick products — quote_increment is just USD precision."""
        pi = self._price_increment or 0.01
        if pi <= 0:
            return f"{price:.2f}"
        rounded = round(price / pi) * pi
        return f"{rounded:.{self._price_decimals()}f}"

    def _format_stop_trigger_price(self, limit_price: float, side: str) -> str:
        """Trigger price for a stop_limit order. The user's stated price is
        the LIMIT (the worst acceptable fill); the trigger is offset ~0.1%
        BEFORE the limit so that when it fires, the resulting limit order
        still has room to cross the book. Also enforces at least 1 tick of
        separation (in case the percent offset rounds onto the limit on
        coarse-tick products).
          BUY  stop (STOP_UP, price rising):  trigger BELOW limit.
          SELL stop (STOP_DOWN, price falling): trigger ABOVE limit."""
        pi = self._price_increment or 0.01
        rounded_limit = round(limit_price / pi) * pi
        if (side or '').upper() == 'BUY':
            candidate = round((limit_price * 0.999) / pi) * pi
            if candidate >= rounded_limit:
                candidate = rounded_limit - pi
        else:
            candidate = round((limit_price * 1.001) / pi) * pi
            if candidate <= rounded_limit:
                candidate = rounded_limit + pi
        return f"{candidate:.{self._price_decimals()}f}"

    def _cb_create_order(self, cb, client_order_id, product_id, side, order_configuration):
        """All perp orders go through here so leverage + margin_type are always
        set. Per Coinbase docs:
          - leverage defaults to "1.0" if omitted (wrong for perp strategies).
          - margin_type defaults to CROSS, explicit is safer.
          - retail_portfolio_id is deprecated for CDP keys, do NOT send."""
        leverage = self._effective_leverage()
        try:
            lev_str = f"{float(leverage):.1f}"
        except Exception:
            lev_str = str(leverage)
        return cb.create_order(
            client_order_id, product_id, side, order_configuration,
            leverage=lev_str, margin_type='CROSS',
        )

    def _cap_base_size(self, base_size_f, intent_label=None):
        base_size_f = self._round_to_increment(base_size_f)
        if self._max_base_size and base_size_f > self._max_base_size:
            self._livelog(f"base_size {base_size_f} capped to max {self._max_base_size}")
            base_size_f = self._round_to_increment(self._max_base_size)
        if self._min_base_size and base_size_f < self._min_base_size:
            msg = (f"computed size {base_size_f} below product min {self._min_base_size} "
                   f"(base-asset units; contract_size={self._contract_size})")
            if intent_label:
                self._notify_order_error(intent_label, 'BELOW_MIN_SIZE', msg)
            else:
                self._livelog(msg + " — order skipped")
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

    def _cancel_and_confirm(self, cb, order_id, timeout=8.0, attempts=3):
        """Cancel a resting order and BLOCK until Coinbase confirms it has left
        the OPEN book (or `timeout` elapses). Returns True only when the order
        is confirmed gone, so the caller can safely place a replacement without
        two near-identical resting stop-limits colliding on the exchange
        (Coinbase rejects the second with UNKNOWN_FAILURE_REASON).

        Used by the trail loop, which re-prices by cancel+replace. Does NOT
        touch the DB or emit cancel events — the WS/reconciliation paths own the
        cancelled row's status update. Safe to run on the background poll thread
        only: update_trailing holds a non-blocking lock, so the frontend price
        path returns the cached price instead of waiting on this."""
        # Retry the cancel REQUEST itself on transient errors (network blips)
        # so a momentary hiccup doesn't make us skip the re-price for a whole
        # poll cycle. A definitive exchange rejection (success:false) is NOT
        # retried — that's a real answer (e.g. the order already filled), and
        # retrying it would be wrong.
        resp = None
        for attempt in range(1, attempts + 1):
            try:
                resp = cb.cancel_orders([order_id])
                break
            except Exception:
                self._livelog(
                    f"Trail cancel request error for {order_id} "
                    f"(attempt {attempt}/{attempts}):\n{traceback.format_exc()}"
                )
                if attempt < attempts:
                    time.sleep(0.5)
        if resp is None:
            return False
        results = resp.get('results', []) if isinstance(resp, dict) else []
        first = results[0] if results else {}
        if not first.get('success'):
            reason = first.get('failure_reason') or 'no response'
            self._livelog(f"Trail cancel of {order_id} rejected by exchange: {reason}")
            return False
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                data = cb.list_orders(order_status=['OPEN'], product_type='FUTURE', limit=100)
                open_orders = data.get('orders', []) or []
                if not any((o.get('order_id') or '') == order_id for o in open_orders):
                    return True
            except Exception:
                self._livelog(f"Trail cancel confirm poll error for {order_id}:\n{traceback.format_exc()}")
            time.sleep(0.4)
        return False

    def _cancel_order(self, order_id):
        # Issue the cancel and BLOCK until Coinbase confirms the order has
        # left OPEN status. Only then update local state. This protects the
        # script from racing — without it, the script sees pendingpositions
        # empty immediately and may place a new order while the old one is
        # still live on the exchange.
        #
        # `order_id` may be either a Coinbase order_id (exchange-backed
        # order) or our internal_id (pending-trail row with no Coinbase
        # order yet); pendingpositions exposes whichever exists.
        cb = CoinbaseHTTP()
        rows = lutil.runselect(
            "SELECT * FROM liveorder "
            "WHERE (coinbase_order_id=? OR internal_id=?) AND scriptid=?",
            (order_id, order_id, self.scriptid))
        if not rows:
            self._livelog(f"cancel_order: no liveorder row matches {order_id}")
            return
        row = rows[0]
        tradetype_name = row['tradetype'] or 'order'
        cb_id = row['coinbase_order_id'] or ''
        internal_id = row['internal_id'] or ''

        # Pending-trail row: nothing on Coinbase to cancel. Just clear the
        # local tracking and emit the event.
        if not cb_id:
            positions = self.namespace.get('pendingpositions', [])
            self.namespace['pendingpositions'] = [
                p for p in positions
                if p.get('id') not in (order_id, internal_id)
            ]
            lutil.runupdate(
                "UPDATE liveorder SET status='cancelled' WHERE id=?",
                (row['id'],))
            self._log_event('cancel:' + tradetype_name, {
                'coinbase_order_id': '',
                'internal_id': internal_id,
                'final_status': 'CANCELLED',
                'pending_trail': True,
            })
            self._livelog(f"Cancelled pending-trail order {internal_id}")
            return

        order_id = cb_id  # use the real Coinbase id from here on
        try:
            resp = cb.cancel_orders([order_id])
        except Exception:
            self._livelog(f"cancel_order request error for {order_id}:\n{traceback.format_exc()}")
            return

        results = resp.get('results', []) if isinstance(resp, dict) else []
        first = results[0] if results else {}
        if not first.get('success'):
            reason = first.get('failure_reason') or 'no response'
            self._livelog(f"cancel_order {order_id} rejected by exchange: {reason} — local state unchanged")
            return
        self._livelog(f"Cancel sent for {order_id} — awaiting exchange confirmation")

        # Poll until the order is no longer OPEN/PENDING on Coinbase. Use
        # list_orders(OPEN) so confirmation matches what the script sees in
        # pendingpositions (which is built from the same call).
        final_status = None
        deadline = time.time() + 10
        while time.time() < deadline:
            try:
                data = cb.list_orders(order_status=['OPEN'], product_type='FUTURE', limit=100)
                open_orders = data.get('orders', []) or []
                if not any((o.get('order_id') or '') == order_id for o in open_orders):
                    # Order has left the open list — get its terminal status.
                    try:
                        od = cb.get_order(order_id).get('order') or {}
                        final_status = (od.get('status') or 'CANCELLED').upper()
                    except Exception:
                        final_status = 'CANCELLED'
                    break
            except Exception:
                self._livelog(f"Cancel confirm poll error for {order_id}:\n{traceback.format_exc()}")
            time.sleep(0.5)

        if final_status is None:
            self._livelog(
                f"Cancel {order_id}: not confirmed gone from open orders within 10s — "
                "local state unchanged (will retry on next tick)"
            )
            return

        self._livelog(f"Cancel {order_id} confirmed: status={final_status}")
        positions = self.namespace.get('pendingpositions', [])
        self.namespace['pendingpositions'] = [p for p in positions if p['id'] != order_id]
        # Only mark cancelled in the DB if Coinbase actually cancelled — if
        # the order filled in the race, the WS/reconciliation path owns the
        # status update.
        if final_status == 'CANCELLED':
            lutil.runupdate(
                "UPDATE liveorder SET status='cancelled' WHERE coinbase_order_id=? AND scriptid=?",
                (order_id, self.scriptid))
        self._log_event('cancel:' + tradetype_name,
                        {'coinbase_order_id': order_id, 'final_status': final_status})

    # ------------------------------------------------------------------ order execution

    def _execute_order(self, trade_order, product_id, close_price):
        cb = CoinbaseHTTP()

        tradetype = trade_order.tradetype
        amount = trade_order.amount
        limitprice = trade_order.limitprice
        stopprice = trade_order.stopprice
        ltp = trade_order.limittrailpercent
        stp = trade_order.stoptrailpercent
        # stoptrailpercent is deprecated. The trail model is driven entirely
        # by limittrailpercent + an activation threshold (lp). Warn and drop
        # so the order proceeds with whatever other fields are set.
        if stp and stp > 0:
            self._livelog(
                f"Warning: stoptrailpercent={stp} is deprecated and ignored. "
                "Use limittrailpercent with a limit price as the activation."
            )
            stp = 0

        script_leverage = self.namespace.get('leverage', 10)
        leverage = self._effective_leverage()
        if self._max_leverage and float(script_leverage) > self._max_leverage:
            self._livelog(
                f"Script leverage {script_leverage}x exceeds exchange max "
                f"{self._max_leverage}x — clamping to {self._max_leverage}x"
            )
        realposition = self.namespace.get('realposition', 0.0)

        # Auto-size: free_margin × script_leverage × 0.99.
        # `usd` is Coinbase's futures_buying_power = FREE MARGIN available
        # for new positions (already nets out current margin holds + accounts
        # for pending transfers). Multiplying by the script's leverage turns
        # margin into the notional we can actually open. Falls back to raw
        # equity if BP is missing.
        if amount == 0:
            usd = self.namespace.get('usd', 0)
            upnl = self.namespace.get('unrealized_pnl', 0.0)
            total_eq = self.namespace.get('total_equity', 0.0) or (usd + upnl)
            free_margin = float(usd or 0) or total_eq
            amount_notional = free_margin * leverage * 0.99
            self._livelog(
                f"Auto-size: ${amount_notional:.2f} = free_margin ${free_margin:.2f} × {leverage}x × 0.99 "
                f"(BP ${float(usd or 0):.2f}, total_eq ${total_eq:.2f})"
            )
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
                # realposition and user-supplied Exit `amount` are in CONTRACTS;
                # downstream helpers (_round_to_increment, _format_base_size)
                # work in BASE-ASSET (BTC) units. Convert here or you end up
                # selling 100 contracts when you have 1.
                cs = self._contract_size or 1.0
                pos_base = abs(pos) * cs
                req_base = (amount * cs) if amount > 0 else pos_base
                close_qty = min(req_base, pos_base)
                close_qty = self._round_to_increment(close_qty)
                if close_qty <= 0:
                    self._notify_order_error(
                        f"EXIT {'long' if pos > 0 else 'short'}",
                        'BELOW_GRANULARITY',
                        f"close_qty {close_qty} below whole-contract granularity "
                        f"(position={pos}, contract_size={self._contract_size})"
                    )
                    return
                base_size = self._format_base_size(close_qty)
                side = 'SELL' if pos > 0 else 'BUY'
                action = 'sell' if pos > 0 else 'buy'
                if ltp > 0:
                    # Trailing exit. Cases:
                    #   4: sp > 0, lp > 0 — hard stop_limit @ sp on exchange,
                    #      lp held locally as activation threshold.
                    #   5: sp == 0, lp > 0 — nothing on exchange yet.
                    #   6: sp == 0, lp == 0 — synthesize activation from
                    #      costbasis: long → costbasis*(1+ltp),
                    #      short → costbasis*(1-ltp). Nothing on exchange.
                    activation = limitprice
                    if activation == 0:
                        costbasis = self.namespace.get('costbasis', 0)
                        if costbasis <= 0:
                            self._livelog(
                                "Exit trailing with no limit price: costbasis "
                                "unknown — cannot synthesize activation."
                            )
                            return
                        activation = (costbasis * (1.0 + ltp) if pos > 0
                                      else costbasis * (1.0 - ltp))
                        self._livelog(
                            f"Exit trailing: synthesized activation @ "
                            f"{activation:.2f} (costbasis {costbasis:.2f} "
                            f"{'+' if pos > 0 else '-'} {ltp*100:.2f}%)"
                        )
                    if stopprice > 0:
                        # Case 4: place initial hard stop, lp is local
                        stop_dir = ('STOP_DIRECTION_STOP_DOWN' if pos > 0
                                    else 'STOP_DIRECTION_STOP_UP')
                        intent = (f"EXIT trailing {action} {base_size} "
                                  f"hard-stop@{stopprice} "
                                  f"activate@{activation:.2f} trail:{ltp}")
                        resp = self._cb_create_order(cb, order_id, product_id, side, {
                            'stop_limit_stop_limit_gtc': {
                                'base_size': base_size,
                                'limit_price': self._format_price(stopprice),
                                'stop_price': self._format_stop_trigger_price(stopprice, side),
                                'stop_direction': stop_dir,
                            }
                        })
                        if not self._check_order_response(resp, intent): return
                        cb_order_id = self._get_cb_order_id(resp, order_id, product_id)
                        self._livelog(intent)
                    else:
                        # Case 5/6: nothing on exchange until activation
                        intent = (f"EXIT trailing-pending {action} {base_size} "
                                  f"activate@{activation:.2f} trail:{ltp}")
                        self._livelog(intent)
                    # Persist the (possibly synthesized) activation as
                    # limitprice for the trail loop to read.
                    limitprice = activation
                elif limitprice > 0 and stopprice > 0:
                    # Bracket TP/SL.
                    intent = f"EXIT bracket {action} {base_size} TP@{limitprice} SL@{stopprice}"
                    resp = self._cb_create_order(cb, order_id, product_id, side, {
                        'trigger_bracket_gtc': {
                            'base_size': base_size,
                            'limit_price': self._format_price(limitprice),
                            'stop_trigger_price': self._format_price(stopprice),
                        }
                    })
                    if not self._check_order_response(resp, intent): return
                    cb_order_id = self._get_cb_order_id(resp, order_id, product_id)
                    self._livelog(intent)
                elif limitprice > 0:
                    intent = f"EXIT limit {action} {base_size} @ {limitprice}"
                    resp = self._cb_create_order(cb, order_id, product_id, side, {
                        'limit_limit_gtc': {'base_size': base_size, 'limit_price': self._format_price(limitprice)}
                    })
                    if not self._check_order_response(resp, intent): return
                    cb_order_id = self._get_cb_order_id(resp, order_id, product_id)
                    self._livelog(intent)
                elif stopprice > 0:
                    intent = f"EXIT stop {action} {base_size} @ {stopprice}"
                    resp = self._cb_create_order(cb, order_id, product_id, side, {
                        'stop_limit_stop_limit_gtc': {
                            'base_size': base_size,
                            'limit_price': self._format_price(stopprice),
                            'stop_price': self._format_stop_trigger_price(stopprice, side),
                            'stop_direction': 'STOP_DIRECTION_STOP_DOWN' if pos > 0 else 'STOP_DIRECTION_STOP_UP',
                        }
                    })
                    if not self._check_order_response(resp, intent): return
                    cb_order_id = self._get_cb_order_id(resp, order_id, product_id)
                    self._livelog(intent)
                else:
                    intent = f"EXIT market {action} {base_size}"
                    resp = self._cb_create_order(cb, order_id, product_id, side, {
                        'market_market_ioc': {'base_size': base_size}
                    })
                    if not self._check_order_response(resp, intent): return
                    cb_order_id = self._get_cb_order_id(resp, order_id, product_id)
                    self._livelog(intent)

            elif tradetype == util.TradeType.Buy:
                if realposition < 0:
                    close_id = str(uuid.uuid4())
                    cs = self._contract_size or 1.0
                    base_size = self._format_base_size(self._round_to_increment(abs(realposition) * cs))
                    intent = f"BUY: close short {base_size} at market"
                    resp = self._cb_create_order(cb, close_id, product_id, 'BUY', {
                        'market_market_ioc': {'base_size': base_size}
                    })
                    if not self._check_order_response(resp, intent): return
                    self._livelog(f"BUY: closed short {base_size} at market")
                    realposition = 0.0

                if ltp > 0:
                    # Buy trailing entry. Activation: price <= lp (market
                    # falls into the buy zone). After activation the trail
                    # tracks the LOW; we place a BUY stop at peak*(1+ltp)
                    # so a bounce off the low fires the entry. sp is ignored
                    # — entries don't have a "stop loss" before opening.
                    if stopprice > 0:
                        self._livelog(
                            f"Buy trailing: stop price {stopprice} ignored "
                            "(entries use only lp + ltp)"
                        )
                        stopprice = 0
                    activation = limitprice
                    if activation == 0:
                        # Case B6: synthesize activation = current market so
                        # the trail kicks in immediately on the next poll.
                        activation = (self._last_price
                                      or float(close_price or 0))
                        if activation <= 0:
                            self._livelog(
                                "Buy trailing with no limit price: no "
                                "market price available — skipping"
                            )
                            return
                        self._livelog(
                            f"Buy trailing: synthesized activation @ "
                            f"{activation:.2f} (current market)"
                        )
                    bs = self._cap_base_size(round(amount_notional / activation, 8),
                                             intent_label=f"BUY trailing-pending @ {activation:.2f}")
                    if bs <= 0: return
                    base_size = self._format_base_size(bs)
                    intent = (f"BUY trailing-pending {base_size} "
                              f"activate@{activation:.2f} trail:{ltp}")
                    self._livelog(intent)
                    # cb_order_id stays None; trail loop places the first
                    # exchange order at activation.
                    limitprice = activation
                elif limitprice > 0 and stopprice > 0:
                    # Bracket entry — either limit (buy on dip) or stop
                    # (buy on breakout) fires; the other auto-cancels.
                    bs = self._cap_base_size(round(amount_notional / limitprice, 8),
                                             intent_label=f"BUY bracket TP@{limitprice} SL@{stopprice}")
                    if bs <= 0: return
                    base_size = self._format_base_size(bs)
                    intent = f"BUY bracket {base_size} TP@{limitprice} SL@{stopprice}"
                    resp = self._cb_create_order(cb, order_id, product_id, 'BUY', {
                        'trigger_bracket_gtc': {
                            'base_size': base_size,
                            'limit_price': self._format_price(limitprice),
                            'stop_trigger_price': self._format_price(stopprice),
                        }
                    })
                    if not self._check_order_response(resp, intent): return
                    cb_order_id = self._get_cb_order_id(resp, order_id, product_id)
                    self._livelog(intent)
                elif limitprice > 0:
                    bs = self._cap_base_size(round(amount_notional / limitprice, 8),
                                             intent_label=f"BUY limit @ {limitprice}")
                    if bs <= 0: return
                    base_size = self._format_base_size(bs)
                    intent = f"BUY limit {base_size} @ {limitprice}"
                    resp = self._cb_create_order(cb, order_id, product_id, 'BUY', {
                        'limit_limit_gtc': {'base_size': base_size, 'limit_price': self._format_price(limitprice)}
                    })
                    if not self._check_order_response(resp, intent): return
                    cb_order_id = self._get_cb_order_id(resp, order_id, product_id)
                    self._livelog(intent)
                elif stopprice > 0:
                    bs = self._cap_base_size(round(amount_notional / stopprice, 8),
                                             intent_label=f"BUY stop @ {stopprice}")
                    if bs <= 0: return
                    base_size = self._format_base_size(bs)
                    intent = f"BUY stop {base_size} @ {stopprice}"
                    resp = self._cb_create_order(cb, order_id, product_id, 'BUY', {
                        'stop_limit_stop_limit_gtc': {
                            'base_size': base_size,
                            'limit_price': self._format_price(stopprice),
                            'stop_price': self._format_stop_trigger_price(stopprice, 'BUY'),
                            'stop_direction': 'STOP_DIRECTION_STOP_UP',
                        }
                    })
                    if not self._check_order_response(resp, intent): return
                    cb_order_id = self._get_cb_order_id(resp, order_id, product_id)
                    self._livelog(intent)
                else:
                    bs = self._cap_base_size(round(amount_notional / close_price, 8),
                                             intent_label="BUY market")
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
                    cs = self._contract_size or 1.0
                    base_size = self._format_base_size(self._round_to_increment(realposition * cs))
                    intent = f"SELL: close long {base_size} at market"
                    resp = self._cb_create_order(cb, close_id, product_id, 'SELL', {
                        'market_market_ioc': {'base_size': base_size}
                    })
                    if not self._check_order_response(resp, intent): return
                    self._livelog(f"SELL: closed long {base_size} at market")
                    realposition = 0.0

                if ltp > 0:
                    # Sell trailing entry. Activation: price >= lp (market
                    # rises into the short zone). After activation the
                    # trail tracks the HIGH; we place a SELL stop at
                    # peak*(1-ltp) so a rejection off the high fires the
                    # short. sp is ignored — entries don't carry a stop
                    # loss before opening.
                    if stopprice > 0:
                        self._livelog(
                            f"Sell trailing: stop price {stopprice} ignored "
                            "(entries use only lp + ltp)"
                        )
                        stopprice = 0
                    activation = limitprice
                    if activation == 0:
                        activation = (self._last_price
                                      or float(close_price or 0))
                        if activation <= 0:
                            self._livelog(
                                "Sell trailing with no limit price: no "
                                "market price available — skipping"
                            )
                            return
                        self._livelog(
                            f"Sell trailing: synthesized activation @ "
                            f"{activation:.2f} (current market)"
                        )
                    bs = self._cap_base_size(round(amount_notional / activation, 8),
                                             intent_label=f"SELL trailing-pending @ {activation:.2f}")
                    if bs <= 0: return
                    base_size = self._format_base_size(bs)
                    intent = (f"SELL trailing-pending {base_size} "
                              f"activate@{activation:.2f} trail:{ltp}")
                    self._livelog(intent)
                    limitprice = activation
                elif limitprice > 0 and stopprice > 0:
                    bs = self._cap_base_size(round(amount_notional / limitprice, 8),
                                             intent_label=f"SELL bracket TP@{limitprice} SL@{stopprice}")
                    if bs <= 0: return
                    base_size = self._format_base_size(bs)
                    intent = f"SELL bracket {base_size} TP@{limitprice} SL@{stopprice}"
                    resp = self._cb_create_order(cb, order_id, product_id, 'SELL', {
                        'trigger_bracket_gtc': {
                            'base_size': base_size,
                            'limit_price': self._format_price(limitprice),
                            'stop_trigger_price': self._format_price(stopprice),
                        }
                    })
                    if not self._check_order_response(resp, intent): return
                    cb_order_id = self._get_cb_order_id(resp, order_id, product_id)
                    self._livelog(intent)
                elif limitprice > 0:
                    bs = self._cap_base_size(round(amount_notional / limitprice, 8),
                                             intent_label=f"SELL limit @ {limitprice}")
                    if bs <= 0: return
                    base_size = self._format_base_size(bs)
                    intent = f"SELL limit {base_size} @ {limitprice}"
                    resp = self._cb_create_order(cb, order_id, product_id, 'SELL', {
                        'limit_limit_gtc': {'base_size': base_size, 'limit_price': self._format_price(limitprice)}
                    })
                    if not self._check_order_response(resp, intent): return
                    cb_order_id = self._get_cb_order_id(resp, order_id, product_id)
                    self._livelog(intent)
                elif stopprice > 0:
                    bs = self._cap_base_size(round(amount_notional / stopprice, 8),
                                             intent_label=f"SELL stop @ {stopprice}")
                    if bs <= 0: return
                    base_size = self._format_base_size(bs)
                    intent = f"SELL stop {base_size} @ {stopprice}"
                    resp = self._cb_create_order(cb, order_id, product_id, 'SELL', {
                        'stop_limit_stop_limit_gtc': {
                            'base_size': base_size,
                            'limit_price': self._format_price(stopprice),
                            'stop_price': self._format_stop_trigger_price(stopprice, 'SELL'),
                            'stop_direction': 'STOP_DIRECTION_STOP_DOWN',
                        }
                    })
                    if not self._check_order_response(resp, intent): return
                    cb_order_id = self._get_cb_order_id(resp, order_id, product_id)
                    self._livelog(intent)
                else:
                    bs = self._cap_base_size(round(amount_notional / close_price, 8),
                                             intent_label="SELL market")
                    if bs <= 0: return
                    base_size = self._format_base_size(bs)
                    intent = f"SELL market {base_size} contracts"
                    resp = self._cb_create_order(cb, order_id, product_id, 'SELL', {
                        'market_market_ioc': {'base_size': base_size}
                    })
                    if not self._check_order_response(resp, intent): return
                    cb_order_id = self._get_cb_order_id(resp, order_id, product_id)
                    self._livelog(intent)

            # Track every order in the DB — including pending trailing rows
            # that have no exchange order yet (cb_order_id is empty until the
            # market-poll path places the first stop on activation). The
            # next-tick reconciliation in _read_account_state fires
            # fill/cancel/fail events for any terminal transitions, as a
            # safety net for WS messages we might miss.
            is_pending_trail = (ltp > 0 and limitprice > 0 and not cb_order_id)
            if cb_order_id or is_pending_trail:
                # Store size as CONTRACT COUNT (venue-stable). The wire
                # `base_size` is contracts on CDE/FCM but base-asset units
                # on INTX; _base_size_to_contracts normalizes both back to
                # contracts so downstream readers (trail loop, UI, events)
                # don't have to know the venue.
                try:
                    base_size_f = self._base_size_to_contracts(base_size)
                except (ValueError, TypeError):
                    cs = self._contract_size or 0.01
                    btc = float(amount_notional / (limitprice or stopprice or close_price or 1))
                    base_size_f = btc / cs if cs > 0 else btc
                lutil.runinsert(
                    "INSERT OR IGNORE INTO liveorder "
                    "(scriptid, coinbase_order_id, internal_id, tradetype, limitprice, stopprice, "
                    "amount, limittrailpercent, stoptrailpercent, status, time, "
                    "activated, peak_price, hard_stopprice) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (self.scriptid, cb_order_id or '', order_id, tradetype.name,
                     limitprice, stopprice, base_size_f, ltp, stp, 'open', int(time.time()),
                     0, 0.0, float(stopprice)))

            # Report the contract count actually placed (after auto-size,
            # capping, and granularity rounding), not the raw user input.
            placed_contracts = self._base_size_to_contracts(base_size)
            # Distinguish "placed on Coinbase" from "tracked locally,
            # awaiting activation" so the event log makes the state clear.
            event_name = ('pending:' if is_pending_trail and not cb_order_id
                          else 'create:') + tradetype.name
            self._log_event(event_name, {
                'tradetype': tradetype.name, 'amount': placed_contracts,
                'limitprice': limitprice, 'stopprice': stopprice,
                'limittrailpercent': float(ltp or 0),
                'stoptrailpercent': float(stp or 0),
                'coinbase_order_id': cb_order_id or '',
                'on_exchange': bool(cb_order_id),
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
        dpnl = self.namespace.get('daily_realized_pnl', 0.0)
        total_equity = self.namespace.get('total_equity', 0.0) or (usd + upnl + dpnl)
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
            'price_increment': self._price_increment,
            'base_currency': self._base_currency,
            'last_tick_time': int(self.namespace.get('time', 0)),
            'log': (lutil.getkeyval(f'live_log_{self.scriptid}') or '').split('\n')[-100:],
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
        key = f'live_log_{scriptid}'
        existing = lutil.getkeyval(key) or ''
        lines = existing.split('\n') if existing else []
        lines.append(sep)
        lutil.setkeyval(key, '\n'.join(lines[-500:]))
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
