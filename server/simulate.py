import util
from coinbase.rest import RESTClient
from datetime import datetime
import time
import json
import talib
import numpy
import traceback
from enum import Enum
sutil = util.util()
import uuid
from datetime import datetime


class Simulation:

    def __init__(self, start, stop, scriptid):
        self.scriptid = scriptid
        self.start = start
        self.stop = stop
        self.good = True
        self.N = 0
        scripts = sutil.runselect("SELECT * FROM scripts WHERE id=?",(self.scriptid,))
        self.script = scripts[0]['script']
        self.namespace = {}
        self.namespace['talib'] = talib
        self.namespace['numpy'] = numpy
        self.namespace['Enum'] = Enum
        self.namespace['calcinds'] = []
        self.namespace['nan'] = numpy.nan
        self.namespace['TradeType'] = util.TradeType
        self.namespace['TradeOrder'] = util.TradeOrder
        self.namespace['granularity'] = "ONE_HOUR"
        self.namespace['pair'] = "btc"
        self.namespace['N'] = 0
        self.namespace['opens'] = []
        self.namespace['closes'] = []
        self.namespace['highs'] = []
        self.namespace['lows'] = []
        self.namespace['volumes'] = []
        self.namespace['candles'] = []
        self.namespace['candle'] = {}
        self.namespace['high'] = 0
        self.namespace['low'] = 0
        self.namespace['open'] = 0
        self.namespace['close'] = 0
        self.namespace['volume'] = 0
        self.namespace['time'] = 0
        self.namespace['maxpositions'] = 1
        self.namespace['pendingpositions'] = []
        self.namespace['realposition'] = 0.0   # contracts held (positive=long, negative=short)
        self.namespace['costbasis'] = 0.0      # average entry price of open position
        self.namespace['realspend'] = 0.0
        self.namespace['makerfee'] = 0.0000    # Coinbase perps maker fee (0% promotional)
        self.namespace['takerfee'] = 0.0003    # Coinbase perps taker fee (0.03%)
        self.namespace['leverage'] = 10        # Coinbase perps max leverage (10x)
        self.namespace['usd'] = 10000.00       # USDC collateral (free margin)
        self.namespace['fee'] = 0
        self.namespace['simlog'] = sutil.simlog
        self.namespace['cancel_order'] = self._cancel_order
        self.historysize = 300
        error = ""
        sutil.setkeyval('simpositions', json.dumps([]))
        try:
            exec(self.script, self.namespace)
        except Exception as e:
            error = traceback.format_exc()
            self.good = False
        if('pair' in self.namespace):
            self.pair = self.namespace['pair']
        if('granularity' in self.namespace):
            self.granularity = self.namespace['granularity']
        historicalpair = self.pair.upper()+'-PERP-INTX'
        print('BTC-PERP-INTX')
        print(historicalpair)
        self.simcandles = sutil.gethistoricledata(self.granularity, historicalpair, self.start, self.stop)
        self.cancelled = False
        self.simid = sutil.runinsert("INSERT INTO exchangesim (log, granularity, pair, start, stop, scriptid, runat, currenttick, totalticks) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                     ("", self.granularity, self.pair, start, stop, scriptid, int(time.time()), 0, len(self.simcandles)))
        sutil.setkeyval('simid', self.simid)
        sutil.setkeyval(f'sim_{self.simid}_leverage', str(self.namespace['leverage']))
        if(not self.good):
            sutil.runupdate("UPDATE exchangesim SET log=?, status=? WHERE id=?", (error, -1, self.simid))
        sutil.SimID = self.simid
        sutil.TickTime = "init"
        sutil.simlog(f"Sim started — pair:{self.pair.upper()}-PERP-INTX granularity:{self.granularity} USD:{self.namespace['usd']:.2f} leverage:{self.namespace['leverage']}x")

        self._SimUSDStart = self.namespace['usd']
        self._SimUSDEnd = self.namespace['usd']
        self._SimTrades = 0
        self._SimEntries = 0
        self._SimExits = 0
        self._SimMarkets = 0
        self._SimLongs = 0
        self._SimShorts = 0
        self._SimFeeTotal = 0
        self._SimProfTradeCount = 0
        self._SimLossTradeCount = 0
        self._SimTradeList = []


    def _cancel_order(self, order_id):
        positions = self.namespace['pendingpositions']
        cancelled = next((p for p in positions if p['id'] == order_id), None)
        new_positions = [p for p in positions if p['id'] != order_id]
        if cancelled:
            sutil.setkeyval('simpositions', json.dumps(new_positions))
            self.namespace['pendingpositions'] = new_positions
            sutil.simlog(f"Cancelled order {order_id}")
            candle = self.namespace.get('candle', {})
            eventdata = {
                'ordertype': cancelled.get('ordertype'), 'limitprice': cancelled.get('limitprice'),
                'stopprice': cancelled.get('stopprice'), 'amount': cancelled.get('amount'),
                'tradetype': cancelled.get('tradetype'),
                'usdcurr': self.namespace['usd'], 'cryptcurr': self.namespace['realposition'],
            }
            sutil.runinsert(
                "INSERT INTO simevent (exchangesimid, candleid, eventtype, eventdata, fee, metadata, time) VALUES(?,?,?,?,?,?,?)",
                (self.simid, candle.get('id'), 'cancel:' + cancelled.get('tradetype', ''), json.dumps(eventdata), 0, "", candle.get('timestamp', 0)))
        else:
            sutil.simlog(f"cancel_order: order {order_id} not found")

    def cleanarr(self, arr):
        arr = numpy.array(arr, dtype=float)
        missing = self.historysize - len(arr)
        if(missing > 0):
            arr = numpy.pad(arr, (missing, 0), constant_values=numpy.nan)
        return arr

    def _precompute_indicators(self):
        self._ind_arrays = {}
        if 'indicators' not in self.namespace:
            return
        n = len(self.simcandles)
        if n == 0:
            return

        self.namespace['opens']   = numpy.array([float(c['open'])   for c in self.simcandles], dtype=float)
        self.namespace['closes']  = numpy.array([float(c['close'])  for c in self.simcandles], dtype=float)
        self.namespace['highs']   = numpy.array([float(c['high'])   for c in self.simcandles], dtype=float)
        self.namespace['lows']    = numpy.array([float(c['low'])    for c in self.simcandles], dtype=float)
        self.namespace['volumes'] = numpy.array([float(c['volume']) for c in self.simcandles], dtype=float)

        try:
            raw = self.namespace['indicators']()
        except Exception as e:
            error = traceback.format_exc()
            sutil.runupdate("UPDATE exchangesim SET log=?, status=? WHERE id=?", (error, -1, self.simid))
            self.good = False
            return

        for indicator, ind in raw.items():
            if ind is None or isinstance(ind, (int, float, numpy.integer, numpy.floating)) or (numpy.isscalar(ind) and numpy.isnan(ind)):
                ind = numpy.array([ind], dtype=float)
            if isinstance(ind, list):
                ind = numpy.array(ind, dtype=float)
            if isinstance(ind, numpy.ndarray):
                ind = (ind,)

            i = 1
            for arr in ind:
                indname = indicator if len(ind) == 1 else f"{indicator}-{i}"
                arr = numpy.array(arr, dtype=float)
                if len(arr) < n:
                    arr = numpy.pad(arr, (n - len(arr), 0), constant_values=numpy.nan)
                elif len(arr) > n:
                    arr = arr[-n:]

                self._ind_arrays[indname] = arr

                params_list = [
                    (self.simid, candle['id'], indname,
                     None if numpy.isnan(v) else float(v),
                     candle['timestamp'])
                    for candle, v in zip(self.simcandles, arr)
                ]
                sutil.runinsertmany(
                    "INSERT INTO simindicator (exchangesimid, candleid, indname, indval, time) VALUES(?,?,?,?,?)",
                    params_list
                )
                i += 1

    def compute_total_equity(self, close_price):
        """Total portfolio equity: free margin + locked margin + unrealized PnL.
        Coinbase INTX cross-margin includes unrealized gains as available collateral."""
        usd = self.namespace['usd']
        position = self.namespace['realposition']
        costbasis = self.namespace['costbasis']
        leverage = self.namespace['leverage']
        if position == 0 or costbasis == 0:
            return usd
        locked = abs(position) * costbasis / leverage
        if position > 0:
            upnl = (close_price - costbasis) * position
        else:
            upnl = (costbasis - close_price) * abs(position)
        return usd + locked + upnl


    def margin_log_suffix(self, close_price):
        """Standard suffix appended to every position-change log entry."""
        usd = self.namespace['usd']
        position = self.namespace['realposition']
        total_equity = self.compute_total_equity(close_price)
        return (f"<br>&nbsp;&nbsp;&nbsp;<b>Free Margin:${usd:.2f}"
                f" | Total Equity:${total_equity:.2f}"
                f" | Contracts:{position:.6f}</b>")


    def has_margin_to_enter(self, close_price):
        """Returns False if free margin is below 1% of total equity."""
        usd = self.namespace['usd']
        total_equity = self.compute_total_equity(close_price)
        if total_equity <= 0:
            return False
        return usd >= total_equity * 0.01


    def autosize_notional(self, close_price, slots_remaining):
        """Auto-size notional exposure: total_equity × leverage × 99%."""
        total_equity = self.compute_total_equity(close_price)
        leverage = self.namespace['leverage']
        return (total_equity * leverage * 0.99) / max(slots_remaining, 1)


    def updatecostbasis(self, price, cryptoamount, fee):
        """
        Futures position accounting.

        cryptoamount > 0 = buying contracts (enter long or exit short)
        cryptoamount < 0 = selling contracts (enter short or exit long)
        fee = fee rate (maker or taker), applied to notional value

        Entry: margin (notional / leverage) + fee deducted from usd.
        Exit:  margin returned + PnL realized, fee deducted from usd.

        Returns: (avg_price, position_contracts, usd_balance, fee_paid, notional)
        """
        curramount = self.namespace['realposition']
        curprice = self.namespace['costbasis']
        currusd = self.namespace['usd']
        leverage = self.namespace['leverage']

        notional = abs(price * cryptoamount)
        newfee = notional * fee
        newprice = curprice
        newamount = curramount
        newusd = currusd

        if price == 0 or cryptoamount == 0:
            pass
        elif (cryptoamount > 0 and curramount >= 0) or (cryptoamount < 0 and curramount <= 0):
            # Opening or increasing a position — deduct margin + fee from free balance
            margin_required = notional / leverage
            newamount = curramount + cryptoamount
            if newamount != 0:
                newprice = (abs(curramount) * curprice + abs(cryptoamount) * price) / abs(newamount)
            else:
                newprice = price
            newusd = currusd - margin_required - newfee
        elif (cryptoamount < 0 and curramount > 0) or (cryptoamount > 0 and curramount < 0):
            # Closing or reducing a position — return margin, realize PnL, deduct fee
            closing_qty = min(abs(cryptoamount), abs(curramount))
            if curramount > 0:
                pnl = (price - curprice) * closing_qty
            else:
                pnl = (curprice - price) * closing_qty
            margin_returned = (curprice * closing_qty) / leverage
            newamount = curramount + cryptoamount
            newprice = curprice if abs(newamount) > 0 else 0.0
            newusd = currusd + margin_returned + pnl - newfee

        self.namespace['realposition'] = newamount
        self.namespace['costbasis'] = newprice
        self.namespace['usd'] = newusd
        return (newprice, newamount, newusd, newfee, notional)


    def checkliquidation(self, candle):
        """Force-closes the position if margin is wiped out (80% loss of initial margin)."""
        position = self.namespace['realposition']
        if position == 0:
            return
        costbasis = self.namespace['costbasis']
        close = float(candle['close'])
        leverage = self.namespace['leverage']
        locked_margin = abs(position) * costbasis / leverage
        if position > 0:
            unrealized_pnl = (close - costbasis) * position
        else:
            unrealized_pnl = (costbasis - close) * abs(position)
        # Liquidate when remaining equity falls below 20% of initial margin
        if locked_margin > 0 and (unrealized_pnl + locked_margin) <= locked_margin * 0.2:
            takerfee = self.namespace['takerfee']
            closing_crypt = -position
            newprice, newamount, newusd, fee, notional = self.updatecostbasis(close, closing_crypt, takerfee)
            sutil.simlog(f"LIQUIDATED at {close:.2f}! PnL:{unrealized_pnl:.2f} InitMargin:{locked_margin:.2f} NewUSD:{newusd:.2f}"
                         + self.margin_log_suffix(close))
            eventdata = {'ordertype': 'Liquidation', 'price': close, 'fee': fee,
                         'cryptodiff': closing_crypt, 'usddiff': notional,
                         'usdcurr': self.namespace['usd'], 'cryptcurr': self.namespace['realposition'],
                         'costbasis': self.namespace['costbasis']}
            sutil.runinsert("INSERT INTO simevent (exchangesimid, candleid, eventtype, eventdata, fee, metadata, time) VALUES(?,?,?,?,?,?,?)",
                            (self.simid, candle['id'], 'fill:Liquidation:Market', json.dumps(eventdata), fee, "", candle['timestamp']))
            sutil.setkeyval('simpositions', json.dumps([]))
            self.namespace['pendingpositions'] = []


    def processtick(self):
        events = []
        indicators = []
        currentcandles = self.simcandles[max(self.N-self.historysize+1, 0):self.N+1]
        self.namespace['opens'] = self.cleanarr([d['open'] for d in currentcandles])
        self.namespace['closes'] = self.cleanarr([d['close'] for d in currentcandles])
        self.namespace['highs'] = self.cleanarr([d['high'] for d in currentcandles])
        self.namespace['lows'] = self.cleanarr([d['low'] for d in currentcandles])
        self.namespace['volumes'] = self.cleanarr([d['volume'] for d in currentcandles])
        candle = currentcandles[self.namespace['N']]
        self.namespace['candle'] = candle
        self.namespace['high'] = candle['high']
        self.namespace['low'] = candle['low']
        self.namespace['open'] = candle['open']
        self.namespace['close'] = candle['close']
        self.namespace['volume'] = candle['volume']
        self.namespace['time'] = candle['timestamp']
        positions = json.loads(sutil.getkeyval('simpositions'))
        self.namespace['pendingpositions'] = positions

        dt = datetime.utcfromtimestamp(self.namespace['time'])
        ticktime = dt.strftime("%m-%d %I:%M%p")
        sutil.TickTime = ticktime

        # Check for liquidation before processing new orders
        self.checkliquidation(candle)
        positions = self.namespace['pendingpositions']  # sync in case liquidation cleared it

        calcinds = {}
        for name, full_arr in self._ind_arrays.items():
            calcinds[name] = self.cleanarr(full_arr[:self.N + 1])
        self.namespace['calcinds'] = calcinds

        if('tick' in self.namespace):
            maxpos = self.namespace['maxpositions']
            makerfee = self.namespace['makerfee']
            takerfee = self.namespace['takerfee']
            leverage = self.namespace['leverage']
            close = self.namespace['close']
            high = self.namespace['high']
            low = self.namespace['low']
            mark = float(candle['close'])

            # Update trailing limit/stop prices before fill checking
            trailing_updated = False
            for position in positions:
                ltp = float(position['limittrailpercent'])
                stp = float(position['stoptrailpercent'])
                tradetype = position['tradetype']
                cur_pos = self.namespace['realposition']

                if ltp > 0 and float(position['limitprice']) > 0:
                    cur_limit = float(position['limitprice'])
                    new_limit = None
                    if tradetype == util.TradeType.Buy.name:
                        # Buy limit is below market — trail up as price rises
                        candidate = mark * (1.0 - ltp)
                        if candidate > cur_limit:
                            new_limit = candidate
                    elif tradetype == util.TradeType.Sell.name:
                        # Sell limit is above market — trail down as price drops
                        candidate = mark * (1.0 + ltp)
                        if candidate < cur_limit:
                            new_limit = candidate
                    elif tradetype == util.TradeType.Exit.name:
                        # limitprice is activation threshold; limittrailpercent trails stop from peak
                        activated = position.get('activated', False)
                        peak = float(position.get('peak_price', 0))
                        hard_stop = float(position.get('hard_stopprice', position.get('stopprice', 0)))
                        if cur_pos > 0:
                            if not activated and mark >= cur_limit:
                                activated = True
                                peak = mark
                                position['activated'] = True
                                position['peak_price'] = peak
                                sutil.simlog(f"Trailing stop activated (long) at {mark:.2f}")
                                trailing_updated = True
                            if activated:
                                if mark > peak:
                                    peak = mark
                                    position['peak_price'] = peak
                                new_stop = peak * (1.0 - ltp)
                                if hard_stop > 0:
                                    new_stop = max(new_stop, hard_stop)
                                if new_stop != float(position['stopprice']):
                                    sutil.simlog(f"Trailing stop update [Exit long]: peak:{peak:.2f} stop:{new_stop:.2f}")
                                    position['stopprice'] = new_stop
                                    trailing_updated = True
                        elif cur_pos < 0:
                            if not activated and mark <= cur_limit:
                                activated = True
                                peak = mark
                                position['activated'] = True
                                position['peak_price'] = peak
                                sutil.simlog(f"Trailing stop activated (short) at {mark:.2f}")
                                trailing_updated = True
                            if activated:
                                if peak == 0 or mark < peak:
                                    peak = mark
                                    position['peak_price'] = peak
                                new_stop = peak * (1.0 + ltp)
                                if hard_stop > 0:
                                    new_stop = min(new_stop, hard_stop)
                                if new_stop != float(position['stopprice']):
                                    sutil.simlog(f"Trailing stop update [Exit short]: trough:{peak:.2f} stop:{new_stop:.2f}")
                                    position['stopprice'] = new_stop
                                    trailing_updated = True

                if stp > 0 and float(position['stopprice']) > 0:
                    cur_stop = float(position['stopprice'])
                    new_stop = None
                    if tradetype == util.TradeType.Buy.name:
                        # Buy stop is above market — trail down as price drops
                        candidate = mark * (1.0 + stp)
                        if candidate < cur_stop:
                            new_stop = candidate
                    elif tradetype == util.TradeType.Sell.name:
                        # Sell stop is below market — trail up as price rises
                        candidate = mark * (1.0 - stp)
                        if candidate > cur_stop:
                            new_stop = candidate
                    elif tradetype == util.TradeType.Exit.name:
                        if cur_pos > 0:
                            # Exit long stop-loss below market — classic trailing stop, trail up as price rises
                            candidate = mark * (1.0 - stp)
                            if candidate > cur_stop:
                                new_stop = candidate
                        elif cur_pos < 0:
                            # Exit short stop-loss above market — trail down as price drops
                            candidate = mark * (1.0 + stp)
                            if candidate < cur_stop:
                                new_stop = candidate
                    if new_stop is not None:
                        sutil.simlog(f"Trailing stop update [{tradetype}]: {cur_stop:.2f} → {new_stop:.2f} (close:{mark:.2f})")
                        position['stopprice'] = new_stop
                        trailing_updated = True

            if trailing_updated:
                sutil.setkeyval('simpositions', json.dumps(positions))

            # Fill any pending limit/stop orders that triggered this candle
            positionsfilled = []
            for position in positions:
                ordertype = position['ordertype']
                amount = float(position['amount'])
                stopprice = float(position['stopprice'])
                limitprice = float(position['limitprice'])
                positionid = position['id']
                tradetype = position['tradetype']
                crypt = 0
                fee = 0
                notional = 0
                filled = False

                if ordertype == util.OrderType.Limit.name or ordertype == util.OrderType.Bracket.name:
                    if tradetype == util.TradeType.Buy.name:
                        if low <= limitprice:
                            if self.namespace['realposition'] < 0:
                                liquid = abs(self.namespace['realposition'])
                                newprice, newamount, newusd, fee, notional = self.updatecostbasis(limitprice, liquid, makerfee)
                                sutil.simlog("Buy limit: closing short first"+
                                             f"<br>&nbsp;&nbsp;&nbsp;Id: {positionid}"+
                                             f"<br>&nbsp;&nbsp;&nbsp;Limit Price:{limitprice}"+
                                             f"<br>&nbsp;&nbsp;&nbsp;Fee Paid:${fee}"+
                                             f"<br>&nbsp;&nbsp;&nbsp;Contracts Closed:{liquid}"+
                                             f"<br>&nbsp;&nbsp;&nbsp;USD Balance:${newusd}"
                                             + self.margin_log_suffix(mark))
                            crypt = amount / limitprice
                            newprice, newamount, newusd, fee, notional = self.updatecostbasis(limitprice, crypt, makerfee)
                            sutil.simlog("Buy Limit filled — entering long"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Id: {positionid}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Limit Price:{limitprice}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Fee Paid:${fee}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Contracts:{crypt}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Notional:${notional}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Avg Price:{newprice}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Position:{newamount}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;USD Balance:${newusd}"
                                         + self.margin_log_suffix(mark))
                            positionsfilled.append(position)
                            filled = True
                    elif tradetype == util.TradeType.Sell.name:
                        if high >= limitprice:
                            if self.namespace['realposition'] > 0:
                                liquid = self.namespace['realposition']
                                newprice, newamount, newusd, fee, notional = self.updatecostbasis(limitprice, -liquid, makerfee)
                                sutil.simlog("Sell limit: closing long first"+
                                             f"<br>&nbsp;&nbsp;&nbsp;Id: {positionid}"+
                                             f"<br>&nbsp;&nbsp;&nbsp;Limit Price:{limitprice}"+
                                             f"<br>&nbsp;&nbsp;&nbsp;Fee Paid:${fee}"+
                                             f"<br>&nbsp;&nbsp;&nbsp;Contracts Closed:{liquid}"+
                                             f"<br>&nbsp;&nbsp;&nbsp;USD Balance:${newusd}"
                                             + self.margin_log_suffix(mark))
                            crypt = -(amount / limitprice)
                            newprice, newamount, newusd, fee, notional = self.updatecostbasis(limitprice, crypt, makerfee)
                            sutil.simlog("Sell Limit filled — entering short"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Id: {positionid}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Limit Price:{limitprice}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Fee Paid:${fee}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Contracts:{crypt}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Notional:${notional}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Avg Price:{newprice}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Position:{newamount}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;USD Balance:${newusd}"
                                         + self.margin_log_suffix(mark))
                            positionsfilled.append(position)
                            filled = True
                    elif tradetype == util.TradeType.Exit.name:
                        cur_pos = self.namespace['realposition']
                        ltp_val = float(position.get('limittrailpercent', 0))
                        # When limittrailpercent is set, limitprice is the activation threshold
                        # for the trailing stop — not a fill target. Skip limit fill in that case.
                        if ltp_val > 0:
                            pass
                        elif (cur_pos > 0 and high >= limitprice) or (cur_pos < 0 and low <= limitprice):
                            close_qty = abs(cur_pos) if amount == 0 else amount
                            crypt = -close_qty if cur_pos > 0 else close_qty
                            newprice, newamount, newusd, fee, notional = self.updatecostbasis(limitprice, crypt, makerfee)
                            direction = "Long" if cur_pos > 0 else "Short"
                            sutil.simlog(f"Exit {direction} Limit filled"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Id: {positionid}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Limit Price:{limitprice}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Fee Paid:${fee}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Contracts:{crypt}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Notional:${notional}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Avg Price:{newprice}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Position:{newamount}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;USD Balance:${newusd}"
                                         + self.margin_log_suffix(mark))
                            positionsfilled.append(position)
                            filled = True
                    if filled:
                        eventdata = {'ordertype': ordertype, 'price': limitprice, 'fee': fee, 'cryptodiff': crypt, 'usddiff': notional,
                                     'usdcurr': self.namespace['usd'], 'cryptcurr': self.namespace['realposition'],
                                     'costbasis': self.namespace['costbasis']}
                        sutil.runinsert("INSERT INTO simevent (exchangesimid, candleid, eventtype, eventdata, fee, metadata, time) VALUES(?,?,?,?,?,?,?)",
                                        (self.simid, candle['id'], 'fill:'+tradetype+':'+ordertype, json.dumps(eventdata), fee, "", candle['timestamp']))
                    filled = False

                if ordertype == util.OrderType.Stop.name or ordertype == util.OrderType.Bracket.name:
                    if tradetype == util.TradeType.Buy.name:
                        if high >= stopprice:
                            if self.namespace['realposition'] < 0:
                                liquid = abs(self.namespace['realposition'])
                                newprice, newamount, newusd, fee, notional = self.updatecostbasis(stopprice, liquid, takerfee)
                                sutil.simlog("Buy stop: closing short first"+
                                             f"<br>&nbsp;&nbsp;&nbsp;Id: {positionid}"+
                                             f"<br>&nbsp;&nbsp;&nbsp;Stop Price:{stopprice}"+
                                             f"<br>&nbsp;&nbsp;&nbsp;Fee Paid:${fee}"+
                                             f"<br>&nbsp;&nbsp;&nbsp;Contracts Closed:{liquid}"+
                                             f"<br>&nbsp;&nbsp;&nbsp;USD Balance:${newusd}"
                                             + self.margin_log_suffix(mark))
                            crypt = amount / stopprice
                            newprice, newamount, newusd, fee, notional = self.updatecostbasis(stopprice, crypt, takerfee)
                            sutil.simlog("Buy Stop filled — entering long"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Id: {positionid}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Stop Price:{stopprice}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Fee Paid:${fee}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Contracts:{crypt}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Notional:${notional}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Avg Price:{newprice}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Position:{newamount}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;USD Balance:${newusd}"
                                         + self.margin_log_suffix(mark))
                            positionsfilled.append(position)
                            filled = True
                    elif tradetype == util.TradeType.Sell.name:
                        if low <= stopprice:
                            if self.namespace['realposition'] > 0:
                                liquid = self.namespace['realposition']
                                newprice, newamount, newusd, fee, notional = self.updatecostbasis(stopprice, -liquid, takerfee)
                                sutil.simlog("Sell stop: closing long first"+
                                             f"<br>&nbsp;&nbsp;&nbsp;Id: {positionid}"+
                                             f"<br>&nbsp;&nbsp;&nbsp;Stop Price:{stopprice}"+
                                             f"<br>&nbsp;&nbsp;&nbsp;Fee Paid:${fee}"+
                                             f"<br>&nbsp;&nbsp;&nbsp;Contracts Closed:{liquid}"+
                                             f"<br>&nbsp;&nbsp;&nbsp;USD Balance:${newusd}"
                                             + self.margin_log_suffix(mark))
                            crypt = -(amount / stopprice)
                            newprice, newamount, newusd, fee, notional = self.updatecostbasis(stopprice, crypt, takerfee)
                            sutil.simlog("Sell Stop filled — entering short"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Id: {positionid}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Stop Price:{stopprice}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Fee Paid:${fee}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Contracts:{crypt}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Notional:${notional}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Avg Price:{newprice}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Position:{newamount}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;USD Balance:${newusd}"
                                         + self.margin_log_suffix(mark))
                            positionsfilled.append(position)
                            filled = True
                    elif tradetype == util.TradeType.Exit.name:
                        cur_pos = self.namespace['realposition']
                        # Stop-loss: exit long when price drops, exit short when price rises
                        if (cur_pos > 0 and low <= stopprice) or (cur_pos < 0 and high >= stopprice):
                            close_qty = abs(cur_pos) if amount == 0 else amount
                            crypt = -close_qty if cur_pos > 0 else close_qty
                            newprice, newamount, newusd, fee, notional = self.updatecostbasis(stopprice, crypt, takerfee)
                            direction = "Long" if cur_pos > 0 else "Short"
                            sutil.simlog(f"Exit {direction} Stop filled"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Id: {positionid}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Stop Price:{stopprice}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Fee Paid:${fee}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Contracts:{crypt}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Notional:${notional}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Avg Price:{newprice}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Position:{newamount}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;USD Balance:${newusd}"
                                         + self.margin_log_suffix(mark))
                            positionsfilled.append(position)
                            filled = True
                    if filled:
                        eventdata = {'ordertype': ordertype, 'price': stopprice, 'fee': fee, 'cryptodiff': crypt, 'usddiff': notional,
                                     'usdcurr': self.namespace['usd'], 'cryptcurr': self.namespace['realposition'],
                                     'costbasis': self.namespace['costbasis']}
                        sutil.runinsert("INSERT INTO simevent (exchangesimid, candleid, eventtype, eventdata, fee, metadata, time) VALUES(?,?,?,?,?,?,?)",
                                        (self.simid, candle['id'], 'fill:'+tradetype+':'+ordertype, json.dumps(eventdata), fee, "", candle['timestamp']))

            for positionfilled in positionsfilled:
                for position in positions:
                    if positionfilled['id'] == position['id']:
                        positions.remove(position)
                        break

            sutil.setkeyval('simpositions', json.dumps(positions))
            self.namespace['pendingpositions'] = positions

            # Call tick() now that fills and trailing updates are fully resolved
            events = []
            try:
                events = self.namespace['tick']()
                for event in events:
                    sutil.runinsert("INSERT INTO simevent (exchangesimid, candleid, eventtype, eventdata, fee, metadata, time) VALUES(?,?,?,?,?,?,?)",
                                    (self.simid, candle['id'], 'user:'+str(event.tradetype.name), str(event), 0.0, "", candle['timestamp']))
            except Exception as e:
                error = traceback.format_exc()
                sutil.runupdate("UPDATE exchangesim SET log=?, status=? WHERE id=?", (error, -1, self.simid))
                return False

            # Process the events returned by the user's tick function
            for event in events:
                amount = event.amount
                limitprice = event.limitprice
                stopprice = event.stopprice
                fee = event.fee
                limittrailpercent = event.limittrailpercent
                stoptrailpercent = event.stoptrailpercent
                ordertype = util.OrderType.NoOrder
                price = 0.0
                sutil.simlog(f"------Processing user request------<br>Time: {self.namespace['time']}"+
                             f"<br>Type:{event.tradetype.name} limit:{limitprice} stop:{stopprice} amount:{amount}<br>")

                if event.tradetype == util.TradeType.Buy:
                    sutil.simlog("Processing Buy")
                    if len(positions) >= maxpos:
                        sutil.simlog("Already at max pending positions")
                        continue
                    # Only block if we're not flipping from short (flip is always allowed)
                    if self.namespace['realposition'] >= 0 and not self.has_margin_to_enter(mark):
                        sutil.simlog("Less than 1% free margin available — cannot add to position")
                        continue
                    if amount == 0:
                        amount = self.autosize_notional(mark, maxpos - len(positions))
                        sutil.simlog("Auto notional (total equity): "+str(amount))
                    if amount <= 0:
                        sutil.simlog("Buy skipped — auto-size returned 0 (equity exhausted?)")
                        continue
                    if limitprice == 0 and stopprice == 0:
                        price = close
                        ordertype = util.OrderType.Market
                        sutil.simlog("Market buy at "+str(price))
                    elif stopprice == 0:
                        if limitprice < close:
                            ordertype = util.OrderType.Limit
                            sutil.simlog("Limit buy at "+str(limitprice))
                        else:
                            ordertype = util.OrderType.Market
                            price = close
                            sutil.simlog("Limit price above market — market buy at "+str(price))
                    elif limitprice == 0:
                        if stopprice > close:
                            ordertype = util.OrderType.Stop
                            sutil.simlog("Stop buy (breakout) at "+str(stopprice))
                        else:
                            ordertype = util.OrderType.Market
                            price = close
                            sutil.simlog("Stop already triggered — market buy at "+str(price))
                    else:
                        if limitprice < close and stopprice > close:
                            ordertype = util.OrderType.Bracket
                            sutil.simlog("Bracket buy order")
                        else:
                            ordertype = util.OrderType.Market
                            price = close
                            sutil.simlog("Outside bracket — market buy at "+str(price))

                elif event.tradetype == util.TradeType.Sell:
                    sutil.simlog("Processing Sell")
                    if len(positions) >= maxpos:
                        sutil.simlog("Already at max pending positions")
                        continue
                    if self.namespace['realposition'] <= 0 and not self.has_margin_to_enter(mark):
                        sutil.simlog("Less than 1% free margin available — cannot add to position")
                        continue
                    if amount == 0:
                        amount = self.autosize_notional(mark, maxpos - len(positions))
                        sutil.simlog("Auto notional (total equity): "+str(amount))
                    if amount <= 0:
                        sutil.simlog("Sell skipped — auto-size returned 0 (equity exhausted?)")
                        continue
                    if limitprice == 0 and stopprice == 0:
                        price = close
                        ordertype = util.OrderType.Market
                        sutil.simlog("Market sell at "+str(price))
                    elif stopprice == 0:
                        if limitprice > close:
                            ordertype = util.OrderType.Limit
                            sutil.simlog("Limit sell at "+str(limitprice))
                        else:
                            ordertype = util.OrderType.Market
                            price = close
                            sutil.simlog("Limit price below market — market sell at "+str(price))
                    elif limitprice == 0:
                        if stopprice < close:
                            ordertype = util.OrderType.Stop
                            sutil.simlog("Stop sell (breakdown) at "+str(stopprice))
                        else:
                            ordertype = util.OrderType.Market
                            price = close
                            sutil.simlog("Stop already triggered — market sell at "+str(price))
                    else:
                        if limitprice > close and stopprice < close:
                            ordertype = util.OrderType.Bracket
                            sutil.simlog("Bracket sell order")
                        else:
                            ordertype = util.OrderType.Market
                            price = close
                            sutil.simlog("Outside bracket — market sell at "+str(price))

                elif event.tradetype == util.TradeType.Exit:
                    sutil.simlog("Processing Exit")
                    if len(positions) >= maxpos:
                        sutil.simlog("Already at max pending positions")
                        continue
                    cur_pos = self.namespace['realposition']
                    if cur_pos == 0:
                        sutil.simlog("No open position to exit")
                        continue
                    if amount == 0:
                        amount = abs(cur_pos)
                        sutil.simlog("Exiting entire position: "+str(amount)+" contracts")
                    if limitprice == 0 and stopprice == 0:
                        price = close
                        ordertype = util.OrderType.Market
                        sutil.simlog("Market exit at "+str(price))
                    elif stopprice == 0:
                        if limittrailpercent > 0:
                            # Trailing exit: limitprice is activation threshold, no hard stop
                            if (cur_pos > 0 and limitprice > close) or (cur_pos < 0 and limitprice < close):
                                ordertype = util.OrderType.Bracket
                                sutil.simlog(f"Trailing exit order: activation at {limitprice} trail:{limittrailpercent}")
                            else:
                                ordertype = util.OrderType.Market
                                price = close
                                sutil.simlog("Trailing exit: activation already passed — market exit at "+str(price))
                        elif (cur_pos > 0 and limitprice > close) or (cur_pos < 0 and limitprice < close):
                            ordertype = util.OrderType.Limit
                            sutil.simlog("Limit exit at "+str(limitprice))
                        else:
                            ordertype = util.OrderType.Market
                            price = close
                            sutil.simlog("Limit already triggered — market exit at "+str(price))
                    elif limitprice == 0:
                        if (cur_pos > 0 and stopprice < close) or (cur_pos < 0 and stopprice > close):
                            ordertype = util.OrderType.Stop
                            sutil.simlog("Stop exit at "+str(stopprice))
                        else:
                            ordertype = util.OrderType.Market
                            price = close
                            sutil.simlog("Stop already triggered — market exit at "+str(price))
                    else:
                        if limittrailpercent > 0:
                            sutil.simlog(f"Trailing exit order: activation at {limitprice} hard stop at {stopprice} trail:{limittrailpercent}")
                        else:
                            sutil.simlog("Bracket exit order")
                        ordertype = util.OrderType.Bracket

                else:
                    sutil.simlog("Unknown trade type: "+str(event.tradetype.name))
                    continue

                new_pos = {'ordertype': ordertype.name, 'price': price, 'amount': amount,
                           'stopprice': stopprice, 'limitprice': limitprice,
                           'limittrailpercent': limittrailpercent, 'stoptrailpercent': stoptrailpercent,
                           'id': str(uuid.uuid4()), 'tradetype': event.tradetype.name,
                           'activated': False, 'peak_price': 0.0, 'hard_stopprice': float(stopprice)}
                positions.append(new_pos)
                eventdata = {'ordertype': ordertype.name, 'limitprice': limitprice, 'stopprice': stopprice,
                             'price': price, 'fee': 0, 'amount': amount,
                             'usdcurr': self.namespace['usd'], 'cryptcurr': self.namespace['realposition']}
                sutil.runinsert("INSERT INTO simevent (exchangesimid, candleid, eventtype, eventdata, fee, metadata, time) VALUES(?,?,?,?,?,?,?)",
                                (self.simid, candle['id'], 'create:'+str(event.tradetype.name)+':'+ordertype.name, json.dumps(eventdata), fee, "", candle['timestamp']))

                # Fill market orders immediately
                def checkmarketorders(position):
                    if position['ordertype'] != util.OrderType.Market.name:
                        return False
                    fill_price = float(candle['close'])
                    tradetype_name = position['tradetype']
                    pos_amount = float(position['amount'])
                    crypt = 0

                    if tradetype_name == util.TradeType.Buy.name:
                        if self.namespace['realposition'] < 0:
                            close_crypt = abs(self.namespace['realposition'])
                            newprice, newamount, newusd, cfee, cnotional = self.updatecostbasis(fill_price, close_crypt, takerfee)
                            sutil.simlog("Buy: closing short before going long"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Price:{fill_price}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Fee Paid:${cfee}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Contracts:{close_crypt}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Position:{newamount}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;USD Balance:${newusd}"
                                         + self.margin_log_suffix(fill_price))
                            eventdata = {'ordertype': position['ordertype'], 'price': fill_price, 'fee': cfee,
                                         'cryptodiff': close_crypt, 'usddiff': cnotional,
                                         'usdcurr': self.namespace['usd'], 'cryptcurr': self.namespace['realposition'],
                                         'costbasis': self.namespace['costbasis']}
                            sutil.runinsert("INSERT INTO simevent (exchangesimid, candleid, eventtype, eventdata, fee, metadata, time) VALUES(?,?,?,?,?,?,?)",
                                            (self.simid, candle['id'], 'fill:Buy:Market:CloseShort', json.dumps(eventdata), cfee, "", candle['timestamp']))
                        # Re-size after closing so we use the freed margin too
                        if pos_amount == 0:
                            pos_amount = self.namespace['usd'] * leverage * 0.99
                        crypt = pos_amount / fill_price
                        notes = "Buy: entering long"

                    elif tradetype_name == util.TradeType.Sell.name:
                        if self.namespace['realposition'] > 0:
                            close_crypt = -self.namespace['realposition']
                            newprice, newamount, newusd, cfee, cnotional = self.updatecostbasis(fill_price, close_crypt, takerfee)
                            sutil.simlog("Sell: closing long before going short"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Price:{fill_price}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Fee Paid:${cfee}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Contracts:{close_crypt}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Position:{newamount}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;USD Balance:${newusd}"
                                         + self.margin_log_suffix(fill_price))
                            eventdata = {'ordertype': position['ordertype'], 'price': fill_price, 'fee': cfee,
                                         'cryptodiff': close_crypt, 'usddiff': cnotional,
                                         'usdcurr': self.namespace['usd'], 'cryptcurr': self.namespace['realposition'],
                                         'costbasis': self.namespace['costbasis']}
                            sutil.runinsert("INSERT INTO simevent (exchangesimid, candleid, eventtype, eventdata, fee, metadata, time) VALUES(?,?,?,?,?,?,?)",
                                            (self.simid, candle['id'], 'fill:Sell:Market:CloseLong', json.dumps(eventdata), cfee, "", candle['timestamp']))
                        if pos_amount == 0:
                            pos_amount = self.namespace['usd'] * leverage * 0.99
                        crypt = -(pos_amount / fill_price)
                        notes = "Sell: entering short"

                    elif tradetype_name == util.TradeType.Exit.name:
                        cur_pos = self.namespace['realposition']
                        if cur_pos == 0:
                            return True
                        close_qty = abs(cur_pos) if pos_amount == 0 else pos_amount
                        crypt = -close_qty if cur_pos > 0 else close_qty
                        direction = "long" if cur_pos > 0 else "short"
                        notes = f"Exit: closing {direction} position"

                    else:
                        return True  # unknown type, discard

                    newprice, newamount, newusd, fee, notional = self.updatecostbasis(fill_price, crypt, takerfee)
                    sutil.simlog(notes+
                                 f"<br>&nbsp;&nbsp;&nbsp;Price:{fill_price}"+
                                 f"<br>&nbsp;&nbsp;&nbsp;Fee Paid:${fee}"+
                                 f"<br>&nbsp;&nbsp;&nbsp;Contracts:{crypt}"+
                                 f"<br>&nbsp;&nbsp;&nbsp;Notional:${notional}"+
                                 f"<br>&nbsp;&nbsp;&nbsp;Avg Price:{newprice}"+
                                 f"<br>&nbsp;&nbsp;&nbsp;Position:{newamount}"+
                                 f"<br>&nbsp;&nbsp;&nbsp;USD Balance:${newusd}"
                                 + self.margin_log_suffix(fill_price))
                    eventdata = {'ordertype': position['ordertype'], 'price': fill_price, 'fee': fee,
                                 'cryptodiff': crypt, 'usddiff': notional,
                                 'usdcurr': self.namespace['usd'], 'cryptcurr': self.namespace['realposition'],
                                 'costbasis': self.namespace['costbasis']}
                    sutil.runinsert("INSERT INTO simevent (exchangesimid, candleid, eventtype, eventdata, fee, metadata, time) VALUES(?,?,?,?,?,?,?)",
                                    (self.simid, candle['id'], 'fill:'+tradetype_name+':Market', json.dumps(eventdata), fee, "", candle['timestamp']))
                    return True

                positions = [x for x in positions if not checkmarketorders(x)]
                sutil.setkeyval('simpositions', json.dumps(positions))
                self.namespace['pendingpositions'] = positions

        self.N += 1
        self.namespace['N'] = min(self.historysize-1, self.N)
        return True


    def runsim(self):
        self._precompute_indicators()
        if not self.good:
            return False
        total = len(self.simcandles)
        while self.N < total:
            if self.cancelled:
                sutil.TickTime = "end"
                sutil.simlog("Simulation cancelled by user")
                sutil.runupdate("UPDATE exchangesim SET currenttick=?, status=? WHERE id=?", (self.N, -2, self.simid))
                return False
            if not self.processtick():
                sutil.runupdate("UPDATE exchangesim SET status=? WHERE id=?", (-1, self.simid))
                return False
            if self.N % 25 == 0:
                sutil.runupdate("UPDATE exchangesim SET currenttick=? WHERE id=?", (self.N, self.simid))
        sutil.TickTime = "end"
        final_usd = self.namespace['usd']
        final_pos = self.namespace['realposition']
        pnl = final_usd - self._SimUSDStart
        sutil.simlog(f"Sim complete — candles:{total} finalUSD:${final_usd:.2f} startUSD:${self._SimUSDStart:.2f} PnL:${pnl:.2f} openContracts:{final_pos:.6f}")
        sutil.runupdate("UPDATE exchangesim SET currenttick=?, status=? WHERE id=?", (total, 1, self.simid))
        return True
