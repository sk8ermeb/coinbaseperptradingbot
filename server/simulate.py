import util
from coinbase.rest import RESTClient
from datetime import datetime
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
        self.namespace['high']=0
        self.namespace['low'] = 0
        self.namespace['open'] = 0
        self.namespace['close'] = 0
        self.namespace['volume'] = 0
        self.namespace['time'] = 0
        self.namespace['maxpositions'] = 1
        self.namespace['pendingpositions'] = []
        self.namespace['realposition'] = 0.0
        self.namespace['costbasis'] = 0.0
        self.namespace['realspend'] = 0.0
        self.namespace['makerfee'] = 0.000
        self.namespace['takerfee'] = 0.0003
        self.namespace['usd'] = 10000.00
        #self.namespace['btc'] = 0
        #self.namespace['eth'] = 0
        self.namespace['fee'] = 0
        self.historysize = 100
        error = ""
        sutil.setkeyval('simpositions', json.dumps([]))
        try:
            exec(self.script, self.namespace)
        except Exception as e:
            error = str(traceback.format_exc().splitlines()[-2:])
            self.good = False
        if('pair' in self.namespace):
            self.pair = self.namespace['pair']
        if('granularity' in self.namespace):
            self.granularity = self.namespace['granularity']
        historicalpair = self.pair.upper()+'-PERP-INTX'
        #historicalpair = 'BTC-PERP-INTX'
        print('BTC-PERP-INTX')
        print(historicalpair)
        self.simcandles = sutil.gethistoricledata(self.granularity, historicalpair, self.start, self.stop)
        self.simid = sutil.runinsert("INSERT INTO exchangesim (log, granularity, pair, start, stop, scriptid) VALUES (?, ?, ?, ?, ?, ?)",
                                     ("", self.granularity, self.pair, start, stop, scriptid))
        sutil.setkeyval('simid', self.simid)
        if(not self.good):
            sutil.runupdate("UPDATE exchangesim SET log=?, status=? WHERE id=?", (error, -1, self.simid))
        sutil.SimID = self.simid
        #sutil.setasset('USD', 10000.00, self.simid)

        self._SimUSDStart =self.namespace['usd']
        self._SimUSDEnd =self.namespace['usd']
        self._SimTrades =0
        self._SimEntries =0
        self._SimExits =0
        self._SimMarkets =0
        self._SimLongs =0
        self._SimShorts =0
        self._SimFeeTotal =0
        self._SimProfTradeCount =0
        self._SimLossTradeCount =0
        self._SimTradeList =[]
        

    def cleanarr(self, arr):
        arr= numpy.array(arr, dtype=float)
        missing = self.historysize - len(arr)
        if(missing > 0):
            arr = numpy.pad(arr, (missing, 0), constant_values=numpy.nan)
        return arr

    def updatecostbasis(self, price, cryptoamount, fee):
        curramount = self.namespace['realposition']
        curprice = self.namespace['costbasis']
        currusd = self.namespace['usd']

        usdvalue = abs(price*cryptoamount)
        newprice = curprice
        newamount = curramount
        newusd = currusd
        newfee = usdvalue * fee

        if(price == 0 or cryptoamount == 0):
            pass
        #increase our position, either short or long. the negatives should work out find in either directiojn
        elif(cryptoamount > 0 and curramount >= 0) or (cryptoamount < 0 and curramount <= 0):
            newamount = curramount+cryptoamount
            newprice = (curramount*curprice + cryptoamount*price)/newamount
            newusd = currusd - usdvalue - newfee
        #Decrease our long or short position. Long and Short work the same way, if you bought in at 100 then return is 
        #110% at 110k long or 90k short
        elif(cryptoamount < 0 and curramount > 0) or (cryptoamount > 0 and curramount < 0):
            pricediff = price - curprice
            if(curramount < 0):
                pricediff = curprice - price
            usdvalue = curprice*abs(cryptoamount) + pricediff * abs(cryptoamount)
            newamount = curramount+cryptoamount
            newprice = currprice
            newusd = usdvalue - newfee

        
        self.namespace['realposition'] = newamount
        self.namespace['costbasis'] = newprice
        self.namespace['usd'] = newusd
        #returns current average price after fill, the total amount of crypto holdings, total usd holdings, total fee
        #for transaction, and the USD for the transaction
        return (newprice, newamount, newusd, newfee, usdvalue)



    def processtick(self):
        events = []
        indicators = []
        currentcandles = self.simcandles[max(self.N-self.historysize+1, 0):self.N+1]
        self.namespace['opens']= self.cleanarr([d['open'] for d in currentcandles])
        self.namespace['closes']= self.cleanarr([d['close'] for d in currentcandles])
        self.namespace['highs']= self.cleanarr([d['high'] for d in currentcandles])
        self.namespace['lows']= self.cleanarr([d['low'] for d in currentcandles])
        self.namespace['volumes']= self.cleanarr([d['volume'] for d in currentcandles])
        candle = currentcandles[self.namespace['N']]
        self.namespace['candle'] = candle
        self.namespace['high']=candle['high']
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
        #Calculate all the user defined indicators. It should always be returned as a list of indicators
        if('indicators' in self.namespace):
            try:
                indicators = self.namespace['indicators']()
            except Exception as e:
                error = str(traceback.format_exc().splitlines()[-2:])
                sutil.runupdate("UPDATE exchangesim SET log=?, status=? WHERE id=?", (error, -1, self.simid))
                return False
            for indicator in indicators:
                ind = indicators[indicator]
                if ind is None or isinstance(ind, (int, float, numpy.integer, numpy.floating)) or (numpy.isscalar(ind) and numpy.isnan(ind)):
                    ind = [ind]
                if isinstance(ind, list):
                    ind= numpy.array(ind, dtype=float)
                if isinstance(ind, numpy.ndarray):
                    ind = (ind,)
                i =1
                indicators[indicator] = ind
                self.indicators = indicators
                for inds in ind:
                    indname = indicator
                    if(len(ind) > 1):
                        indname = indicator +"-"+str(i)
                    res = sutil.runinsert("INSERT INTO simindicator (exchangesimid, candleid, indname, indval, time) VALUES(?,?,?,?,?)",
                                          (self.simid, candle['id'], indname, inds[-1], candle['timestamp']))
                    i+=1

        #recalculate calinds so the next user tick will have access to the latest indicator values for this tic
        simindicators = {}
        indnames = sutil.runselect("SELECT DISTINCT indname FROM simindicator WHERE exchangesimid=? ORDER BY indname", (self.simid,))
        for indname in indnames:
            name = indname['indname']
            siminddata = sutil.runselect("SELECT indval FROM simindicator WHERE exchangesimid=? AND indname=? AND indval IS NOT NULL ORDER BY time", (self.simid,name))
            indlist = [key['indval'] for key in siminddata]
            indlist = self.cleanarr(indlist)
            simindicators[name] = indlist
        self.namespace['calcinds'] = simindicators


        #this is the part of the function that would actually create a user entry or exit
        if('tick' in self.namespace):
            events = []
            try:
                events = self.namespace['tick']()
                for event in events:
                    sutil.runinsert("INSERT INTO simevent (exchangesimid, candleid, eventtype, eventdata, fee, metadata, time) VALUES(?,?,?,?,?,?,?)",
                                    (self.simid, candle['id'], 'user:'+str(event.tradetype.name), str(event), 0.0, "", candle['timestamp']))
            except Exception as e:
                error = str(traceback.format_exc().splitlines()[-2:])
                sutil.runupdate("UPDATE exchangesim SET log=?, status=? WHERE id=?", (error, -1, self.simid))
                return False
            #positions = self.namespace['pendingpositions'] 
            realposition = self.namespace['realposition'] 
            realspend = self.namespace['realspend'] 
            maxpos = self.namespace['maxpositions']
            makerfee = self.namespace['makerfee']
            takerfee = self.namespace['takerfee']
            usd = self.namespace['usd']
            close = self.namespace['close']
            high = self.namespace['high']
            low = self.namespace['low']
            pair = self.namespace['pair']
            

            #first see if any open orders will be filled
            positionsfilled = []
            for position in positions:
                ordertype = position['ordertype']
                price = float(position['price'])
                amount = float(position['amount'])
                stopprice = float(position['stopprice'])
                limitprice = float(position['limitprice'])
                limittrailpercent = float(position['limittrailpercent'])
                stoptrailpercent = float(position['stoptrailpercent'])
                positionid = position['id']
                tradetype = position['tradetype']
                crypt = 0
                fee = 0
                usd = 0
                filled = False
                if ordertype == util.OrderType.Limit.name or ordertype == util.OrderType.Bracket.name:
                    if tradetype == util.TradeType.EnterLong.name:
                        if low <= limitprice:
                            if(self.namespace['realposition'] < 0):
                                liquid = -self.namespace['realposition'] 
                                newprice, newamount, newusd, fee, usd = self.updatecostbasis(limitprice, liquid, makerfee)
                                sutil.simlog("Filling long, need to exit short first "+
                                             f"<br>&nbsp;&nbsp;&nbsp;Id: {positionid}" +
                                             f"<br>&nbsp;&nbsp;&nbsp;Limit Price:{str(limitprice)}"+
                                             f"<br>&nbsp;&nbsp;&nbsp;Maker Fee:{str(makerfee)}"+
                                             f"<br>&nbsp;&nbsp;&nbsp;Fee Paid:${str(fee)}"+
                                             f"<br>&nbsp;&nbsp;&nbsp;Crypto Change${str(liquid)}"+
                                             f"<br>&nbsp;&nbsp;&nbsp;USD Change${str(usd)}"+
                                             f"<br>&nbsp;&nbsp;&nbsp;Average Price${str(newprice)}"+
                                             f"<br>&nbsp;&nbsp;&nbsp;Crypto Holdings${str(newamount)}"+
                                             f"<br>&nbsp;&nbsp;&nbsp;USD Holdings${str(newusd)}")

                            fee = makerfee*amount
                            crypt = (amount - fee)/limitprice
                            newprice, newamount, newusd, fee, usd = self.updatecostbasis(limitprice, crypt, makerfee)
                            sutil.simlog("Enter Long Limit order filled! "+
                                         f"<br>&nbsp;&nbsp;&nbsp;Id: {positionid}" +
                                         f"<br>&nbsp;&nbsp;&nbsp;Limit Price:{str(limitprice)}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Maker Fee:{str(makerfee)}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Fee Paid:${str(fee)}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Crypto Change${str(crypt)}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;USD Change${str(usd)}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Average Price${str(newprice)}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Crypto Holdings${str(newamount)}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;USD Holdings${str(newusd)}")
                            positionsfilled.append(position)
                            filled = True
                        else:
                            pass
                    elif tradetype == util.TradeType.ExitLong.name:
                        if high >= limitprice:
                            crypt = -amount
                            newprice, newamount, newusd, fee, usd = self.updatecostbasis(limitprice, crypt, makerfee)
                            sutil.simlog("Exit Long Limit order filled!"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Id: {positionid}" +
                                         f"<br>&nbsp;&nbsp;&nbsp;Limit Price:{str(limitprice)}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Maker Fee:{str(makerfee)}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Fee Paid:${str(fee)}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Crypto Change${str(crypt)}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;USD Change${str(usd)}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Average Price${str(newprice)}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Crypto Holdings${str(newamount)}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;USD Holdings${str(newusd)}")
                            positionsfilled.append(position)
                            filled = True
                        else:
                            #price did not hit limit
                            pass
                    elif tradetype == util.TradeType.EnterShort.name:
                        if high >= limitprice:
                            fee = makerfee*amount
                            crypt = -(amount - fee)/limitprice
                            newprice, newamount, newusd, fee, usd = self.updatecostbasis(limitprice, crypt, makerfee)
                            sutil.simlog("Enter Short Limit order filled!"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Id: {positionid}" +
                                         f"<br>&nbsp;&nbsp;&nbsp;Limit Price:{str(limitprice)}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Maker Fee:{str(makerfee)}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Fee Paid:${str(fee)}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Crypto Change${str(crypt)}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;USD Change${str(usd)}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Average Price${str(newprice)}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Crypto Holdings${str(newamount)}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;USD Holdings${str(newusd)}")
                            positionsfilled.append(position)
                            filled = True
                        else:
                            #price did not hit limit
                            pass
                    elif tradetype == util.TradeType.ExitShort.name:
                        if low <= limitprice:
                            crypt = amount
                            newprice, newamount, newusd, fee, usd = self.updatecostbasis(limitprice, crypt, makerfee)
                            sutil.simlog("Exit Long Limit order filled!"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Id: {positionid}" +
                                         f"<br>&nbsp;&nbsp;&nbsp;Limit Price:{str(limitprice)}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Maker Fee:{str(makerfee)}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Fee Paid:${str(fee)}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Crypto Change${str(crypt)}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;USD Change${str(usd)}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Average Price${str(newprice)}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Crypto Holdings${str(newamount)}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;USD Holdings${str(newusd)}")
                            sutil.simlog("Exit Short Limit order filled! "+positionid)
                            positionsfilled.append(position)
                            filled = True
                        else:
                            #price did not hit limit
                            pass
                    if(filled):
                        eventdata = {'ordertype':ordertype, 'price':limitprice, 'fee':fee, 'cryptodiff':crypt, 'usddiff':usd, 
                                     'usdcurr':self.namespace['usd'], 'cryptcurr':self.namespace['realposition'], 
                                     'costbasis':self.namespace['costbasis']}
                        sutil.runinsert("INSERT INTO simevent (exchangesimid, candleid, eventtype, eventdata, fee, metadata, time) VALUES(?,?,?,?,?,?,?)",
                                        (self.simid, candle['id'], 'fill:'+str(tradetype)+':'+ordertype, json.dumps(eventdata), fee, "", candle['timestamp']))
                    filled = False 
                if ordertype == util.OrderType.Stop.name or ordertype == util.OrderType.Bracket.name:
                    if tradetype == util.TradeType.EnterLong.name:
                        if high >= stopprice:
                            crypt = ((1-makerfee)*amount)/stopprice
                            newprice, newamount, newusd, fee, usd = self.updatecostbasis(stopprice, crypt, makerfee)
                            sutil.simlog("Exit Long Limit order filled!"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Id: {positionid}" +
                                         f"<br>&nbsp;&nbsp;&nbsp;Stop Price:{str(stopprice)}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Maker Fee:{str(makerfee)}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Fee Paid:${str(fee)}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Crypto Change${str(crypt)}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;USD Change${str(usd)}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Average Price${str(newprice)}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Crypto Holdings${str(newamount)}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;USD Holdings${str(newusd)}")
                            sutil.simlog("Exit Short Limit order filled! "+positionid)
                            positionsfilled.append(position)
                            filled = True
                        else:
                            #price did not hit stop
                            pass
                    elif tradetype == util.TradeType.ExitLong.name:
                        if low <= stopprice:
                            crypt = amount
                            newprice, newamount, newusd, fee, usd = self.updatecostbasis(stopprice, crypt, makerfee)
                            sutil.simlog("Exit Long Limit order filled!"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Id: {positionid}" +
                                         f"<br>&nbsp;&nbsp;&nbsp;Stop Price:{str(stopprice)}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Maker Fee:{str(makerfee)}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Fee Paid:${str(fee)}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Crypto Change${str(crypt)}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;USD Change${str(usd)}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Average Price${str(newprice)}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Crypto Holdings${str(newamount)}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;USD Holdings${str(newusd)}")
                            positionsfilled.append(position)
                            filled = True
                        else:
                            #price did not hit limit
                            pass
                    elif tradetype == util.TradeType.EnterShort.name:
                        if low <= stopprice:
                            crypt = ((1-makerfee)*amount)/stopprice
                            newprice, newamount, newusd, fee, usd = self.updatecostbasis(stopprice, crypt, makerfee)
                            sutil.simlog("Exit Long Limit order filled!"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Id: {positionid}" +
                                         f"<br>&nbsp;&nbsp;&nbsp;Stop Price:{str(stopprice)}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Maker Fee:{str(makerfee)}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Fee Paid:${str(fee)}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Crypto Change${str(crypt)}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;USD Change${str(usd)}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Average Price${str(newprice)}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Crypto Holdings${str(newamount)}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;USD Holdings${str(newusd)}")
                            positionsfilled.append(position)
                            filled = True
                        else:
                            #price did not hit limit
                            pass
                    elif tradetype == util.TradeType.ExitShort.name:
                        if high >= stopprice:
                            crypt = amount
                            newprice, newamount, newusd, fee, usd = self.updatecostbasis(stopprice, crypt, makerfee)
                            sutil.simlog("Exit Long Limit order filled!"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Id: {positionid}" +
                                         f"<br>&nbsp;&nbsp;&nbsp;Stop Price:{str(stopprice)}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Maker Fee:{str(makerfee)}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Fee Paid:${str(fee)}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Crypto Change${str(crypt)}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;USD Change${str(usd)}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Average Price${str(newprice)}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Crypto Holdings${str(newamount)}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;USD Holdings${str(newusd)}")
                            positionsfilled.append(position)
                            filled = True
                        else:
                            pass
                            #price did not hit limit
                    if(filled):
                        eventdata = {'ordertype':ordertype, 'price':stopprice, 'fee':fee, 'cryptodiff':crypt, 'usddiff':usd, 
                                     'usdcurr':self.namespace['usd'], 'cryptcurr':self.namespace['realposition'], 
                                     'costbasis':self.namespace['costbasis']}
                        sutil.runinsert("INSERT INTO simevent (exchangesimid, candleid, eventtype, eventdata, fee, metadata, time) VALUES(?,?,?,?,?,?,?)",
                                        (self.simid, candle['id'], 'fill:'+str(tradetype)+':'+ordertype, json.dumps(eventdata), fee, "", candle['timestamp']))
            for positionfilled in positionsfilled:
                for position in positions:
                    if(positionfilled['id'] == position['id']):
                        positions.remove(position)
                        break

            sutil.setkeyval('simpositions', json.dumps(positions))
            self.namespace['pendingpositions'] = positions

            #Now Process the events returned by the user

            for event in events:
                amount = event.amount
                limitprice = event.limitprice
                stopprice = event.stopprice
                fee = event.fee
                limittrailpercent = event.limittrailpercent
                stoptrailpercent = event.stoptrailpercent
                ordertype = util.OrderType.NoOrder
                price = 0.0
                create = False
                sutil.simlog(f"------Processing user request------<br>Time: {self.namespace['time']}"+
                             f"limit price:{limitprice}<br>stop price:{stopprice} <br>"+
                             f"amount:{amount}<br>")

                if event.tradetype == util.TradeType.EnterLong:
                    sutil.simlog("Processing request to enter long")
                    if len(positions)>=maxpos:
                        sutil.simlog("Already at max pending positions")
                        continue
                    #if self.namespace['realposition'] < 0:
                    #    sutil.simlog("Can't enter long with an active short position")
                    #    continue
                    if amount ==0:
                        amount = (usd*0.99)/(maxpos - len(positions))
                        sutil.simlog( "amount not set. calculating based on remaining possible positions of "+str(amount))
                    if limitprice == 0 and stopprice == 0:
                        price = close
                        ordertype = util.OrderType.Market
                        sutil.simlog( "Price not set. Entering long at market rate of "+str(price))
                    elif stopprice == 0:
                        if limitprice < close:
                            ordertype = util.OrderType.Limit
                            sutil.simlog( "Price above limit price entering a limit long")
                        elif limitprice >= close:
                            ordertype = util.OrderType.Market
                            price = close
                            sutil.simlog( "Price already below limit price. Entering market long order at close price ")
                    elif limitprice == 0:
                        if stopprice > close:
                            ordertype = util.OrderType.Stop
                            sutil.simlog( "Price below close entering a stop long")
                        elif limitprice <= close:
                            ordertype = util.OrderType.Market
                            price = close
                            sutil.simlog( "Price already above stop price. Entering market long order at close price ")
                    else:
                        if limitprice < close and stopprice > close:
                            ordertype = util.OrderType.Bracket
                            sutil.simlog( "Price inbetween stop and limit. Creating bracket entry long order")
                        else:
                            ordertype = util.OrderType.Market
                            price = close
                            sutil.simlog( "Price outside bracket limit or stop. Creating market long order at current close")

                if event.tradetype == util.TradeType.ExitLong:
                    sutil.simlog("Processing request to exit long")
                    if len(positions)>=maxpos:
                        sutil.simlog( "Already at max pending positions")
                        continue
                    if self.namespace['realposition'] < 0:
                        sutil.simlog( "Can't exit long with an active short position")
                        continue
                    if amount ==0:
                        amount = self.namespace['realposition']
                        sutil.simlog( "amount not set. Exiting entire long position of "+str(amount))
                    if limitprice == 0 and stopprice == 0:
                        price = close
                        ordertype = util.OrderType.Market
                        sutil.simlog( "Price not set. Exiting long at market rate of "+str(price))
                    elif stopprice == 0:
                        if limitprice > close:
                            ordertype = util.OrderType.Limit
                            sutil.simlog( "Price below limit price creating an exit limit long")
                        elif limitprice >= close:
                            ordertype = util.OrderType.Market
                            price = close
                            sutil.simlog( "Price already above limit price. Exiting market long order at close price ")
                    elif limitprice == 0:
                        if stopprice < close:
                            ordertype = util.OrderType.Stop
                            sutil.simlog( "Price above close creating an exit stop long")
                        elif limitprice >= close:
                            ordertype = util.OrderType.Market
                            price = close
                            sutil.simlog( "Price already below stop price. Exiting market exit long order at close price ")
                    else:
                        if limitprice > close and stopprice < close:
                            ordertype = util.OrderType.Bracket
                            sutil.simlog( "Price in between stop and limit. Creating bracket exit long order")
                        else:
                            ordertype = util.OrderType.Market
                            price = close
                            sutil.simlog( "Price outside bracket limit or stop. Creating market order at current close")

                if event.tradetype == util.TradeType.EnterShort:
                    sutil.simlog("Processing request to enter short position")
                    if len(positions)>=maxpos:
                        sutil.simlog( "Already at max pending positions")
                        continue
                    #if self.namespace['realposition'] > 0:
                    #    sutil.simlog( "Can't enter short with an active long position")
                    #    continue
                    if amount ==0:
                        amount = (usd*0.99)/(maxpos - len(positions))
                        sutil.simlog( "amount not set. calculating based on remaining possible positions of "+str(amount))
                    if limitprice == 0 and stopprice == 0:
                        price = close
                        ordertype = util.OrderType.Market
                        sutil.simlog( "Price not set. Entering short at market rate of "+str(price))
                    elif stopprice == 0:
                        if limitprice > close:
                            ordertype = util.OrderType.Limit
                            sutil.simlog( "Price below limit price entering a limit short")
                        elif limitprice <= close:
                            ordertype = util.OrderType.Market
                            price = close
                            sutil.simlog( "Price already above limit price. Entering market short order at close price ")
                    elif limitprice == 0:
                        if stopprice < close:
                            ordertype = util.OrderType.Stop
                            sutil.simlog( "Price above close entering a stop short")
                        elif limitprice <= close:
                            ordertype = util.OrderType.Market
                            price = close
                            sutil.simlog( "Price already below stop price. Entering market short order at close price ")
                    else:
                        if limitprice > close and stopprice < close:
                            ordertype = util.OrderType.Bracket
                            sutil.simlog( "Price inbetween stop and limit. Creating bracket short entry order")
                        else:
                            ordertype = util.OrderType.Market
                            price = close
                            sutil.simlog( "Price outside bracket limit or stop. Creating market short order at current close")

                
                if event.tradetype == util.TradeType.ExitShort:
                    sutil.simlog( "Processing request to exit short")
                    if len(positions)>=maxpos:
                        sutil.simlog( "Already at max pending positions")
                        continue
                    if self.namespace['realposition'] > 0:
                        sutil.simlog( "Can't exit short with an active long position")
                        continue
                    if amount ==0:
                        amount = self.namespace['realposition']
                        sutil.simlog( "amount not set. Using entire short position of "+str(amount))
                    if limitprice == 0 and stopprice == 0:
                        price = close
                        ordertype = util.OrderType.Market
                        sutil.simlog( "Price not set. Exiting short at market rate of "+str(price))
                    elif stopprice == 0:
                        if limitprice < close:
                            ordertype = util.OrderType.Limit
                            sutil.simlog( "Price above limit price exiting a limit short")
                        elif limitprice >= close:
                            ordertype = util.OrderType.Market
                            price = close
                            sutil.simlog( "Price already below limit price. Exiting market short order at close price ")
                    elif limitprice == 0:
                        if stopprice > close:
                            ordertype = util.OrderType.Stop
                            sutil.simlog( "Price below close exiting a stop short")
                        elif limitprice <= close:
                            ordertype = util.OrderType.Market
                            price = close
                            sutil.simlog( "Price already above stop price. Exiting market short order at close price ")
                    else:
                        if limitprice < close and stopprice > close:
                            ordertype = util.OrderType.Bracket
                            sutil.simlog( "Price inbetween stop and limit. Creating bracket exit short order")
                        else:
                            ordertype = util.OrderType.Market
                            price = close
                            sutil.simlog( "Price outside bracket limit or stop. Creating market exit short order at current close")

                
                
                positions.append({'ordertype':ordertype.name, 'price':price, 'amount':amount, 
                                  'stopprice':stopprice, 'limitprice':limitprice, 'limittrailpercent':limittrailpercent,
                                  'stoptrailpercent':stoptrailpercent, 'id':str(uuid.uuid4()), 'tradetype':event.tradetype.name})
                eventdata = {'ordertype':ordertype.name, 'limitprice':limitprice, 'stopprice':stopprice, 'price':price, 'fee':0, 'amount':amount, 
                             'usdcurr':self.namespace['usd'], 'cryptcurr':self.namespace['realposition']}
                sutil.runinsert("INSERT INTO simevent (exchangesimid, candleid, eventtype, eventdata, fee, metadata, time) VALUES(?,?,?,?,?,?,?)",
                                (self.simid, candle['id'], 'create:'+str(event.tradetype.name)+':'+ordertype.name, json.dumps(eventdata), fee, "", candle['timestamp']))
               
                #Finally fill any market orders immidiately

                def checkmarketorders(position):
                    if position['ordertype'] == util.OrderType.Market.name:
                        usd = 0
                        crypt = 0
                        fee = 0
                        price = 0
                        price = candle['close']
                        notes = ''
                        if position['tradetype'] == util.TradeType.EnterLong.name:
                            notes = "Entered long market order"
                            crypt = ((1-takerfee)*position['amount'])/price
                        elif position['tradetype'] == util.TradeType.ExitLong.name:
                            notes = "Exiting Long Market Order"
                            crypt = -position['amount']
                        if position['tradetype'] == util.TradeType.EnterShort.name:
                            notes = "Entering Short Market Order"
                            crypt = -((1-taklerfee)*position['amount'])/candle['close']
                        elif position['tradetype'] == util.TradeType.ExitShort.name:
                            notes = "Exiting Short Market Order"
                            crypt = -position['amount']
                       
                        if((position['tradetype'] == util.TradeType.EnterLong.name and self.namespace['realposition'] < 0) or
                           (position['tradetype'] == util.TradeType.EnterShort.name and self.namespace['realposition'] > 0)):
                            liquidamount = -self.namespace['realposition']
                            newprice, newamount, newusd, fee, usd = self.updatecostbasis(price, liquidamount, takerfee)
                            sellfirst = "Exiting short position and entering long at once. Shown as 2 separate transactions" 
                            if(position['tradetype'] == util.TradeType.EnterLong.name):
                                sellfirst = "Exiting long position and entering short at once. Shown as 2 separate transactions" 

                            sutil.simlog(sellfirst+
                                         f"<br>&nbsp;&nbsp;&nbsp;Limit Price:{str(price)}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Maker Fee:{str(takerfee)}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Fee Paid:${str(fee)}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Crypto Change${str(liquidamount)}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;USD Change${str(usd)}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Average Price${str(newprice)}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;Crypto Holdings${str(newamount)}"+
                                         f"<br>&nbsp;&nbsp;&nbsp;USD Holdings${str(newusd)}")
                            eventdata = {'ordertype':ordertype.name, 'price':price, 'fee':fee, 'cryptodiff':crypt, 'usddiff':usd, 
                                         'usdcurr':self.namespace['usd'], 'cryptcurr':self.namespace['realposition'], 
                                         'costbasis':self.namespace['costbasis']}
                            sutil.runinsert("INSERT INTO simevent (exchangesimid, candleid, eventtype, eventdata, fee, metadata, time) VALUES(?,?,?,?,?,?,?)",
                                            (self.simid, candle['id'], 'fill:'+str(event.tradetype.name)+':'+ordertype.name, json.dumps(eventdata), fee, "", candle['timestamp']))



                        newprice, newamount, newusd, fee, usd = self.updatecostbasis(price, crypt, takerfee)
                        sutil.simlog(notes+
                                     f"<br>&nbsp;&nbsp;&nbsp;Limit Price:{str(price)}"+
                                     f"<br>&nbsp;&nbsp;&nbsp;Maker Fee:{str(takerfee)}"+
                                     f"<br>&nbsp;&nbsp;&nbsp;Fee Paid:${str(fee)}"+
                                     f"<br>&nbsp;&nbsp;&nbsp;Crypto Change${str(crypt)}"+
                                     f"<br>&nbsp;&nbsp;&nbsp;USD Change${str(usd)}"+
                                     f"<br>&nbsp;&nbsp;&nbsp;Average Price${str(newprice)}"+
                                     f"<br>&nbsp;&nbsp;&nbsp;Crypto Holdings${str(newamount)}"+
                                     f"<br>&nbsp;&nbsp;&nbsp;USD Holdings${str(newusd)}")

                        eventdata = {'ordertype':ordertype.name, 'price':price, 'fee':fee, 'cryptodiff':crypt, 'usddiff':usd, 
                                     'usdcurr':self.namespace['usd'], 'cryptcurr':self.namespace['realposition'], 
                                     'costbasis':self.namespace['costbasis']}
                        sutil.runinsert("INSERT INTO simevent (exchangesimid, candleid, eventtype, eventdata, fee, metadata, time) VALUES(?,?,?,?,?,?,?)",
                                        (self.simid, candle['id'], 'fill:'+str(event.tradetype.name)+':'+ordertype.name, json.dumps(eventdata), fee, "", candle['timestamp']))
                        return True

                    return False

                positions = [x for x in positions if not checkmarketorders(x)]
                sutil.setkeyval('simpositions', json.dumps(positions))
                self.namespace['pendingpositions'] = positions

        

        self.N += 1
        self.namespace['N'] = min(self.historysize-1, self.N)
        return True



    def runsim(self):
        while(self.N < len(self.simcandles)):
            if not self.processtick():
                return False
        return True
