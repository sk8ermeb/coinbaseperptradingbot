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
#client = sutil.getclient()


#product_id = "BTC-USD"
#start = int(datetime(2025, 7, 21).timestamp())
#end = int(datetime(2025, 7, 22).timestamp())
#granularity = "ONE_HOUR"

#candles = gethistoricledata(granularity, product_id, start, end)
#sutil.runupdate("DELETE FROM candle;",())

#Market order, Limit order, Stop-limit order, Bracket order, Take Profit/Stop Loss order, TWAP order



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
        self.namespace['makerfee'] = 0.0003
        self.namespace['takerfee'] = 0.0001
        self.namespace['usd'] = 10000.00
        self.namespace['btc'] = 0
        self.namespace['eth'] = 0
        self.namespace['fee'] = 0
        self.historysize = 100
        error = ""
        util.setkeyval('simpositions', json.dumps([]))
        try:
            exec(self.script, self.namespace)
        except Exception as e:
            error = str(traceback.format_exc().splitlines()[-2:])
            self.good = False
        if('pair' in self.namespace):
            self.pair = self.namespace['pair']
        if('granularity' in self.namespace):
            self.granularity = self.namespace['granularity']


        self.simcandles = sutil.gethistoricledata(self.granularity, self.pair, self.start, self.stop)
        self.simid = sutil.runinsert("INSERT INTO exchangesim (log, granularity, pair, start, stop, scriptid) VALUES (?, ?, ?, ?, ?, ?)",
                                     ("", self.granularity, self.pair, start, stop, scriptid))
        sutil.setkeyval('simid', self.simid)
        if(not self.good):
            sutil.runupdate("UPDATE exchangesim SET log=?, status=? WHERE id=?", (error, -1, self.simid))

        sutil.setasset('USD', 10000.00, self.simid)

    def cleanarr(self, arr):
        arr= numpy.array(arr, dtype=float)
        missing = self.historysize - len(arr)
        if(missing > 0):
            arr = numpy.pad(arr, (missing, 0), constant_values=numpy.nan)
        return arr


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

        positions = json.loads(util.getkeyval('simpositions'))
        self.namespace['pendingpositions'] = positions
        
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
            try:
                events = self.namespace['tick']()
                for event in events:
                    sutil.runinsert("INSERT INTO simevent (exchangesimid, candleid, eventtype, eventdata, fee, metadata, time) VALUES(?,?,?,?,?,?,?)",
                                    (self.simid, candle['id'], str(event.tradetype), str(event), 0.0, "", candle['timestamp']))
                #positions = self.namespace['pendingpositions'] 
                realposition = self.namespace['realposition'] 
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

                    #positions.append({'ordertype':ordertype.name, 'price':price, 'amount':amount, 'side':side, 
                    #                  'stopprice':stopprice, 'limitprice':limitprice, 'limittrailpercent':limittrailpercent,
                    #                  'stoptrailpercent':stoptrailpercent, 'id':str(uuid.uuid4()), 'tradetype':event.tradetype.name})
                    if position['ordertype'] == util.OrderType.Limit.name:
                        if position['side'] == 'buy':
                            if position['tradetype'] == TradeType.EnterLong.name:
                                if candle['low'] <= position['price']:
                                    crypt = ((1-makerfee)*position['amount'])/position['price']
                                    fee = makerfee*position['amount']
                                    self.namespace['usd'] -= position['amount']
                                    self.namespace[pair] += crypt
                                    print("Enter Long Limit order filled! "+position['id'])
                                    positionsfilled.append(position)
                                else:
                                    #price did not hit limit
                                    pass
                            elif position['tradetype'] == TradeType.ExitLong.name:
                                if candle['high'] >= position['price']:
                                    usd = position['amount']*position['price']
                                    fee = makerfee*usd
                                    usd = usd*(1-makerfee)
                                    self.namespace['usd'] += position['amount']
                                    self.namespace[pair] -= crypt
                                    print("Exit Long Limit order filled! "+position['id'])
                                    positionsfilled.append(position)
                                else:
                                    #price did not hit limit
                                    pass
                        elif position['side'] == 'sell':
                            if position['tradetype'] == TradeType.EnterShort.name:
                                if candle['high'] >= position['price']:
                                    crypt = ((1-makerfee)*position['amount'])/position['price']
                                    fee = makerfee*position['amount']
                                    self.namespace['usd'] -= position['amount']
                                    self.namespace[pair] += crypt
                                    print("Enter Short Limit order filled! "+position['id'])
                                    positionsfilled.append(position)
                                else:
                                    #price did not hit limit
                                    pass
                            elif position['tradetype'] == TradeType.ExitShort.name:
                                if candle['low'] <= position['price']:
                                    usd = position['amount']*position['price']
                                    fee = makerfee*usd
                                    usd = usd*(1-makerfee)
                                    self.namespace['usd'] += position['amount']
                                    self.namespace[pair] -= crypt
                                    print("Exit Short Limit order filled! "+position['id'])
                                    positionsfilled.append(position)
                                else:
                                    #price did not hit limit
                                    pass

                    if position['ordertype'] == util.OrderType.Stop.name:
                        if position['side'] == 'buy':
                            if position['tradetype'] == TradeType.EnterLong.name:
                                if candle['high'] >= position['price']:
                                    crypt = ((1-makerfee)*position['amount'])/position['price']
                                    fee = makerfee*position['amount']
                                    self.namespace['usd'] -= position['amount']
                                    self.namespace[pair] += crypt
                                    print("Enter Long Stop order filled! "+position['id'])
                                    positionsfilled.append(position)
                                else:
                                    #price did not hit stop
                                    pass
                            elif position['tradetype'] == TradeType.ExitLong.name:
                                if candle['low'] <= position['price']:
                                    usd = position['amount']*position['price']
                                    fee = makerfee*usd
                                    usd = usd*(1-makerfee)
                                    self.namespace['usd'] += position['amount']
                                    self.namespace[pair] -= crypt
                                    print("Exit Long Stop order filled! "+position['id'])
                                    positionsfilled.append(position)
                                else:
                                    #price did not hit limit
                                    pass
                        elif position['side'] == 'sell':
                            if position['tradetype'] == TradeType.EnterShort.name:
                                if candle['low'] <= position['price']:
                                    crypt = ((1-makerfee)*position['amount'])/position['price']
                                    fee = makerfee*position['amount']
                                    self.namespace['usd'] -= position['amount']
                                    self.namespace[pair] += crypt
                                    print("Enter Short Stop order filled! "+position['id'])
                                    positionsfilled.append(position)
                                else:
                                    #price did not hit limit
                                    pass
                            elif position['tradetype'] == TradeType.ExitShort.name:
                                if candle['high'] >= position['price']:
                                    usd = position['amount']*position['price']
                                    fee = makerfee*usd
                                    usd = usd*(1-makerfee)
                                    self.namespace['usd'] += position['amount']
                                    self.namespace[pair] -= crypt
                                    print("Exit Short Stop order filled! "+position['id'])
                                    positionsfilled.append(position)
                                else:
                                    pass
                                    #price did not hit limit
                    #TODO need to do bracket order which are just a combination of limit and stop. 


                for event in events:
                    amount = event.amount
                    limitprice = event.limitprice
                    stopprice = event.stopprice
                    fee = event.fee
                    limittrailpercent = event.limittrailpercent
                    stoptrailpercent = event.stoptrailpercent
                    ordertype = util.OrderType.NoOrder
                    price = 0.0
                    side = 'buy'
                    if event.tradetype == util.TradeType.EnterLong:
                        side = 'buy'
                        if len(positions)>=maxpos:
                            print("Already at max pending positions")
                            continue
                        if realposition < 0:
                            print("Can't enter long with an active short position")
                            continue
                        if amount ==0:
                            amount = (usd*0.99)/(maxpos - len(positions))
                            print("amount not set. calculating based on remaining possible positions of "+str(amount))
                        if limitprice == 0 and stopprice == 0:
                            price = close
                            ordertype = util.OrderType.Market
                            print("Price not set. Entering long at market rate of "+str(price))
                        elif stopprice == 0:
                            if limitprice < close:
                                ordertype = util.OrderType.Limit
                                print("Price above limit price entering a limit long")
                            elif limitprice >= close:
                                ordertype = util.OrderType.Market
                                price = close
                                print("Price already below limit price. Entering market long order at close price ")
                        elif limitprice == 0:
                            if stopprice > close:
                                ordertype = util.OrderType.Stop
                                print("Price below close entering a stop long")
                            elif limitprice <= close:
                                ordertype = util.OrderType.Market
                                price = close
                                print("Price already above stop price. Entering market long order at close price ")
                        else:
                            if limitprice < close and stopprice > close:
                                ordertype = util.OrderType.Bracket
                                print("Price inbetween stop and limit. Creating bracket entry long order")
                            else:
                                ordertype = util.OrderType.Market
                                price = close
                                print("Price outside bracket limit or stop. Creating market long order at current close")

                    if event.tradetype == util.TradeType.ExitLong:
                        side = 'buy'
                        if len(positions)>=maxpos:
                            print("Already at max pending positions")
                            continue
                        if realposition < 0:
                            print("Can't exit long with an active short position")
                            continue
                        if amount ==0:
                            amount = realposition
                            print("amount not set. Exiting entire long position of "+str(amount))
                        if limitprice == 0 and stopprice == 0:
                            price = close
                            ordertype = util.OrderType.Market
                            print("Price not set. Exiting long at market rate of "+str(price))
                        elif stopprice == 0:
                            if limitprice > close:
                                ordertype = util.OrderType.Limit
                                print("Price below limit price creating an exit limit long")
                            elif limitprice >= close:
                                ordertype = util.OrderType.Market
                                price = close
                                print("Price already above limit price. Exiting market long order at close price ")
                        elif limitprice == 0:
                            if stopprice < close:
                                ordertype = util.OrderType.Stop
                                print("Price above close creating an exit stop long")
                            elif limitprice >= close:
                                ordertype = util.OrderType.Market
                                price = close
                                print("Price already below stop price. Exiting market exit long order at close price ")
                        else:
                            if limitprice > close and stopprice < close:
                                ordertype = util.OrderType.Bracket
                                print("Price in between stop and limit. Creating bracket exit long order")
                            else:
                                ordertype = util.OrderType.Market
                                price = close
                                print("Price outside bracket limit or stop. Creating market order at current close")

                    
                    if event.tradetype == util.TradeType.EnterShort:
                        side = 'sell'
                        if len(positions)>=maxpos:
                            print("Already at max pending positions")
                            continue
                        if realposition > 0:
                            print("Can't enter short with an active long position")
                            continue
                        if amount ==0:
                            amount = (usd*0.99)/(maxpos - len(positions))
                            print("amount not set. calculating based on remaining possible positions of "+str(amount))
                        if limitprice == 0 and stopprice == 0:
                            price = close
                            ordertype = util.OrderType.Market
                            print("Price not set. Entering short at market rate of "+str(price))
                        elif stopprice == 0:
                            if limitprice > close:
                                ordertype = util.OrderType.Limit
                                print("Price below limit price entering a limit short")
                            elif limitprice <= close:
                                ordertype = util.OrderType.Market
                                price = close
                                print("Price already above limit price. Entering market short order at close price ")
                        elif limitprice == 0:
                            if stopprice < close:
                                ordertype = util.OrderType.Stop
                                print("Price above close entering a stop short")
                            elif limitprice <= close:
                                ordertype = util.OrderType.Market
                                price = close
                                print("Price already below stop price. Entering market short order at close price ")
                        else:
                            if limitprice > close and stopprice < close:
                                ordertype = util.OrderType.Bracket
                                print("Price inbetween stop and limit. Creating bracket short entry order")
                            else:
                                ordertype = util.OrderType.Market
                                price = close
                                print("Price outside bracket limit or stop. Creating market short order at current close")

                    
                    if event.tradetype == util.TradeType.ExitShort:
                        side = 'buy'
                        if len(positions)>=maxpos:
                            print("Already at max pending positions")
                            continue
                        if realposition > 0:
                            print("Can't exit short with an active long position")
                            continue
                        if amount ==0:
                            amount = realposition
                            print("amount not set. Using entire short position of "+str(amount))
                        if limitprice == 0 and stopprice == 0:
                            price = close
                            ordertype = util.OrderType.Market
                            print("Price not set. Exiting short at market rate of "+str(price))
                        elif stopprice == 0:
                            if limitprice < close:
                                ordertype = util.OrderType.Limit
                                print("Price above limit price exiting a limit short")
                            elif limitprice >= close:
                                ordertype = util.OrderType.Market
                                price = close
                                print("Price already below limit price. Exiting market short order at close price ")
                        elif limitprice == 0:
                            if stopprice > close:
                                ordertype = util.OrderType.Stop
                                print("Price below close exiting a stop short")
                            elif limitprice <= close:
                                ordertype = util.OrderType.Market
                                price = close
                                print("Price already above stop price. Exiting market short order at close price ")
                        else:
                            if limitprice < close and stopprice > close:
                                ordertype = util.OrderType.Bracket
                                print("Price inbetween stop and limit. Creating bracket exit short order")
                            else:
                                ordertype = util.OrderType.Market
                                price = close
                                print("Price outside bracket limit or stop. Creating market exit short order at current close")

                    
                    
                    positions.append({'ordertype':ordertype.name, 'price':price, 'amount':amount, 'side':side, 
                                      'stopprice':stopprice, 'limitprice':limitprice, 'limittrailpercent':limittrailpercent,
                                      'stoptrailpercent':stoptrailpercent, 'id':str(uuid.uuid4()), 'tradetype':event.tradetype.name})
                    
                    def checkmarketorders(position):
                        if position['ordertype'] == util.OrderType.Market.name:
                            if position['side'] == 'buy':
                                if position['tradetype'] == TradeType.EnterLong.name:
                                    crypt = ((1-makerfee)*position['amount'])/candle['close']
                                    fee = makerfee*position['amount']
                                    self.namespace['usd'] -= position['amount']
                                    self.namespace[pair] += crypt
                                elif position['tradetype'] == TradeType.ExitLong.name:
                                    usd = position['amount']*candle['close']
                                    fee = makerfee*usd
                                    usd = usd*(1-makerfee)
                                    self.namespace['usd'] += position['amount']
                                    self.namespace[pair] -= crypt
                            elif position['side'] == 'sell':
                                if position['tradetype'] == TradeType.EnterShort.name:
                                    crypt = ((1-makerfee)*position['amount'])/candle['close']
                                    fee = makerfee*position['amount']
                                    self.namespace['usd'] -= position['amount']
                                    self.namespace[pair] += crypt
                                elif position['tradetype'] == TradeType.ExitShort.name:
                                    usd = position['amount']*candle['close']
                                    fee = makerfee*usd
                                    usd = usd*(1-makerfee)
                                    self.namespace['usd'] += position['amount']
                                    self.namespace[pair] -= crypt
                            return True

                        return False

                    positions = [x for x in positions if not checkmarketorders(x)]
                    util.setkeyval('simpositions', json.dumps(positions))
                    self.namespace['pendingpositions'] = positions

                        

            except Exception as e:
                error = str(traceback.format_exc().splitlines()[-2:])
                sutil.runupdate("UPDATE exchangesim SET log=?, status=? WHERE id=?", (error, -1, self.simid))
                return False
        
        

        self.N += 1
        self.namespace['N'] = min(self.historysize-1, self.N)
        return True



    def runsim(self):
        while(self.N < len(self.simcandles)):
            if not self.processtick():
                return False
        return True
