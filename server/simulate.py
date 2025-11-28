import util
from coinbase.rest import RESTClient
from datetime import datetime
import json
import talib
import numpy
import traceback
from enum import Enum
sutil = util.util()
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
        self.namespace['pair'] = "BTC-USD"
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
        self.namespace['usd'] = 10000.00
        self.historysize = 100
        error = ""
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

        self.namespace['calcinds'] = self.indicators
        if('tick' in self.namespace):
            try:
                events = self.namespace['tick']()
                print(events)
                for event in events:
                    print(event)
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
