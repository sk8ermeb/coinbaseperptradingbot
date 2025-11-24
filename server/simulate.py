import util
from coinbase.rest import RESTClient
from datetime import datetime
import json
import talib
import numpy

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
        self.N = 0
        scripts = sutil.runselect("SELECT * FROM scripts WHERE id=?",(self.scriptid,))
        self.script = scripts[0]['script']
        self.namespace = {}
        self.namespace['talib'] = talib
        self.namespace['numpy'] = numpy
        self.namespace['granularity'] = "ONE_HOUR"
        self.namespace['pair'] = "BTC-USD"
        self.namespace['N'] = 0
        self.namespace['opens'] = []
        self.namespace['closes'] = []
        self.namespace['highs'] = []
        self.namespace['lows'] = []
        self.namespace['valumes'] = []
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
        exec(self.script, self.namespace)
        if('pair' in self.namespace):
            self.pair = self.namespace['pair']
        if('granularity' in self.namespace):
            self.granularity = self.namespace['granularity']


        self.simcandles = sutil.gethistoricledata(self.granularity, self.pair, self.start, self.stop)
        self.simid = sutil.runinsert("INSERT INTO exchangesim (log, granularity, pair, start, stop, scriptid) VALUES (?, ?, ?, ?, ?, ?)",
                                     ("", self.granularity, self.pair, start, stop, scriptid))
        sutil.setkeyval('simid', self.simid)

    def processtick(self):
        events = []
        indicators = []
        currentcandles = self.simcandles[max(self.N-self.historysize+1, 0):self.N+1]
        self.namespace['opens']= [d['open'] for d in currentcandles]
        self.namespace['closes']= [d['close'] for d in currentcandles]
        self.namespace['highs']= [d['high'] for d in currentcandles]
        self.namespace['lows']= [d['low'] for d in currentcandles]
        self.namespace['volumes']= [d['volume'] for d in currentcandles]
        candle = currentcandles[self.namespace['N']]
        self.namespace['candle'] = candle
        self.namespace['high']=candle['high']
        self.namespace['low'] = candle['low']
        self.namespace['open'] = candle['open']
        self.namespace['close'] = candle['close']
        self.namespace['volume'] = candle['volume']
        self.namespace['time'] = candle['timestamp']
       
        if('indicators' in self.namespace):
            indicators = self.namespace['indicators']()
        if('tick' in self.namespace):
            events = self.namespace['tick']
        
        

        self.N += 1
        self.namespace['N'] = min(self.historysize, self.N)

    def runsim(self):
        while(self.N < len(self.simcandles)):
              self.processtick()
