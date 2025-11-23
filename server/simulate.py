import util
from coinbase.rest import RESTClient
from datetime import datetime
import json
import talib
import numpy

sutil = util.util()
client = sutil.getclient()


#product_id = "BTC-USD"
#start = int(datetime(2025, 7, 21).timestamp())
#end = int(datetime(2025, 7, 22).timestamp())
#granularity = "ONE_HOUR"

#candles = gethistoricledata(granularity, product_id, start, end)
#sutil.runupdate("DELETE FROM candle;",())

#Market order, Limit order, Stop-limit order, Bracket order, Take Profit/Stop Loss order, TWAP order



class Simulation:

    def __init__(self, start, stop, scriptid):
        scripts = sutil.runselect("SELECT * FROM scripts WHERE id=?",(scriptid,))
        self.script = scripts[0]['script']
        self.start = start
        self.stop = stop
        self.N = 0
        self.namespace = {}
        self.namespace['talib'] = talib
        self.namespace['numpy'] = numpy
        self.namespace['granularity'] = "ONE_HOUR"
        self.namespace['pair'] = "BTC-PERP"
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
            self.pair = namespace['pair']
        if('granularity' in self.namespace):
            self.granularity = namespace['granularity']


        self.simcandles = sutil.gethistoricaldata(self.granularity, self.pair, self.start, self.stop)


    def processtick(self):
        events = []
        indicators = []
        currentcandles = self.simcandles[max(N-self.historysize+1, 0):N+1]
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
        self.namespace['time'] = candle['time']
       
        if('indicators' in self.namespace):
            indicators = self.namespace['indicators']()
        if('tick' in self.namespace):
            events = self.namespace['tick']
        self.N += 1
        self.namespace['N'] = min(self.history, self.N)


