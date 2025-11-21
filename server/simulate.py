import util
from coinbase.rest import RESTClient
from datetime import datetime
import json

sutil = util.util()
client = sutil.getclient()


def gethistoricledata(granulatiry:str, pair:str, start:int, stop:int):
    timebase = 0
    candles = sutil.runselect("SELECT * FROM candle WHERE duration=? and pair=? ORDER BY timestamp LIMIT 1", (granularity, pair))
    if(len(candles)> 0):
        timebase = candles[0]['timestamp']
        print("Have previous data going back to "+str(timebase))
    else:
        #timebase = int(datetime.now().timestamp())
        timebase = stop
        print("We do not have previous data, going until "+str(timebase)) 

    if start < timebase:
        print("Downloading data from "+str(start)+" to "+str(timebase))
        tstart = start
        while tstart<timebase:
            pagediff = tstart + sutil.granularities[granularity]*250
            if(pagediff > timebase):
                pagediff = timebase
            print("calculated page = "+str(tstart)+ " to "+str(pagediff)+" with "+str(sutil.granularities[granularity]) + " granularity")

            response = client.get_candles(
                product_id,
                start=str(tstart),
                end=str(pagediff),
                granularity=granularity
            )
            tstart = pagediff
            candles = response.to_dict()
            candles = candles['candles']
            print("Downloaded "+str(len(candles)) + " from coinabse")
            for candle in candles:
                #print(candle)
                sutil.runinsert("INSERT INTO candle (pair, open, close, high, low, volume, timestamp, duration) VALUES(?,?,?,?,?,?,?,?)", 
                                (pair, candle['open'], candle['close'], candle['high'], candle['low'], candle['volume'], candle['start'], granularity))
    else:
        print("Already had candles older enough for the start time")

    
    candles = sutil.runselect("SELECT * FROM candle WHERE duration=? and pair=? ORDER BY timestamp DESC LIMIT 1", (granularity, pair))
    timebase = stop
    tstart = candles[0]['timestamp']
    if tstart < timebase:
        print("Downloading missing newer candles " + str(start)+ " to " + str(timebase))
        while tstart<timebase:
            pagediff = tstart + sutil.granularities[granularity]*250
            if(pagediff > timebase):
                pagediff = timebase
            print("calculated page = "+str(tstart)+ " to "+str(pagediff)+" with "+str(sutil.granularities[granularity]) + " granularity")

            response = client.get_candles(
                product_id,
                start=str(tstart),
                end=str(pagediff),
                granularity=granularity
            )
            tstart = pagediff
            candles = response.to_dict()
            candles = candles['candles']
            print("Downloaded "+str(len(candles)) + " from coinabse")
            for candle in candles:
                #print(candle)
                sutil.runinsert("INSERT INTO candle (pair, open, close, high, low, volume, timestamp, duration) VALUES(?,?,?,?,?,?,?,?)", 
                                (pair, candle['open'], candle['close'], candle['high'], candle['low'], candle['volume'], candle['start'], granularity))

    else:
        print("We already had candles for the stop time")

    candles = sutil.runselect("SELECT * FROM candle WHERE timestamp>? AND timestamp<? AND duration=? and pair=? ORDER BY timestamp", (start, stop, granularity, pair))
    return candles

#product_id = "BTC-USD"
#start = int(datetime(2025, 4, 19).timestamp())
#end = int(datetime(2025, 7, 22).timestamp())
#granularity = "ONE_HOUR"

#candles = gethistoricledata(granularity, product_id, start, end)
#sutil.runupdate("DELETE FROM candle;",())



