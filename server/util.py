import os
import sqlite3
from threading import Lock
from coinbase.rest import RESTClient
from enum import Enum
#this class is singleton so anywhere the code needs config data or to do database read/writes
#there won't be duplicate instances of anything

class util:
    
    _instance = None
    configs = None
    _cur = None
    _conn = None
    _sqlfile = None
    lock = Lock()
    client = None
    granularities = {'ONE_MINUTE':60, 'FIVE_MINUTE':300, 'FIFTEEN_MINUTE':900, 'ONE_HOUR':3600, 'SIX_HOUR':21600, 'ONE_DAY':86400}
    class OrderType(Enum):
        #buy at the curent market long or short price
        market = 1
        #will fill if the price is better (low for long buy high for long sell) then the current market
        limit = 2
        #set 2 prices, stop and limit. if at 100, set stop to 105, and limit to 106, will activate at 105 and fill at 106
        stop_limit = 3
        #bracket is on open positions only. so for a limit buy of 100 you might set 95 stop loss and 110 take profit. Coinbase
        #set a 1.5% limit stop under the hood on the stop loss, so if the market if moving very fast you MIGHT get a price up to
        #1.5 percent worse then what you set
        braket = 4
        #in simulations this just executes equal market orders given your time frame. example 2 hours, 15 minute buckets 8 buckets on
        #1000 with execute 8 buys of $~125, essentially dollar cost averaging. In reality coinbase should always give you a better
        #price then in simulation because its setting intelligent limit order to snag something better
        twap = 5

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            path = os.path.dirname(os.path.abspath(__file__))
            cls._sqlfile = os.path.join(path, 'data', 'db.sqlite')
            conn = sqlite3.connect(cls._sqlfile)
            cur = conn.cursor()
            cur.execute("""CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                name TEXT,
                pass TEXT
            )""")
            cur.execute("""CREATE TABLE IF NOT EXISTS sessions (
                id INTEGER PRIMARY KEY,
                user INTEGER,
                sessionid TEXT,
                expiration INTEGER
            )""")
            cur.execute("""CREATE TABLE IF NOT EXISTS candle (
                id INTEGER PRIMARY KEY,
                pair TEXT,
                open DECIMAL(20,8),
                close DECIMAL(20,8),
                high DECIMAL(20,8),
                low DECIMAL(20,8),
                volume REAL,
                timestamp INTEGER,
                duration TEXT
            )""")
            cur.execute("""CREATE TABLE IF NOT EXISTS scripts (
                id INTEGER PRIMARY KEY,
                script TEXT,
                name TEXT,
                status INTEGER
            )""")
            cur.execute("""CREATE TABLE IF NOT EXISTS indicator (
                id INTEGER PRIMARY KEY,
                equation TEXT,
                status INTEGER
            )""")
            cur.execute("""CREATE TABLE IF NOT EXISTS event (
                id INTEGER PRIMARY KEY,
                eventtype TEXT,
                timestamp INTEGER,
                price INTEGER,
                status INTEGER
            )""")
            cur.execute("""CREATE TABLE IF NOT EXISTS metadata (
                id INTEGER PRIMARY KEY,
                metakey TEXT UNIQUE,
                metavalue TEXT
            )""")
            conn.commit()
            cur.close()
            conn.close()
        return cls._instance


    def setkeyval(self, key:str, val:str)->bool:
        res = self.getkeyval(key)
        if(res is not None):
            succ = self.runupdate("UPDATE metadata SET metavalue=? WHERE metakey=?", (val, key))
            if(succ > 0):
                return True
            else:
                return False
        else:
            succ = self.runinsert("INSERT INTO metadata (metakey, metavalue) VALUES (?, ?)", (key, val))
            if succ > -1:
                return True
            else:
                return False
    def getkeyval(self, key:str)-> str:
        res = self.runselect("SELECT * FROM metadata WHERE metakey=?", (key,))
        if(len(res) > 0):
            return res[0]['metavalue']
        else:
            return None

    def runupdate(self, sql:str, params:tuple)->int:
        rowcnt = -1
        with self.lock:
            try:
                self._conn = sqlite3.connect(self._sqlfile)
                self._cur = self._conn.cursor()
                self._cur.execute(sql, params)
                rowcnt = self._cur.rowcount
                self._conn.commit()
            finally:
                try:
                    self._cur.close()
                except:
                    pass
                try:
                    self._conn.close()
                except:
                    pass
        return rowcnt

    def runinsert(self, sql:str, params:tuple)->int:
        new_id = -1
        with self.lock:
            try:
                self._conn = sqlite3.connect(self._sqlfile)
                self._cur = self._conn.cursor()
                self._cur.execute(sql, params)
                new_id = self._cur.lastrowid
                self._conn.commit()
            finally:
                try:
                    self._cur.close()
                except:
                    pass
                try:
                    self._conn.close()
                except:
                    pass
        return new_id

    def runselect(self, sql:str, params:tuple) -> list[dict[str, object]]:
        res = []
        try:
            conn = sqlite3.connect(self._sqlfile)
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute(sql, params)
            rows = cur.fetchall()
            res = [dict(row) for row in rows]
        finally:
            try:
                cur.close()
            except:
                pass
            try:
                conn.close()
            except:
                pass
        return res

    def getclient(self):
        if(self.client is None):
            key = None
            secret = None
            res = self.runselect("SELECT * FROM metadata WHERE metakey=?", ('cbkey',))
            if(len(res)> 0):
                key = res[0]['metavalue']
            res = self.runselect("SELECT * FROM metadata WHERE metakey=?", ('cbsecret',))
            if(len(res)> 0):
                secret = res[0]['metavalue']
            if key is not None and secret is not None:
                self.client = RESTClient(api_key=key, api_secret=secret)
        return self.client

    def getconfig(self, key: str) -> str:
        if self.configs is None:
            self.configs = {}
            path = os.path.dirname(os.path.abspath(__file__))
            configf = os.path.join(path, '..', 'config.txt')
            with open(configf, 'r') as f:
                for line in f:
                    ls = line.strip()
                    if ":" in ls and not ls.startswith("#"):
                        strparts = ls.split(":")
                        try:
                            self.configs[strparts[0].strip().lower()] = strparts[1].strip().lower()
                        except:
                            print("ERROR: Problem with config file on \n"+ls)

        lkey = key.lower()
        if lkey in self.configs:
            return self.configs[lkey]
        else:
            return ""
    

    #returns the cert and the key file or creates them if missing
    def getservercert(self)->(str, str):
        path = os.path.dirname(os.path.abspath(__file__))
        certfile = os.path.normpath(path+"/data/server.crt")
        keyfile = os.path.normpath(path+"/data/server.pem")
        cert_exists = os.path.isfile(certfile)
        key_exists = os.path.isfile(keyfile)
        if(not key_exists or not cert_exists):
            from cryptography.hazmat.primitives import serialization, hashes
            from cryptography.hazmat.primitives.asymmetric import rsa
            from cryptography.x509.oid import NameOID
            from cryptography import x509
            from datetime import datetime, timedelta

            key = rsa.generate_private_key(public_exponent=65537, key_size=4096)
            with open(keyfile, "wb") as f:
                f.write(key.private_bytes(
                    encoding=serialization.Encoding.PEM,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.NoEncryption()
                ))

            subject = issuer = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")])
            cert = x509.CertificateBuilder().subject_name(subject) \
                .issuer_name(issuer) \
                .public_key(key.public_key()) \
                .serial_number(x509.random_serial_number()) \
                .not_valid_before(datetime.utcnow()) \
                .not_valid_after(datetime.utcnow() + timedelta(days=3650)) \
                .add_extension(x509.SubjectAlternativeName([x509.DNSName("localhost")]), critical=False) \
                .sign(key, hashes.SHA256())

            with open(certfile, "wb") as f:
                f.write(cert.public_bytes(serialization.Encoding.PEM))

        return(certfile, keyfile)

    def gethistoricledata(self, granularity:str, pair:str, start:int, stop:int):
        timebase = 0
        candles = self.runselect("SELECT * FROM candle WHERE duration=? and pair=? ORDER BY timestamp LIMIT 1", (granularity, pair))
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
                pagediff = tstart + self.granularities[granularity]*250
                if(pagediff > timebase):
                    pagediff = timebase
                print("calculated page = "+str(tstart)+ " to "+str(pagediff)+" with "+str(self.granularities[granularity]) + " granularity")

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
                    self.runinsert("INSERT INTO candle (pair, open, close, high, low, volume, timestamp, duration) VALUES(?,?,?,?,?,?,?,?)", 
                                    (pair, candle['open'], candle['close'], candle['high'], candle['low'], candle['volume'], candle['start'], granularity))
        else:
            print("Already had candles older enough for the start time")

        
        candles = self.runselect("SELECT * FROM candle WHERE duration=? and pair=? ORDER BY timestamp DESC LIMIT 1", (granularity, pair))
        timebase = stop
        tstart = candles[0]['timestamp']
        if tstart < timebase:
            print("Downloading missing newer candles " + str(tstart)+ " to " + str(timebase))
            while tstart<timebase:
                pagediff = tstart + self.granularities[granularity]*250
                if(pagediff > timebase):
                    pagediff = timebase
                print("calculated page = "+str(tstart)+ " to "+str(pagediff)+" with "+str(self.granularities[granularity]) + " granularity")

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
                    self.runinsert("INSERT INTO candle (pair, open, close, high, low, volume, timestamp, duration) VALUES(?,?,?,?,?,?,?,?)", 
                                    (pair, candle['open'], candle['close'], candle['high'], candle['low'], candle['volume'], candle['start'], granularity))

        else:
            print("We already had candles for the stop time")

        candles = self.runselect("SELECT * FROM candle WHERE timestamp>? AND timestamp<? AND duration=? and pair=? ORDER BY timestamp", (start, stop, granularity, pair))
        return candles

