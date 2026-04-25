import os
import sqlite3
from threading import Lock
from coinbase.rest import RESTClient
from enum import Enum
import json
#this class is singleton so anywhere the code needs config data or to do database read/writes
#there won't be duplicate instances of anything
class TradeType(Enum):
    NoTrade = 0
    Exit = 1
    Buy = 2
    Sell = 3
class OrderType(Enum):
    NoOrder = 0
    #buy at the curent market long or short price
    Market = 1
    #will fill if the price is better (low for long buy high for long sell) then the current market
    Limit = 2
    #will fill if the price is worse low for short high for long when entering. 
    Stop = 3
    #Simulatenous Limit and Stop order on the same funds
    Bracket = 4

class TradeOrder:
    def __init__(self, tradetype: TradeType = TradeType.NoTrade, amount: float=0.0, limitprice: float=0.0, 
                 stopprice: float=0.0, ordertype: OrderType=OrderType.NoOrder, fee: float=0.0,
                 limittrailpercent: float=0.0, stoptrailpercent: float=0.0):
        self.tradetype = tradetype
        self.amount = amount
        self.limitprice = limitprice
        self.stopprice = stopprice
        self.ordertype = ordertype
        self.fee=fee
        self.limittrailpercent = limittrailpercent
        self.stoptrailpercent = stoptrailpercent

    def __str__(self):
        return self.getjson()
    def getjson(self):
        mydict = {'tradetype':self.tradetype.name, 'amount':self.amount, 'limitprice':self.limitprice,
                  'stopprice':self.stopprice, 'ordertype':self.ordertype.name, 'fee':self.fee,
                  'limittrailpercent':self.limittrailpercent, 'stoptrailpercent':self.stoptrailpercent}
        return json.dumps(mydict)
    def fromjson(jsonstr):
        mydict = json.loads(jsonstr)
        tradetype = TradeType[mydict['tradetype']]
        amount = mydict['amount']
        limitprice = mydict['limitprice']
        stopprice = mydict['stopprice']
        ordertype = OrderType[mydict['ordertype']]
        fee = mydict['fee']
        limittrailpercent = mydict['limittrailpercent']
        stoptrailpercent = mydict['stoptrailpercent']

        myto = TradeOrder(tradetype=tradetype, amount=amount, limitprice=limitprice, stopprice=stopprice, 
                          ordertype=ordertype, fee=fee, limittrailpercent=limittrailpercent, stoptrailpercent=stoptrailpercent)
        return myto


class TradePosition:
    def __init__(self, tradetype: TradeType = TradeType.NoTrade, amount: float=0.0, price: float=0.0, ordertype: OrderType=OrderType.NoOrder, fee: float=0.0):
        self.tradetype = tradetype
        self.Price = price
        self.Amount = amount
        self.ordertype = ordertype
        self.Fee=fee

    def __str__(self):
        return self.tradetype.name+" at "+str(self.Price)+ " for $"+str(self.Amount)+" USD."
    def getjson(self):
        mydict = {'tradetype':self.tradetype.name, 'price':self.Price, 'amount':self.Amount, 'fee':self.Fee, 'ordertype':self.ordertype.name}
        return json.dumps(mydict)
    def fromjson(jsonstr):
        mydict = json.loads(jsonstr)
        tradetype = TradeType[mydict['tradetype']]
        price = mydict['price']
        amount = mydict['amount']
        fee = mydict['fee']
        ordertype = OrderType[mydict['ordertype']]

        myto = TradeOrder(tradetype=tradetype, amount=amount, price=price, ordertype=ordertype, fee=fee)
        return myto



class util:
    
    _instance = None
    configs = None
    _cur = None
    _conn = None
    _sqlfile = None
    lock = Lock()
    client = None
    granularities = {'ONE_MINUTE':60, 'FIVE_MINUTE':300, 'FIFTEEN_MINUTE':900, 'ONE_HOUR':3600, 'SIX_HOUR':21600, 'ONE_DAY':86400}
    SimID = None
    TickTime = None
    
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            path = os.path.dirname(os.path.abspath(__file__))
            cls._sqlfile = os.path.join(path, 'data')
            os.makedirs(cls._sqlfile, exist_ok=True)
            cls._sqlfile = os.path.join(cls._sqlfile, 'db.sqlite')
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
                duration TEXT,
                UNIQUE (pair, timestamp)
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
            cur.execute("""CREATE TABLE IF NOT EXISTS exchangesim (
                id INTEGER PRIMARY KEY,
                log TEXT,
                granularity TEXT,
                pair TEXT,
                start INTEGER,
                stop INTEGER,
                scriptid INTEGER,
                status INTEGER
            )""")
            cur.execute("""CREATE TABLE IF NOT EXISTS simevent (
                id INTEGER PRIMARY KEY,
                exchangesimid INTEGER,
                candleid INTEGER,
                eventtype TEXT,
                ordertype TEXT,
                eventdata TEXT,
                fee DECIMAL(20,8),
                metadata TEXT,
                time INTEGER
            )""")
            cur.execute("""CREATE TABLE IF NOT EXISTS simindicator (
                id INTEGER PRIMARY KEY,
                exchangesimid INTEGER,
                candleid INTEGER,
                indname TEXT,
                inddata TEXT,
                indval DECIMAL(20,8),
                metadata TEXT,
                time INTEGER
            )""")
            cur.execute("""CREATE TABLE IF NOT EXISTS simasset (
                id INTEGER PRIMARY KEY,
                exchangesimid INTEGER,
                assettype TEXT,
                assetamount DECIMAL(20,8),
                UNIQUE (assettype, assetamount)
            )""")
            cur.execute("""CREATE TABLE IF NOT EXISTS liveevent (
                id INTEGER PRIMARY KEY,
                scriptid INTEGER,
                eventtype TEXT,
                eventdata TEXT,
                time INTEGER
            )""")
            cur.execute("""CREATE TABLE IF NOT EXISTS liveorder (
                id INTEGER PRIMARY KEY,
                scriptid INTEGER,
                coinbase_order_id TEXT,
                internal_id TEXT UNIQUE,
                tradetype TEXT,
                limitprice REAL,
                stopprice REAL,
                amount REAL,
                limittrailpercent REAL,
                stoptrailpercent REAL,
                status TEXT,
                time INTEGER,
                activated INTEGER DEFAULT 0,
                peak_price REAL DEFAULT 0,
                hard_stopprice REAL DEFAULT 0
            )""")
            # Migrate existing liveorder tables that predate the trailing-stop columns
            for col, default in [('activated', '0'), ('peak_price', '0'), ('hard_stopprice', '0')]:
                try:
                    cur.execute(f"ALTER TABLE liveorder ADD COLUMN {col} REAL DEFAULT {default}")
                except Exception:
                    pass
            # Migrate exchangesim to add runat timestamp
            try:
                cur.execute("ALTER TABLE exchangesim ADD COLUMN runat INTEGER DEFAULT 0")
            except Exception:
                pass
            # Migrate exchangesim to add simulation progress columns
            try:
                cur.execute("ALTER TABLE exchangesim ADD COLUMN currenttick INTEGER DEFAULT 0")
            except Exception:
                pass
            try:
                cur.execute("ALTER TABLE exchangesim ADD COLUMN totalticks INTEGER DEFAULT 0")
            except Exception:
                pass
            conn.commit()
            cur.close()
            conn.close()
        return cls._instance

    def setasset(self, assetname, amount, simid=None):
        amount = self.getasset(assetname, simid)
        if(amount is None):
            self.runinsert("INSERT INTO simasset (exchangesimid, assettype, assetamount) VALUES(?,?,?)",(simid, assetname, amount))
        else: 
            self.runupdate("UPDATE simasset SET assetamount=? WHERE assettype=? AND exchangesimid=?",(amount, assetname, simid))

    def getasset(self, assetname, simid=None):
        assetentry = self.runselect("SELECT * FROM simasset WHERE assettype=? and exchangesimid=? LIMIT 1",(assetname, simid))
        if len(assetentry) > 0:
            return assetentry[0]['assetamount']
        else:
            return None

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

    def runinsertmany(self, sql: str, params_list: list) -> int:
        count = 0
        with self.lock:
            try:
                self._conn = sqlite3.connect(self._sqlfile)
                self._cur = self._conn.cursor()
                self._cur.executemany(sql, params_list)
                count = self._cur.rowcount
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
        return count

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

    def simlog(self, newlog:str):
        print(newlog)
        succ = 0
        newlog = self.TickTime+":"+newlog+"<br>";
        try:
            succ = self.runupdate("UPDATE exchangesim SET log = log || ? WHERE id=?;", (newlog, self.SimID))
        except:
            print("FAILED TO UPATE SIM LOG(ERROR)")
        if succ == 0:
            print("FAILED TO UPATE SIM LOG(NO SIM ID)")
            return False
        return True


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
        product_id = pair
        client = self.getclient()
        if client is None:
            return []
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
                candles = response.to_dict()
                candles = candles['candles']
                print("Downloaded "+str(len(candles)) + " from coinabse")
                for candle in candles:
                    #edge cases are handles with try except instead of actually fixing the off by one bug, because coinbase includes
                    #candle edge case where its docs says it does not. So incase the fix it this algorithm pulls 1 too many candles
                    #and just won't insert the duplicates
                    try:
                        self.runinsert("INSERT INTO candle (pair, open, close, high, low, volume, timestamp, duration) VALUES(?,?,?,?,?,?,?,?)", 
                                        (pair, candle['open'], candle['close'], candle['high'], candle['low'], candle['volume'], candle['start'], granularity))
                    except sqlite3.IntegrityError as e:
                        if 'UNIQUE constraint failed' in str(e):
                            print("did not insert "+str(candle['start'])+" twice")
                tstart = pagediff
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
                    try:
                        self.runinsert("INSERT INTO candle (pair, open, close, high, low, volume, timestamp, duration) VALUES(?,?,?,?,?,?,?,?)", 
                                        (pair, candle['open'], candle['close'], candle['high'], candle['low'], candle['volume'], candle['start'], granularity))
                    except sqlite3.IntegrityError as e:
                        if 'UNIQUE constraint failed' in str(e):
                            print("did not insert "+str(candle['start'])+" twice")

        else:
            print("We already had candles for the stop time")

        candles = self.runselect("SELECT * FROM candle WHERE timestamp>=? AND timestamp<=? AND duration=? and pair=? ORDER BY timestamp", (start, stop, granularity, pair))
        return candles

