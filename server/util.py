import os
import sqlite3
from threading import Lock


#this class is singleton so anywhere the code needs config data or to do database read/writes
#there won't be duplicate instances of anything

class util:
    
    _instance = None
    configs = None
    _cur = None
    _conn = None
    _sqlfile = None
    lock = Lock()

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
                open INTEGER,
                close INTEGER,
                high INTEGER,
                low INTEGER,
                timestamp INTEGER,
                duration INTEGER
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

