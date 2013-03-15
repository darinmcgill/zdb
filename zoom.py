import sqlite3
import sys
import os
import random
import time

def _toPair(thing):
    if isinstance(thing,(str,int,None,float)): 
        return None,thing
    raise Exception("not expecting: %s,%s" % (type(thing),thing))

def _fromPair(valType,valData):
    if valType is None:
        return valData
    else:
        raise Exception("not implemented")

_letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
def _newId():
    "".join([random.choice(_letters) for i in range(10)])

class Proxy(object):
    def __init__(self,conn,nodeId):
        if isinstance(conn,str):
            #if not os.path.exists(conn):
            conn = sqlite3.connect(conn)
            conn.text_factory = str
            with open("tbl.sql") as handle:
                conn.execute(handle.read())
        self._conn = conn
        self._nodeId = nodeId
        self._cache = dict()
        self._fresh = False
    def refresh(self):
        query = """
            select key,valType,valData
            from tbl 
            where nodeId=? 
            order by timeStamp; """
        cur = self._conn.execute(query,(self._nodeId,))
        for row in cur.fetchall():
            key,valType,value = row
            self._cache[key] = _fromPair(valType,value)
        self._fresh = True
    def __getitem__(self,key):
        if not self._fresh: self.refresh()
        return self._cache[key]
    def __setitem__(self,key,value):
        query = """
            insert into tbl (recordId,nodeId,key,valType,valData,timeStamp)
            values (?,?,?,?,?,?); """ 
        valType,valData = _toPair(value)
        self._conn.execute(query,
            (_newId(),self._nodeId,key,valType,valData,time.time()))
        self._cache[key] = value
    def __del__(self):
        self._conn.commit()
    def commit(self):
        self._conn.commit()

if __name__ == "__main__":
    p = Proxy("/tmp/test.zdb",0)
    if len(sys.argv) == 1:
        p.refresh()
        print p._cache
    elif len(sys.argv) == 2:
        print p[sys.argv[1]]
    else:
        p[sys.argv[1]] = sys.argv[2]
