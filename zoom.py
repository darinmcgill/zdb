import sqlite3
import sys
import os
import random
import time

_letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
def _newId():
    "".join([random.choice(_letters) for i in range(10)])
    
class Proxy(object):
    def __init__(self,conn,nodeId):
        self._conn = conn
        self._nodeId = nodeId
        self._cache = dict()
        self._fresh = False
    def _fromPair(self,valType,valData):
        if valType is None:
            return valData
        elif valType is 6:
            return Proxy(self._conn,valData)
        else:
            raise Exception("not implemented")
    def _toPair(self,thing):
        if thing is None or isinstance(thing,(str,int,float)): 
            return None,thing
        if isinstance(thing,Proxy):
            if thing._conn is not self._conn: 
                raise Exception("external references not yet supported")
            return 6,thing._nodeId
        raise Exception("not expecting: %s,%s" % (type(thing),thing))
    def refresh(self):
        if self._nodeId is None:
            query = """
                select key,valType,valData
                from tbl 
                where nodeId is NULL
                order by timeStamp; """
            cur = self._conn.execute(query)
        else:
            query = """
                select key,valType,valData
                from tbl 
                where nodeId=? 
                order by timeStamp; """
            cur = self._conn.execute(query,(self._nodeId,))
        for row in cur.fetchall():
            key,valType,value = row
            self._cache[key] = self._fromPair(valType,value)
        self._fresh = True
    def keys(self):
        if not self._fresh: self.refresh()
        return sorted(self._cache.keys())
    def values(self):
        return [self._cache[k] for k in self.keys()]
    def items(self):
        return [(k,self._cache[k]) for k in self.keys()]
    def __getitem__(self,key):
        if not self._fresh: self.refresh()
        return self._cache[key]
    def update(self,dictLike):
        started = time.time()
        query = """
            insert into tbl (recordId,nodeId,key,valType,valData,timeStamp)
            values (?,?,?,?,?,?); """ 
        tuples = list()
        for key,value in dictLike.items():
            valType,valData = self._toPair(value)
            tmp = (_newId(),self._nodeId,key,valType,valData,started)
            tuples.append(tmp)
            self._cache[key] = value
        self._conn.executemany(query,tuples)
        return self
    def __setitem__(self,key,value):
        self.update({key:value})
    def __del__(self):
        self._conn.commit()
    def commit(self):
        self._conn.commit()

class Zoom(Proxy):
    def __init__(self,fn):
        conn = sqlite3.connect(fn)
        conn.text_factory = str
        with open("tbl.sql") as handle:
            conn.execute(handle.read())
        Proxy.__init__(self,conn,None)
    def make(self): return Proxy(self._conn,_newId())
    def dump(self,dictLike): return self.make().update(dictLike)
    def log(self,dictLike): self[time.time()] = self.dump(dictLike)

if __name__ == "__main__":
    p = Zoom("/tmp/test.zdb")
    if len(sys.argv) == 1:
        p.refresh()
        print p._cache
    elif len(sys.argv) == 2:
        print p[sys.argv[1]]
    else:
        p[sys.argv[1]] = sys.argv[2]
