#!/usr/local/bin/python
import sqlite3
import sys
import os
import random
import time
    
class Proxy(object):

    _letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    _newId = lambda x: "".join([random.choice(x._letters) for i in range(10)])

    def __init__(self,conn,nodeId):
        self._conn = conn
        self._nodeId = nodeId
        self._cache = dict()
        self._fresh = False

    def __repr__(self):
        return "<zdb.Proxy %s>" % self._nodeId

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
            tmp = (self._newId(),self._nodeId,key,valType,valData,started)
            tuples.append(tmp)
            self._cache[key] = value
        self._conn.executemany(query,tuples)
        return self

    def __setitem__(self,key,value):
        self.update({key:value})

    def commit(self):
        self._conn.commit()

    def make(self,key=None,updateWith=None): 
        out = Proxy(self._conn,self._newId())
        if updateWith: out.update(updateWith)
        if key is not None: self[key] = out
        return out

    def log(self,dictLike): 
        return self.make(key=time.time(),updateWith=dictLike)

    def __str__(self):
        self.refresh()
        out = ""
        for k,v in self.items():
            out += "%10s => %r\n" % (repr(k),v)
        return out

class Zdb(Proxy):
    def __init__(self,fn="test.zdb"):
        conn = sqlite3.connect(fn)
        conn.text_factory = str
        with open("tbl.sql") as handle:
            conn.execute(handle.read())
        Proxy.__init__(self,conn,None)
    def __del__(self):
        self._conn.commit()

def runTests():
    print "running tests!"

if __name__ == "__main__":
    if len(sys.argv) == 1:
        runTests()
    elif len(sys.argv) == 2:
        print str(Zdb(sys.argv[1]))
    elif len(sys.argv) == 3:
        try:
            v = z = Zdb(sys.argv[1])
            key = sys.argv[2]
            for link in key.split("/"):
                if not link: continue
                v = v[link]
            print str(v)
        except KeyError:
            sys.exit(1)
    else:
        try:
            v = z = Zdb(sys.argv[1])
            key = sys.argv[2]
            value = sys.argv[3]
            links = [x for x in key.split("/") if x]
            last = links.pop()
            for link in links: v = v[link]
            value = v.make() if value == "/" else value
            v[last] = value
        except KeyError:
            sys.exit(1)
