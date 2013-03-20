#!/usr/local/bin/python
import sqlite3
import sys
import os
import time
import uuid
import json
import pickle
    
class Node(object):

    def __init__(self,conn,nodeId):
        self._conn = conn
        self._nodeId = nodeId
        self._cache = dict()
        self._fresh = False

    def __repr__(self):
        return "<zdb.Node %s>" % self._nodeId

    def _fromPair(self,valType,valData):
        if isinstance(valData,buffer): valData = str(valData)
        if valType is None: return valData
        elif valType is 6: return Node(self._conn,valData)
        elif valType is 2: return json.loads(valData)
        elif valType is 5: return pickle.loads(valData)
        else: raise Exception("valType %s not implemented" % valType)

    def _toPair(self,thing):
        if isinstance(thing,Node):
            if thing._conn is not self._conn: 
                raise Exception("external references not yet supported")
            return (6,thing._nodeId)
        if thing is None or isinstance(thing,(unicode,int,float)): 
            return (None,thing)
        if isinstance(thing,str):
            return (None,buffer(thing))
        if isinstance(thing,(dict,list,tuple)):
            try: return (2,buffer(json.dumps(thing,separators=(',',':'))))
            except: pass
        try: return (5,buffer(pickle.dumps(thing,2)))
        except: pass
        raise ValueError("unable to serialize: %s,%s" % (type(thing),thing))

    def refresh(self):
        if self._nodeId is None:
            query = """
                select key,valType,valData
                from nodes 
                where nodeId is NULL
                order by timeStamp; """
            cur = self._conn.execute(query)
        else:
            query = """
                select key,valType,valData
                from nodes 
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

    def get(self,key,default=None):
        if not self._fresh: self.refresh()
        return self._cache.get(key,default)

    def update(self,dictLike):
        started = time.time()
        query = """
            insert into nodes (recordId,nodeId,key,valType,valData,timeStamp)
            values (?,?,?,?,?,?); """ 
        tuples = list()
        for key,value in dictLike.items():
            valType,valData = self._toPair(value)
            tmp = (str(uuid.uuid4()),self._nodeId,key,valType,valData,started)
            tuples.append(tmp)
            self._cache[key] = value
        self._conn.executemany(query,tuples)
        return self

    def __setitem__(self,key,value):
        self.update({key:value})

    def log(self,**d): 
        self[time.time()] = d

    def __str__(self):
        self.refresh()
        out = "\n"
        for k,v in self.items():
            out += "%10s => %r\n" % (repr(k),v)
        return out

class File(object):
    def __init__(self,fn):
        self.fn = fn
        _columns = "recordId, nodeId, key, valType, valData, timeStamp, src"
        self.conn = sqlite3.connect(fn)
        self.conn.execute("create table if not exists nodes (%s);" % _columns)
    def __repr__(self): return "<zdb.File %s>" % self.fn
    def getRoot(self): return Node(self.conn,None)
    def makeNode(self): return Node(self.conn,str(uuid.uuid4()))
    def getSimple(self,tbl="simple"): return Simple(self.conn,tbl)
    def __del__(self): self.close()
    def commit(self): self.conn.commit()
    def close(self): 
        if self.conn:
            self.conn.commit()
            self.conn.close()
            self.conn = None

def doTest(*args):
    fn = "/tmp/test.zdb"
    if os.path.exists(fn): os.unlink(fn)
    zeta = File(fn)
    meta = zeta.getSimple("meta")
    meta["foo"] = "bar"
    meta[9223372036854775807] = "cheese"
    assert set(meta.keys()) == set(['foo',9223372036854775807]),meta.keys()
    assert meta[9223372036854775807] == "cheese"
    root = zeta.getRoot()
    root["abc"] = [1,2,"three"]
    zeta.close()
    zeta2 = File(fn)
    assert zeta2.getRoot()["abc"] == [1,2,"three"]
    zeta2.close()
    #os.unlink(fn)
    print "ok!"

def doGet(fn,key=None):
    z = File(fn)
    v = z.getRoot()
    if key is not None:
        for link in key.split("/"):
            if not link: continue
            v = v[link]
    print str(v)

def doSet(fn,key,value=None):
    f = File(fn)
    v = f.getRoot()
    links = [x for x in key.split("/") if x]
    last = links.pop()
    for link in links: v = v[link]
    value = f.makeNode() if value is None else value
    v[last] = value
    
    

if __name__ == "__main__":
    try:
        if len(sys.argv) == 1:
            print "usage..."
            sys.exit(0)
        elif sys.argv[1] == "test":    doTest(*sys.argv[2:])
        elif sys.argv[1] == "get":     doGet(*sys.argv[2:])
        elif sys.argv[1] == "set":     doSet(*sys.argv[2:])
        elif sys.argv[1] == "select":  doSelect(*sys.argv[2:])
    except KeyError:
        sys.exit(1)
