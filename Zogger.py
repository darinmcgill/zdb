#!/usr/local/bin/python
import sqlite3
import sys
import os
import time
import datetime
import random

class Zogger(object):

    def __init__(self,thing):
        if isinstance(thing,(str,unicode)): 
            thing = sqlite3.connect(thing)
        assert isinstance(thing,sqlite3.Connection),(type(thing),thing)
        self.conn = thing
        #self.conn.text_factory = str
        self.cur = self.conn.cursor()
        self.cur.execute("""
            create table if not exists zog 
            (_id integer primary key asc);""")
        self._next = random.randint(0,999)
        self.refresh()

    def refresh(self):
        self.cur.execute("select * from zog limit 1;")
        self.cols = set([x[0] for x in self.cur.description])

    def zog(self,*a,**b):
        b["_id"] = 1000*int(1000*1000*time.time()) + self._next
        self._next = (1 + self._next) % 1000
        for i,v in enumerate(a):
            b["_%d" % i] = v
        keys = b.keys()
        for key in b.keys():
            if key not in self.cols: 
                self.cur.execute("alter table zog add column %s;" % key)
                self.refresh()
        ns = ",".join(keys)
        qs = ",".join(["?" for x in keys])
        vs = [b[key] for key in keys]
        self.cur.execute("insert into zog (%s) values (%s);" % (ns,qs),vs)

    def commit(self):
        self.conn.commit()

    def close(self):
        try:
            if self.conn:
                self.conn.commit()
                self.conn.close()
                self.conn = None
        except Exception as e: 
            print e

    def __del__(self):
        self.close()

    def getEntries(self,text_factory=None):
        if text_factory:
            self.conn.text_factory = text_factory
        self.cur.execute("select * from zog order by _id;")
        raw = self.cur.fetchall()
        cols = [x[0] for x in self.cur.description]
        out = list()
        for row in raw:
            dRow = dict()
            for i in range(len(cols)):
                if cols[i] == "_id": 
                    ts = datetime.datetime.fromtimestamp(row[i]/1e9)
                    continue
                if row[i] is not None:
                    dRow[cols[i]] = row[i]
            out.append((ts,dRow))
        return out
        
if __name__ == "__main__":
    fn = "/tmp/test.zog"
    if os.path.exists(fn): os.unlink(fn)
    zogger = Zogger(fn)
    zogger.zog()
    zogger.zog()
    zogger.zog()
    zogger.zog(hello="world",foo="bar")
    zogger.zog(cheese="fries",hello="universe")
    zogger.zog("martin",hello="again")
    zogger.close()
    zogger = Zogger(fn)
    for entry in zogger.getEntries(str):
        print entry[0],entry[1]
