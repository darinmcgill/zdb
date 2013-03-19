import sys
import os
import sqlite3
import re
import uuid

src = sys.argv[1]
assert os.path.exists(src),src
ts = float(sys.argv[2])

tmp = "/tmp/getDelta_%s.db" % uuid.uuid4()

srcConn = sqlite3.connect(src)
tmpConn = sqlite3.connect(tmp)

with open("tbl.sql") as handle: 
    tmpConn.execute(handle.read())

cur = srcConn.execute("select * from tbl where timeStamp > %s" % ts)
columns = ",".join([x[0] for x in cur.description])
tmpConn.execute("create table tbl (%s)" % columns)
rows = cur.fetchall()
qs = ",".join(["?" for x in rows[0]])
tmpConn.executemany("insert into tbl values (%s)" % qs,rows)
tmpConn.commit()
print tmp
print len(rows),"rows"
