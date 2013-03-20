#!/usr/local/bin/python
import sys
import os
import re
import time
import datetime
import zdb

def doSelect(node,columns=None):
    colList = columns.split(",") if columns else None
    for thing in node.values():
        if colList:
            try: print ",".join([str(thing.get(c)) for c in colList])
            except AttributeError: pass
        else:
            print thing

if __name__ == "__main__":
    if len(sys.argv) == 1: 
        print "usage..."
        sys.exit(0)
    fn = sys.argv[1]
    assert os.path.exists(fn),fn
    handle = zdb.File(fn)
    root = handle.getRoot()
    columns = None
    if len(sys.argv) > 2: columns = sys.argv[2]
    doSelect(root,columns)
