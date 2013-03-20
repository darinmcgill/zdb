
class Simple(object):

    def __init__(self,conn,tbl):
        self.conn = conn
        self.tbl = tbl
        self.conn.execute("create table if not exists %s (key,value);" % tbl)

    def __setitem__(self,k,v):
        self.conn.execute("delete from %s where key=?;" % self.tbl,(k,))
        self.conn.execute("insert into %s values (?,?);" % self.tbl,(k,v))

    def __delitem__(self,key):
        self.conn.execute("delete from %s where key=?;" % self.tbl,(key,))

    def __getitem__(self,key):
        rows = self.conn.execute(
            "select value from %s where key=?;" % self.tbl,
            (key,)).fetchall()
        if not rows: raise KeyError(key)
        return rows[0][0]

    def get(self,key,default=None):
        try: return self[key]
        except KeyError: return default

    def keys(self):
        rows = self.conn.execute("select key from %s;" % self.tbl).fetchall()
        return [row[0] for row in rows]

    def values(self):
        rows = self.conn.execute("select value from %s;" % self.tbl).fetchall()
        return [row[0] for row in rows]

    def items(self):
        return self.conn.execute(
            "select key,value from %s;" % self.tbl).fetchall()

    def __contains__(self,key):
        return self.conn.execute(
            "select count(*) as n from %s where key=?;" % self.tbl,
            (key,)).fetchall()[0][0]

    def setdefault(self,key,default):
        try: return self[key]
        except KeyError:
            self[key] = default
            return default

    def update(self,d):
        for k,v in d.items():
            self[k] = v

    def commit(self):
        self.conn.commit()

    def __del__(self):
        try: self.conn.commit()
        except: pass
