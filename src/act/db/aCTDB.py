import datetime
from act.db import aCTDBMS
from act.common.aCTConfig import aCTConfigARC

class aCTDB(object):
    '''Superclass representing a general table in the DB'''

    def __init__(self, logger, tablename, db=None):
        self.log = logger
        self.table = tablename
        self.conf = aCTConfigARC()
        self.db = db
        if self.db is None:
            self.db = aCTDBMS.getDB(self.log, self.conf)

    def _column_list2str(self,columns):
        s=""
        if columns:
            for col in columns:
                s+=col+", "
            s=s.strip(", ")
        else:
            s="*"
        return s

    def getTimeStamp(self, seconds=None):
        if seconds:
            return datetime.datetime.utcfromtimestamp(seconds).isoformat()
        else:
            return datetime.datetime.utcnow().isoformat()

    def timeStampLessThan(self, column, timediff, utc=True):
        return self.db.timeStampLessThan(column, timediff, utc)

    def timeStampGreaterThan(self, column, timediff, utc=True):
        return self.db.timeStampGreaterThan(column, timediff, utc)

    def Commit(self, lock=False):
        if lock:
            res = self.db.releaseMutexLock(self.table)
            if not res:
                self.log.warning("Could not release lock: %s" % str(res))
        try:
            self.db.conn.commit()
        except Exception as e:
            self.log.error("Exception on commit: %s" % str(e))
        if lock:
            c = self.db.getCursor()
            c.execute("UNLOCK TABLES")

    def close(self):
        self.db.close()

