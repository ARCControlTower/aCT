import datetime
from act.db import aCTDBMS
from act.common.aCTConfig import aCTConfigARC
from contextlib import contextmanager

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

    # WARNING! Assumes the use of aCTDBMySQL!!! Other DB might not be
    # handled correctly. The proper solution would be to improve the entire
    # database model.
    @contextmanager
    def namedLock(self, name, timeout=10):
        """
        Handle named DB lock in a context with automatic release.

        Returns:
            A boolean indicating whether the lock could be acquired.

        with db.namedLock('lockname', timeout=5) as lock:
            if not lock:
                # handle case of not getting lock
            else:
                # do whatever in DB
        """
        try:
            res = False
            res = self.db.getMutexLock(name, timeout)
            if not res:
                yield False
            else:
                res = True
                yield True
        finally:
            if res:
                res = self.db.releaseMutexLock(name)
                if not res:
                    self.log.warning(f'Could not release named lock {name}')
