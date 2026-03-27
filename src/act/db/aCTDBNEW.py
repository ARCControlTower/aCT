from datetime import datetime, timezone, timedelta
from act.db import aCTDBMS
from act.common.aCTConfig import aCTConfigARC
from contextlib import contextmanager
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, ForeignKey, select, update
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, sessionmaker

class aCTDB(object):
    '''Superclass representing a general table in the DB'''

    def __init__(self, logger, tablename, db=None):
        self.log = logger
        self.table = tablename
        self.conf = aCTConfigARC()
        self.engine = create_engine(
            f'{self.conf.db.type}+{self.conf.db.driver}://{self.conf.db.user}:{self.conf.db.password}@{self.conf.db.host}:{self.conf.db.port}/{self.conf.db.name}',
            pool_size=2,
            max_overflow=0,
            pool_pre_ping=True,
            pool_recycle=3600,
            )
        self.Session = sessionmaker(bind=self.engine)
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
            return datetime.fromtimestamp(timezone.utc, seconds)
        else:
            return datetime.now(timezone.utc)

    def timeStampLessThan(column, seconds, utc=True):
        now = datetime.now(timezone.utc) if utc else datetime.now()
        cutoff = now - timedelta(seconds=seconds)
        return column < cutoff

    def timeStampGreaterThan(column, seconds, utc=True):
        now = datetime.now(timezone.utc) if utc else datetime.now()
        cutoff = now - timedelta(seconds=seconds)
        return column > cutoff
    
    #stmt = select(MyTable).where(timeStampLessThan(MyTable.created_at, 60))

    #unused
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
    #unused
    def close(self):
        self.db.close()
