from datetime import datetime, timezone, timedelta
from act.common.aCTConfig import aCTConfigARC
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

class aCTDB(object):
    '''Superclass representing a general table in the DB'''

    def __init__(self, logger):
        self.log = logger
        self.conf = aCTConfigARC()
        self.engine = create_engine(
            self.conf.db.link,
            pool_size=2,
            max_overflow=0,
            pool_pre_ping=True,
            pool_recycle=3600,
            )
        self.Session = sessionmaker(bind=self.engine)

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
