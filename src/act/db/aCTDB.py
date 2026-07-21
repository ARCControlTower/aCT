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

    def getTimeStamp(self, seconds=None):
        if seconds:
            return datetime.fromtimestamp(seconds, timezone.utc)
        else:
            return datetime.now(timezone.utc)

    def timeStampLessThan(self, column, seconds, utc=True):
        now = datetime.now(timezone.utc) if utc else datetime.now()
        cutoff = now - timedelta(seconds=seconds)
        return column < cutoff

    def timeStampGreaterThan(self, column, seconds, utc=True):
        now = datetime.now(timezone.utc) if utc else datetime.now()
        cutoff = now - timedelta(seconds=seconds)
        return column > cutoff
