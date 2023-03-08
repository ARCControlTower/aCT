def getDB(log, config):
    '''Factory method for getting specific DB implementation'''

    supported_dbms = {}

    try:
        from .aCTDBSqlite import aCTDBSqlite
        supported_dbms['sqlite'] = aCTDBSqlite
    except:
        pass
    try:
        from .aCTDBMySQL import aCTDBMySQL
        supported_dbms['mysql'] = aCTDBMySQL
    except:
        pass
    try:
        from .aCTDBOracle import aCTDBOracle
        supported_dbms['oracle'] = aCTDBOracle
    except:
        pass

    dbtype = config.db.type.lower()
    if dbtype not in supported_dbms:
        raise Exception("DB type %s is not implemented." % dbtype)
    return supported_dbms[dbtype](log, config)


class aCTDBMS(object):
    '''
    Class for generic DB Mgmt System db operations. Specific subclasses
    implement methods for their own database implementation.
    '''

    def __init__(self, log, config):
        self.log = log
        self.socket = config.db.socket or None
        self.dbname = config.db.name
        self.user = config.db.user or None
        self.passwd = config.db.password or None
        self.host = config.db.host or None
        self.port = config.db.port or None

    # Each subclass must implement the 6 methods below
    def getCursor(self):
        raise Exception("Method not implemented")

    def timeStampLessThan(self, column, timediff):
        raise Exception("Method not implemented")

    def timeStampGreaterThan(self, column, timediff):
        raise Exception("Method not implemented")

    def addLock(self):
        raise Exception("Method not implemented")

    def getMutexLock(self, lock_name, timeout=2):
        raise Exception("Method not implemented")

    def releaseMutexLock(self, lock_name):
        raise Exception("Method not implemented")
