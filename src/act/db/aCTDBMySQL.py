import mysql.connector as mysql
from act.common import aCTUtils
from act.db.aCTDBMS import aCTDBMS

class aCTDBMySQL(aCTDBMS):
    """Class for MySQL specific db operations."""

    def __init__(self, log, config):
        aCTDBMS.__init__(self, log, config)
        # mysql.connector must be 8.
        if mysql.__version_info__[0] != 8:
            raise Exception("mysql-connector must be version 8.x")
        try:
            self._connect(self.dbname)
        except mysql.Error as err:
            # if db doesnt exist, create it
            if err.errno != 1049:
                raise err
            self.log.warning("Database doesn't exist, will try to create it")
            self._connect()
            c = self.conn.cursor()
            c.execute("CREATE DATABASE "+self.dbname)
            self._connect(self.dbname)

        self.log.debug("initialized aCTDBMySQL")

    def close(self):
        if self.conn is not None:
            self.conn.close()
        if self.log is not None:
            self.log.warning("Database session closed")

    def _connect(self, dbname=None):
        if self.socket != 'None':
            self.conn = mysql.connect(unix_socket=self.socket, database=dbname)
        elif self.user and self.passwd:
            if self.host != 'None' and self.port != 'None':
                self.conn = mysql.connect(user=self.user, password=self.passwd, host=self.host, port=self.port, database=dbname)
            else:
                self.conn = mysql.connect(user=self.user, password=self.passwd, db=dbname)

    def getCursor(self):
        # make sure cursor reads newest db state
        try:
            self.conn.commit()
        except mysql.errors.InternalError as e:
            # Unread result, force reconnection
            self.log.warning(str(e))
            self.conn.close()
            self._connect(self.dbname)

        for _ in range(3):
            try:
                cur = self.conn.cursor(dictionary=True)
                return cur
            except mysql.errors.OperationalError as err:
                self.log.warning("Error getting cursor: %s" % str(err))
                aCTUtils.sleep(1)
                # Try closing connection and reconnecting
                self.conn.close()
                self._connect(self.dbname)

        raise Exception("Could not get cursor")

    def timeStampLessThan(self,column,timediff,utc=True):
        # utc should be set to False for columns with auto incremented timestamp
        return f"UNIX_TIMESTAMP({column}) < UNIX_TIMESTAMP({'UTC_TIMESTAMP()' if utc else ''}) - {timediff}"

    def timeStampGreaterThan(self,column,timediff,utc=True):
        # utc should be set to False for columns with auto incremented timestamp
        return f"UNIX_TIMESTAMP({column}) > UNIX_TIMESTAMP({'UTC_TIMESTAMP()' if utc else ''}) - {timediff}"

    def addLock(self):
        return " FOR UPDATE"

    def getMutexLock(self, lock_name, timeout=2):
        """
        Function to get named lock. Returns 1 if lock was obtained, 0 if attempt timed out, None if error occured.
        """
        c=self.getCursor()
        select="GET_LOCK('"+lock_name+"',"+str(timeout)+")"
        c.execute("SELECT "+select)
        return c.fetchone()[select]

    def releaseMutexLock(self, lock_name):
        """
        Function to release named lock. Returns 1 if lock was released, 0 if someone else owns the lock, None if error occured.
        """
        c=self.getCursor()
        select="RELEASE_LOCK('"+lock_name+"')"
        c.execute("SELECT "+select)
        return c.fetchone()[select]
