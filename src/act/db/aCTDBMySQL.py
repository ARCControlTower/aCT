import mysql.connector as mysql
from act.db.aCTDBMS import aCTDBMS

class aCTDBMySQL(aCTDBMS):
    """Class for MySQL specific db operations."""

    def __init__(self, log, config):
        aCTDBMS.__init__(self, log, config)
        # mysql.connector must be 2.1.x
        if mysql.__version_info__[:2] != (2, 1):
            raise Exception("mysql-connector must be version 2.1.x")
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

    def _connect(self, dbname=None):
        if self.socket != 'None':
            self.conn=mysql.connect(unix_socket=self.socket,db=dbname)
        elif self.user and self.passwd:
            if self.host != 'None' and self.port != 'None':
                self.conn=mysql.connect(user=self.user, password=self.passwd, host=self.host, port=self.port, db=dbname)
            else:
                self.conn=mysql.connect(user=self.user, password=self.passwd, db=dbname)

    def getCursor(self):
        # make sure cursor reads newest db state
        self.conn.commit()
        tries = 3
        while tries:
            try:
                cur = self.conn.cursor(dictionary=True)
                return cur
            except mysql.errors.OperationalError as err:
                self.log.warning("Error getting cursor: %s" % str(err))
            tries -= 1
        raise Exception("Could not get cursor")

    def timeStampLessThan(self,column,timediff):
        return "UNIX_TIMESTAMP("+column+") < UNIX_TIMESTAMP(UTC_TIMESTAMP()) - "+str(timediff)

    def timeStampGreaterThan(self,column,timediff):
        return "UNIX_TIMESTAMP("+column+") > UNIX_TIMESTAMP(UTC_TIMESTAMP()) - "+str(timediff)

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
