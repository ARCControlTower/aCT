"""
This module defines object for managing client engine's table in database.
"""
# TODO: Check if all methods from ClientDB are still used after changes.
# TODO: Check mysql escaping TODOs

import arc
import logging

from act.db.aCTDB import aCTDB


class ClientDB(aCTDB):
    """
    Object for managing client engine's table in database.

    The way MySQL exceptions are dealt with is to log and reraise
    the exception. The reason for this is that currently ClientDB does not
    check client's input so it rather passes all info on problems to client
    to deal with them.

    Another approach would be to check input and provide simpler error
    interface, but that is not the priority yet.

    Several methods support lazy flag argument that determines whether
    transaction should be commited after query. When lazy operations are used
    (lazy=True), commit should be called manually. Coneniently, ClientDB
    has :meth:`Commit`  method (inherited from ancestors).
    """

    def __init__(self, logger=logging.getLogger(__name__)):
        """
        Initialize base object.

        Args:
            logger: An object for logging.
        """
        aCTDB.__init__(self, logger, "clientjobs")

    def createTables(self):
        """Create clientjobs table."""
        c = self.db.getCursor()

        # delete table if already exists
        try:
            c.execute('DROP TABLE IF EXISTS clientjobs')
            self.Commit()
        except:
            self.log.exception('Error dropping clientjobs table')
            raise

        # create table
        query = """CREATE TABLE clientjobs (
            id INTEGER PRIMARY KEY AUTO_INCREMENT,
            modified TIMESTAMP,
            created TIMESTAMP,
            jobname VARCHAR(255),
            jobdesc integer,
            siteName VARCHAR(255),
            arcjobid integer,
            proxyid integer
        )"""
        try:
            c.execute(query)
            c.execute('ALTER TABLE clientjobs ADD INDEX (arcjobid)')
            self.Commit()
        except:
            self.log.exception('Error creating clientjobs table')
            raise

        return True

    def deleteTables(self):
        """Delete clientjobs table."""
        c = self.db.getCursor()
        try:
            c.execute('DROP TABLE clientjobs')
        except:
            self.log.exception('Error dropping clientjobs table')
            raise
        else:
            self.Commit()

    def insertJob(self, jobdesc, proxyid, siteName, lazy=False):
        """
        Insert job into clientjobs table.

        This function does not insert job decription in to the database. It
        has to be inserted separately. However, job description is still needed
        to determine the name of the job. This function is meant for clients
        that need to perform additional work on job descriptions.

        Args:
            jobdesc: A string with xRSL job description.
            proxyid: ID from proxies table of a proxy that job will
                be submitted with.
            siteName: A string with name of a site in configuration
                that job will be submitted to.
            lazy: A boolean that determines whether transaction should be
                commited after operation.

        Returns:
            ID of inserted job.
        """
        # get job name from xRSL
        jobdescs = arc.JobDescriptionList()
        # Error is not checked because caller (actsub.py) already checked
        # validity of xrsl.
        arc.JobDescription_Parse(str(jobdesc), jobdescs)
        jobname = jobdescs[0].Identification.JobName

        # insert job
        query = """
            INSERT INTO clientjobs (created, jobname, jobdesc, siteName, proxyid)
            VALUES (%s, %s, %s, %s, %s)
        """
        c = self.db.getCursor()
        try:
            c.execute(query, [self.getTimeStamp(), jobname, None, siteName, proxyid])
            c.execute('SELECT LAST_INSERT_ID()')
            jobid = c.fetchone()['LAST_INSERT_ID()']
        except:
            self.log.exception('Error while inserting new job')
            raise
        else:
            if not lazy:
                self.Commit()
            return jobid

    def insertDescription(self, jobdesc, lazy=False):
        c = self.db.getCursor()

        # first, insert job description and retreive the job ID
        try:
            query = 'INSERT INTO jobdescriptions (jobdescription) VALUES (%s)'
            c.execute(query, [jobdesc])
            c.execute('SELECT LAST_INSERT_ID()')
            jobdescid = c.fetchone()['LAST_INSERT_ID()']
        except:
            self.log.exception('Error inserting job description')
            raise
        else:
            return jobdescid


    def insertJobAndDescription(self, jobdesc, proxyid, siteName, lazy=False):
        """
        Insert job into clientjobs and job description into jobdescriptions.

        This function also inserts job description. It is meant for clients
        that can insert everything at the same time.

        Args:
            jobdesc: A string with xRSL job description.
            proxyid: ID from proxies table of a proxy that job will
                be submitted with.
            siteName: A string with name of a site in configuration
                that job will be submitted to.
            lazy: A boolean that determines whether transaction should be
                commited after operation.

        Returns:
            ID of inserted job.
        """
        c = self.db.getCursor()

        # first, insert job description and retreive the job ID
        try:
            query = 'INSERT INTO jobdescriptions (jobdescription) VALUES (%s)'
            c.execute(query, [jobdesc])
            c.execute('SELECT LAST_INSERT_ID()')
            jobdescid = c.fetchone()['LAST_INSERT_ID()']
        except:
            self.log.exception('Error inserting job description')
            raise

        # get job name from xRSL
        jobdescs = arc.JobDescriptionList()
        arc.JobDescription_Parse(str(jobdesc), jobdescs)
        jobname = jobdescs[0].Identification.JobName

        # insert job
        query = """
            INSERT INTO clientjobs (created, jobname, jobdesc, siteName, proxyid)
            VALUES (%s, %s, %s, %s, %s)
        """
        c = self.db.getCursor()
        try:
            c.execute(query, [self.getTimeStamp(), jobname, jobdescid, siteName, proxyid])
            c.execute('SELECT LAST_INSERT_ID()')
            jobid = c.fetchone()['LAST_INSERT_ID()']
        except:
            self.log.exception('Error while inserting new job')
            raise
        else:
            if not lazy:
                self.Commit()
            return jobid

    def insertArcJob(self, jobdesc, jobdescid, proxyid='', maxattempts=0, clusterlist='', appjobid='', downloadfiles='', fairshare=''):
        '''
        Insert job into arcjobs table.

        This function is a modified version of insertArcJobDescription from aCTDBArc
        module. Because client engine uses jobdescriptions table to store job descriptions,
        it cannot use job insertion functions from aCTDBArc for passing jobs to ARC engine,
        because those insert job description themselves (duplicating it).

        Function is kept to be similar and violate some conventions (exceptions) of other
        functions in this module (clientdb) on purpose for now.
        '''
        # extract priority from job desc (also checks if desc is valid)
        jobdescs = arc.JobDescriptionList()
        if not arc.JobDescription_Parse(str(jobdesc), jobdescs):
            self.log.error("%s: Failed to prepare job description" % appjobid)
            return None
        priority = jobdescs[0].Application.Priority
        if priority == -1: # use nicer default priority
            priority = 50

        c = self.db.getCursor()

        desc = {}
        desc['created'] = self.getTimeStamp()
        desc['arcstate'] = "tosubmit"
        desc['tarcstate']  = desc['created']
        desc['tstate'] = desc['created']
        desc['cluster']  = ''
        desc['clusterlist'] = clusterlist
        desc['jobdesc'] = jobdescid
        desc['attemptsleft'] = maxattempts
        desc['proxyid'] = proxyid
        desc['appjobid'] = appjobid
        desc['downloadfiles'] = downloadfiles
        desc['priority'] = priority
        desc['fairshare'] = fairshare
        s="insert into arcjobs" + " ( " + \
            ",".join(["%s" % (k) for k in desc.keys()]) + \
            " ) " + " values " + " ( " + \
            ",".join(['%s' % (k) for k in ["%s"] * len(desc)]) + " ) "
        c.execute(s, list(desc.values()))
        c.execute("SELECT LAST_INSERT_ID()")
        row = c.fetchone()
        self.Commit()
        return row

    def deleteJobs(self, where, params):
        """
        Delete jobs from table.

        Args:
            where: A string with custom WHERE clause for DELETE query. Empty
                means all rows.

        Returns:
            Number of rows deleted.
        """
        query = 'DELETE FROM clientjobs'
        if where:
            query += ' WHERE {}'.format(where)

        c = self.db.getCursor()
        try:
            c.execute(query, params)
        except:
            self.log.exception('Error deleting jobs with query: "{}"'.format(where))
            raise
        else:
            self.Commit()
            return c.rowcount

    # Although function is only used inside of aCT, column checking is still
    # done in case it gets into API.
    def getJobsInfo(self, columns=[], **kwargs):
        """
        Return info for selected jobs.

        Args:
            columns: A list of column names that will be fetched.
            **kwargs: Additional arguments for SQL statement:
                where: A string with WHERE clause.
                where_params: A list with values for WHERE clause.
                order_by: A string with ORDER BY clause.
                order_by_params: A list with values for ORDER BY clause.
                limit: An integer with number of rows to be returned.

        Returns:
            A list of dictionaries of column_name:value.
        """
        if not self._checkColumns('clientjobs', columns):
            return None

        # query params
        params = []
        # create query
        query = 'SELECT {} FROM clientjobs '.format(
            self._column_list2str(columns))
        if 'where' in kwargs:
            query += ' WHERE {} '.format(kwargs['where'])
            params.extend(kwargs['where_params'])
        if 'order_by' in kwargs:
            query += ' ORDER BY {} '.format(kwargs['order_by'])
            params.extend(kwargs['order_by_params'])
        if 'limit' in kwargs:
            query += ' LIMIT %s'
            params.append(kwargs['limit'])

        # execute
        c = self.db.getCursor()
        try:
            c.execute(query, params)
        except:
            self.log.exception('Error getting job info')
            raise
        else:
            return c.fetchall()

    def getProxies(self):
        """Return a list of all proxies in client engine's table."""
        c = self.db.getCursor()
        try:
            c.execute('SELECT DISTINCT(proxyid) AS proxyid FROM clientjobs')
        except:
            self.log.exception('Error getting proxies')
            raise
        else:
            rows = c.fetchall()
            return [row['proxyid'] for row in rows]

    def updateJob(self, jobid, patch, lazy=False):
        """
        Update job wih given information.

        Information is given as a dictionary, keys are column names and values
        are new column values.

        Args:
            jobid: ID of a job to be changed.
            patch: A dictionary with new job information. Keys are fields
                where values should be set.
            lazy: A boolean that determines whether transaction should be
                commited after operation.
        """
        if not self._checkColumns('clientjobs', patch.keys()):
            raise Exception("Invalid job attribute")

        query = 'UPDATE clientjobs SET '
        params = []
        for key in patch.keys():
            query += '{} = %s, '.format(key)
            params.append(patch[key])
        query = query.rstrip(', ')
        query += ' WHERE id = %s'
        params.append(jobid)

        c = self.db.getCursor()
        try:
            c.execute(query, params)
        except:
            self.log.exception('Error updating job {}'.format(jobid))
            raise
        else:
            if not lazy:
                self.Commit()

    # TODO: mysql escaping
    def getJoinJobsInfo(self, clicols=[], arccols=[], **kwargs):
        """
        Return job info from ARC engine's and client engine's table inner join.

        Args:
            clicols: A list of fields from client engine's table that will
                be fetched.
            arccols: A list of fields from arc engine's table that will
                be fetched.
            **kwargs: Additional arguments for SQL statement:
                where: A string with WHERE clause.
                where_params: A list with values for WHERE clause.
                order_by: A string with ORDER BY clause.
                order_by_params: A list with values for ORDER BY clause.
                limit: An integer with number of rows to be returned.

        Returns:
            A list of dictionaries with column_name:value. Column names
            will have 'c_' prepended for columns from client engine's table
            and 'a_' for columns from ARC engine's table.
        """
        if not clicols and not arccols:
            return []

        if not self._checkColumns('clientjobs', clicols) or \
                not self._checkColumns('arcjobs', arccols):
            raise Exception("Invalid columns")

        c = self.db.getCursor()
        query = "SELECT "
        # prepend table name for all columns and add them to query
        for col in clicols:
            query += 'c.{} AS c_{}, '.format(col, col)
        for col in arccols:
            query += 'a.{} AS a_{}, '.format(col, col)
        query = query.rstrip(', ') # strip last comma and space

        # inner join
        query += " FROM clientjobs c "
        query += " INNER JOIN arcjobs a ON c.arcjobid = a.id"

        params = []
        # select job
        if 'where' in kwargs:
            query += ' WHERE {}'.format(kwargs['where'])
            params.extend(kwargs['where_params'])
        if 'order_by' in kwargs:
            query += ' ORDER BY {}'.format(kwargs['order_by'])
            params.extend(kwargs['order_by_params'])
        if 'limit' in kwargs:
            query += ' LIMIT %s'
            params.append(kwargs['limit'])

        c = self.db.getCursor()
        try:
            c.execute(query, params)
        except:
            self.log.exception('Error getting inner join for query {}'.format(query))
            raise
        else:
            return c.fetchall()

    # TODO: Most of code is duplicated, refactor!
    # TODO: mysql escaping
    def getLeftJoinJobsInfo(self, clicols=[], arccols=[], **kwargs):
        """
        Return job info from ARC engine's and client engine's table left join.

        This method is almost exactly the same as :meth:`getJoinJobsInfo`
        but it uses left join instead of inner join. That is necessary to be
        able to get info for jobs which haven't been fed to ARC engine yet.

        Args:
            clicols: A list of fields from client engine's table that will
                be fetched.
            arccols: A list of fields from arc engine's table that will
                be fetched.
            **kwargs: Additional arguments for SQL statement:
                where: A string with WHERE clause.
                where_params: A list with values for WHERE clause.
                order_by: A string with ORDER BY clause.
                order_by_params: A list with values for ORDER BY clause.
                limit: An integer with number of rows to be returned.

        Returns:
            A list of dictionaries with column_name:value. Column names
            will have 'c_' prepended for columns from client engine's table
            and 'a_' for columns from ARC engine's table.
        """
        if not clicols and not arccols:
            return []

        if not self._checkColumns('clientjobs', clicols) or \
                not self._checkColumns('arcjobs', arccols):
            raise Exception("Invalid columns")

        c = self.db.getCursor()
        query = "SELECT "
        # prepend table name for all columns and add them to query
        for col in clicols:
            query += 'c.{} AS c_{}, '.format(col, col)
        for col in arccols:
            query += 'a.{} AS a_{}, '.format(col, col)
        query = query.rstrip(', ') # strip last comma and space

        # left join
        query += " FROM clientjobs c "
        query += "LEFT JOIN arcjobs a ON c.arcjobid = a.id"

        params = []
        # select job
        if 'where' in kwargs:
            query += ' WHERE {}'.format(kwargs['where'])
            params.extend(kwargs['where_params'])
        if 'order_by' in kwargs:
            query += ' ORDER BY {}'.format(kwargs['order_by'])
            params.extend(kwargs['order_by_params'])
        if 'limit' in kwargs:
            query += ' LIMIT %s'
            params.append(kwargs['limit'])

        c = self.db.getCursor()
        try:
            c.execute(query, params)
        except:
            self.log.exception('Error getting left join for query {}'.format(query))
            raise
        else:
            return c.fetchall()

    # TODO: mysql escaping
    def getColumns(self, tableName):
        """
        Get column names for table in database.

        The reason why this method allows to get column names for any table
        in database is because other aCT engines do not provide this
        functionality. So to keep things simple, this method is used also to
        get column names for ARC engine's table.

        The most semantic solution would be that this method just returned
        column names from client engine's table and that ARC engine's object
        for database (:class:~`act.arc.aCTDBArc.aCTDBArc`) implemented
        the same interface.
        """
        c = self.db.getCursor()
        query = 'SHOW columns FROM {}'.format(tableName)
        try:
            c.execute(query)
        except:
            self.log.exception('Error getting columns for table {}'.format(tableName))
            raise
        else:
            rows = c.fetchall()
            return [row['Field'] for row in rows]

    def _checkColumns(self, tableName, columns):
        """Return True if all columns are in table, false otherwise."""
        tableColumns = self.getColumns(tableName)
        for col in columns:
            if col not in tableColumns:
                return False
        return True


def createMysqlEscapeList(num):
    """Create a string with a list of %s for escaping."""
    esc_str = ''
    for i in range(num):
        esc_str += '%s, '
    return esc_str.rstrip(', ')

