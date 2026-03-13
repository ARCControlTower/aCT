"""
This module defines object for managing client engine's table in database.
"""
# TODO: Check if all methods from ClientDB are still used after changes.
# TODO: Check mysql escaping TODOs

import arc
import logging

from act.db.aCTDBNEW import aCTDB
from act.client.errors import InvalidColumnError
from sqlalchemy import select, update, delete, inspect, or_, and_, insert
from act.arc.aCTDBARCModels import ArcJob, Proxy, JobDescription
from act.client.clientdbmodels import ClientJob


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

    def __init__(self, logger=logging.getLogger(__name__), db=None):
        """
        Initialize base object.

        Args:
            logger: An object for logging.
        """
        aCTDB.__init__(self, logger, "clientjobs", db=db)

    def createTables(self):
        """Create clientjobs table."""
        c = self.db.getCursor()

        # delete table if already exists
        try:
            c.execute('DROP TABLE IF EXISTS clientjobs')
            self.Commit()
        except Exception as exc:
            self.log.error(f'Error dropping clientjobs table: {exc}')
            c.close()
            raise

        # create table
        query = """CREATE TABLE clientjobs (
            id INTEGER PRIMARY KEY AUTO_INCREMENT,
            modified TIMESTAMP,
            created TIMESTAMP,
            jobname VARCHAR(255),
            jobdesc mediumtext,
            clusterlist VARCHAR(1024),
            arcjobid integer,
            proxyid integer
        )"""
        try:
            c.execute(query)
            c.execute('ALTER TABLE clientjobs ADD INDEX (arcjobid)')
            self.Commit()
        except Exception as exc:
            self.log.error(f'Error creating clientjobs table: {exc}')
            raise
        finally:
            c.close()

        return True

    def deleteTables(self):
        """Delete clientjobs table."""
        c = self.db.getCursor()
        try:
            c.execute('DROP TABLE clientjobs')
        except Exception as exc:
            self.log.error(f'Error dropping clientjobs table: {exc}')
            raise
        else:
            self.Commit()
        finally:
            c.close()

    def insertJob(self, proxyid, session, clusterlist):
        """
        Insert job into clientjobs table.

        Args:
            proxyid: ID from proxies table of a proxy that job will
                be submitted with.
            session: SQLAlchemy session
            clusterlist: A string of comma separated URLs of clusters that job
                will be submitted to.

        Returns:
            ID of inserted job.
        """
        return session.execute(insert(ClientJob).values(proxyid=proxyid, clusterlist=clusterlist).returning(ClientJob.id)).scalar_one()

    def deleteJobs(self, session, jobids, table):
        """
        Delete jobs from table.

        Args:
            session: SQLAlchemy session
            jobids: list of arcjobs or clientjobs ids
            table: SQLAlchemy orm object (ClientJob/ArcJob)

        Returns:
            Number of rows deleted.
        """
        session.execute(delete(table).where(table.id.in_(jobids)))

    # Although function is only used inside of aCT, column checking is still
    # done in case it gets into API.
    def getJobsInfo(self, proxyid, session, limit):
        """
        Return info for selected jobs. Lock with FOR UPDATE.

        Args:
            proxyid: id of proxy
            session: sqlalchemy session
            limit: limit for number of rows

        Returns:
            A list of dictionaries of column_name:value.
        """
        stmt = select(ClientJob.id, ClientJob.jobdesc, ClientJob.clusterlist) \
            .where(ClientJob.proxyid==proxyid, ClientJob.arcjobid.is_(None), ClientJob.jobdesc.is_not(None)) \
            .order_by(ClientJob.id).limit(limit).with_for_update()
        return session.execute(stmt).all()

    def getProxies(self, session):
        """Return a list of all proxies in client engine's table."""
        rows=session.execute(select(ClientJob.proxyid).distinct())
        return [row.proxyid for row in rows]

    def updateJob(self, proxyid, session, jobid, values_dict):
        """
        Update clientjob wih given values.

        Args:
            proxyid: ID of proxy for this job
            session: sqlalchemy session object
            jobid: ID of a job to be changed.
            values_dict: dictionary of values for job columns to be updated
        """
        session.execute(update(ClientJob).where(ClientJob.proxyid==proxyid, ClientJob.id==jobid).values(**values_dict))

    # TODO: mysql escaping
    def getJoinJobsInfo(self, proxyid, session, jobids=None, state_filter=None, name_filter=None, clicols=[], arccols=[], jobname=None, forupdate=False):
        """
        Return job info from ARC engine's and client engine's table inner join.

        Args:
            proxyid: ID of proxy for the specified job
            session: an sqlalchemy session object
            jobids: a list of clientjob ids
            state_filter: a list of states that the arcjob arcstate must be in.
                if the list is empty, we do a left outer join.
            name_filter: a string that the jobname must include
            clicols: A list of fields from client engine's table that will
                be fetched.
            arccols: A list of fields from arc engine's table that will
                be fetched.
            jobname: String that must equal the jobname of the clientjob
            forupdate: Boolean that locks selected rows with FOR UPDATE

        Returns:
            A list of row objects with column_name:value. Column names
            will have 'c_' prepended for columns from client engine's table
            and 'a_' for columns from ARC engine's table.
        """
        selected_columns = []
        for colname in clicols:
            col = getattr(ClientJob, colname)
            selected_columns.append(col.label(f'c_{colname}'))
        for colname in arccols:
            col = getattr(ArcJob, colname)
            selected_columns.append(col.label(f'a_{colname}'))
        
        if not selected_columns:
            return []

        stmt = select(*selected_columns)

        if state_filter:
            stmt = stmt.join(ClientJob.arcjob).where(ArcJob.arcstate.in_(state_filter))
        else:
            stmt = stmt.outerjoin(ClientJob.arcjob)
        stmt = stmt.where(ClientJob.proxyid==proxyid)

        if jobname:
            stmt = stmt.where(ClientJob.jobname==jobname)
        elif name_filter:
            escaped = name_filter.replace('_', r'\_')
            stmt = stmt.where(ClientJob.jobname.like(f'%{escaped}%', escape='\\'))

        if jobids:
            stmt = stmt.where(ClientJob.id.in_(jobids))

        if forupdate:
            stmt = stmt.with_for_update()

        return session.execute(stmt).all()

    def updateArcstate(self, session, jobids, arcstate):
        session.execute(update(ArcJob).where(ArcJob.id.in_(jobids)).values(arcstate=arcstate, tarcstate=self.getTimeStamp()))

    def checkClientJobs(self, proxyid, session, jobids):
        return session.execute(select(ClientJob.id).where(ClientJob.proxyid==proxyid, ClientJob.id.in_(jobids))).all()
    
    def getProxyInfo(self, session, filter, columns):
        selected_columns = []
        for colname in columns:
            col = getattr(Proxy, colname)
            selected_columns.append(col)
        session.execute(select(*selected_columns).filter_by(**filter)).first()
