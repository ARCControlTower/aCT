"""
This is a module that provides job management functionality.
"""
# TODO: Check if all methods are still used after changes.

import logging
import shutil
import os

import arc
from act.arc.aCTDBArcNEW import aCTDBArc
from act.common.aCTConfig import aCTConfigARC, aCTConfigAPP
from act.client.clientdb import ClientDB, createMysqlEscapeList
from act.client.errors import NoSuchProxyError, NoJobDirectoryError
from act.client.errors import ConfigError, InvalidJobDescriptionError
from act.client.errors import NoSuchSiteError, InvalidJobRangeError
from act.client.errors import InvalidJobIDError
from act.client.common import readSites
from sqlalchemy import select, update, delete, inspect, or_, and_
from act.arc.aCTDBARCModels import ArcJob, Proxy, JobDescription
from act.client.clientdbmodels import ClientJob


logger = logging.getLogger(__name__)


class JobManager(object):
    """
    Object for managing jobs using tables in aCT database.

    MySQL errors that might happen are ignored unless stated otherwise where
    they are not.

    This object works heavily with ARC engine. It does that by feeding job
    information to ARC engine's table and reading job information from that
    table.

    Object has two sets of methods: methods that operate on one job and
    methods that operate on multiple jobs. Single job operations were
    implemented first and provided very verbose information to client when
    something went wrong. This approach required multiple queries for every
    single job which is very ineffective.

    Job operations were changed to solve this. Now, every database operation
    on jobs happens on all jobs that client requested in one single query.
    That means that it's impossible to see for instance why operation
    could not be executed on certain jobs that client requested. To find out,
    job state has to be checked (which makes sense). So the following
    approach is used when something goes wrong: instead of relying on
    for instance job cleaner to tell why not all jobs were cleaned, client
    should check the state of those jobs that were not cleaned.

    The former job management approach will probably be removed in future, as
    it's not used anymore.

    Attributes:
        logger: An object for logging.
        arcdb: An object that is interface to ARC engine's table.
        clidb: An object that is interface to client engine's table.
    """

    def __init__(self, db=None):
        """Initialize object's attributes."""
        self.log = logging.getLogger(__name__)
        self.arcdb = aCTDBArc(self.log, db=db)
        self.clidb = ClientDB(self.log, db=db)

        # TODO: if and when sites from arc config are used, move everything
        # that uses arc config to this class
        self.arcconf = aCTConfigARC()
        self.appconf = aCTConfigAPP()

    def checkProxy(self, proxyid):
        """
        Check if proxy exists in database.

        Function is a very thin wrapper around aCTDBArc functionality that
        adds exception instead of checking return value. Does nothing if proxy
        exists.

        Args:
            proxyid: An integer ID of proxy.

        Raises:
            NoSuchProxyError: Proxy does not exist in database.
        """
        with self.arcdb.Session() as session:
            result = session.get(Proxy, proxyid)
            if result is None:
                raise NoSuchProxyError(proxyid, None)
        #if not self.arcdb.getProxy(proxyid): # TODO
        #    raise NoSuchProxyError(proxyid, None)

    def getClientColumns(self):
        """Return a list of column names from client engine's table."""
        # TODO: hardcoded
        with self.arcdb.Session() as session:
            inspector = inspect(session.get_bind())
            columns = inspector.get_columns("clientjobs")
        return [col['name'] for col in columns]
        #return self.clidb.getColumns('clientjobs') # TODO

    def getArcColumns(self):
        """Return a list of column names from ARC engine's table."""
        # TODO: hardcoded
        with self.arcdb.Session() as session:
            inspector = inspect(session.get_bind())
            columns = inspector.get_columns("arcjobs")
        return [col['name'] for col in columns]
        #return self.clidb.getColumns('arcjobs') # TODO

    # TODO: return a list of IDs rather than number
    def cleanJobs(self, proxyid, jobids=[], state_filter='', name_filter='', clicols=[], arccols=[], jobname=''):
        """
        Clean given jobs that match optional filters.

        Clean operation happens in two steps. Information has to be fetched
        first to get job results directories that have to be cleaned. Only
        after directories have been deleted can jobs be cleaned from both
        ARC and client engines (their tables).

        Clean operation is done by setting job's ARC state to 'toclean' which
        is then further handled by ARC engine and by deleting job's entry from
        client engine's table. ARC state is set in ARC engine's table.

        Args:
            proxyid: An integer ID of proxy.
            jobids: A list of integer IDs of jobs that should be cleaned.
                Empty list means all jobs.
            state_filter: A string with state that jobs should match.
            name_filter: A string that job names should match.

        Returns:
            A list of IDs of deleted jobs.
        """
        # wrong state filter, return immediately
        # Forgot why is '' used here ...
        if state_filter not in ('', 'done', 'donefailed', 'cancelled', 'failed', 'lost'):
            return []
        if state_filter:
            state_filter = [state_filter]
        else:
            state_filter = ['done', 'donefailed', 'cancelled', 'failed', 'lost']

        with self.arcdb.Session.begin() as session:
            jobs = self.make_select(proxyid, jobids, state_filter, None, ['id'], ['id', 'arcstate', 'JobID'], None, session)
            if not jobs:
                return []

            arc_ids = []
            client_ids = []

            # Cleanup directories and prepare lists for updates/deletes
            for c_id, a_id, arcstate, jobid in jobs:
                if arcstate in ('done', 'donefailed'):
                    try:
                        jobdir = self.getJobOutputDir(jobid)
                        shutil.rmtree(jobdir)
                    except OSError:
                        self.log.error(f'Could not clean job results in {jobdir}')
                    except NoJobDirectoryError:
                        self.log.info(f'Job {c_id} has no job results to clean')

                client_ids.append(c_id)
                arc_ids.append(a_id)

            if client_ids:
                self.updateArcstate(arc_ids, 'toclean', session)
                self.deleteClientJobs(client_ids, session)

        for c_id in client_ids:
            jobdir = self.getJobOutputDir(str(c_id))
            shutil.rmtree(jobdir, ignore_errors=True)

        return client_ids
    
    def updateArcstate(self, jobids, arcstate, session):
        session.execute(update(ArcJob).where(ArcJob.id.in_(jobids)).values(arcstate=arcstate, tarcstate=self.clidb.getTimeStamp()))

    def deleteClientJobs(self, jobids, session):
        session.execute(delete(ClientJob).where(ClientJob.id.in_(jobids)))

    def forceCleanJobs(self, results):
        """
        Clean given rows from aCT tables and results in tmp.

        State of jobs is not checked. Neither is consistency whether ARC
        table entries really belong to client table entries.
        Should be used only internally as a part of bigger transaction.

        This method is used when client is getting jobs. Job results can only
        be cleaned after the client has transfered them. This is what this
        method does. It relies on :meth:`getJobs` to provide correct IDs.

        More information on getting jobs can be found in :meth:`getJobs`

        Args:
            results: A :class:`JobGetResults` object with results.
        """
        with self.arcdb.Session.begin() as session:
            if results.arcIDs: # jobs are cleaned from ARC by setting their state
                session.execute(update(ArcJob).where(ArcJob.id.in_(results.arcIDs)).values(arcstate='toclean', tarcstate=self.arcdb.getTimeStamp()))
            if results.clientIDs:
                session.execute(delete(ClientJob).where(ClientJob.id.in_(results.clientIDs)))

        for result in results.jobdicts:
            if result['dir']:
                shutil.rmtree(result['dir'])

    def fetchJobs(self, proxyid, jobids=[], name_filter=''):
        """
        Assign given failed jobs that match optional filter for fetching.

        Fetch operation is done by setting ARC state of job to 'tofetch' from
        where ARC engine takes on. ARC state is set in ARC engine's table.

        Args:
            proxyid: An integer ID of proxy.
            jobids: A list of integer IDs of jobs.
            name_filter: A string that job names should match.

        Returns:
            A list of IDs of fetched jobs.
        """
        stmt = select(ClientJob.id, ArcJob.id).join(ClientJob.arcjob).where(ClientJob.proxyid==proxyid, ArcJob.arcstate=='failed')

        if jobids:
            stmt = stmt.where(ClientJob.id.in_(jobids))

        if name_filter:
            escaped_filter = name_filter.replace('_', r'\_')
            stmt = stmt.where(ClientJob.jobname.like(f'%{escaped_filter}%', escape='\\'))

        with self.arcdb.Session.begin() as session:
            jobs = session.execute(stmt).all()

            if not jobs:
                return []
            c_ids = [c_id for c_id, _ in jobs]
            a_ids = [a_id for _, a_id in jobs]
            
            stmt = update(ArcJob).where(ArcJob.id.in_(a_ids)).values(arcstate='tofetch', tarcstate=self.arcdb.getTimeStamp())
            session.execute(stmt)
        return c_ids

    def refetchJobs(self, proxyid, jobids=[], name_filter=''):
        """
        Refetch given jobs from cluster.

        Sometimes it happens that downloaded job results are corrupt. It is
        necessary to fetch results again if that happens. This means that
        already fetched results have to be deleted as well.

        Jobs that haven't yet been fetched (for instance failed jobs) can also
        be assigned for fetching in this operation.

        Args:
            proxyid: An integer ID of proxy.
            jobids: A list of integer IDs of jobs.
            name_filter: A string that job names should match.

        Returns:
            A list of IDs of jobs that will be refetched.
        """
        # create filters in query
        stmt = select(ClientJob.id, ArcJob.id, ArcJob.arcstate, ArcJob.JobID).join(ClientJob.arcjob).where((ClientJob.proxyid==proxyid), ArcJob.arcstate.in_(['done', 'donefailed', 'failed']))

        if jobids:
            stmt = stmt.where(ClientJob.id.in_(jobids))

        if name_filter:
            escaped_filter = name_filter.replace('_', r'\_')
            stmt = stmt.where(ClientJob.jobname.like(f'%{escaped_filter}%', escape='\\'))

        with self.arcdb.Session.begin() as session:
            jobs = session.execute(stmt).all()

            if not jobs:
                return []
            
            tofetch = []
            finished = []

            for c_id, a_id, arcstate, JobID in jobs:
                if arcstate=='failed':
                    tofetch.append(a_id)
                else:
                    try:
                        jobdir = self.getJobOutputDir(JobID)
                        shutil.rmtree(jobdir, ignore_errors=True)
                    except OSError as exc:
                        # just log this problem, user doesn't need results anyway
                        self.log.error(f'Could not clean job results in {jobdir}: {exc}')
                    except NoJobDirectoryError as exc:
                        # just log this problem, user doesn't need results anyway
                        self.log.error(f'Could not clean job results in {exc.jobdir}: {exc}')
                    # finished jobs become done, tofetch jobs become donefailed;
                    # the job status should be preserved
                    if arcstate == 'done':
                        finished.append(a_id)
                    else:
                        tofetch.append(a_id)

            tstamp = self.arcdb.getTimeStamp()
            if tofetch:
                stmt = update(ArcJob).where(ArcJob.id.in_(tofetch)).values(arcstate='tofetch', tarcstate=tstamp)
                session.execute(stmt)
            if finished:
                stmt = update(ArcJob).where(ArcJob.id.in_(finished)).values(arcstate='finished', tarcstate=tstamp)
                session.execute(stmt)

        return [job.id for job in jobs]

    def getJobs(self, proxyid, jobids=[], state_filter='', name_filter=''):
        """
        Get given finished jobs that match optional filter.

        Get operation is done in two steps: first, information on where job
        results are need to be fetched and returned to client. This step is
        done by this method. Then, client needs to transfer job results to
        whatever destination. Only then can jobs be cleaned from aCT. Cleaning
        is a second step that needs to be initiated by client and is done by
        calling :meth:`forceCleanJobs`.

        This method also prepares all queries needed to clean jobs from aCT
        so that it's not needed to check and filter jobs again in second step.

        Args:
            proxyid: An integer ID of proxy.
            jobids: A list of job ID integers.
            state_filter: A string with state that jobs should match.
            name_filter: A string that job names should match.

        Returns:
            A :class:`JobGetResults` object with results.
        """
        results = JobGetResults()
        # wrong state filter, return immediately
        if state_filter not in ('', 'done', 'donefailed'):
            return results # return empty results
        # create query with filters
        stmt = select(ClientJob.id, ClientJob.jobname, ArcJob.id, ArcJob.JobID).join(ClientJob.arcjob).where(ClientJob.proxyid==proxyid)

        if state_filter:
            stmt = stmt.where(ArcJob.arcstate==state_filter)
        else:
            stmt = stmt.where(ArcJob.arcstate.in_(['done', 'donefailed']))

        if jobids:
            stmt = stmt.where(ClientJob.id.in_(jobids))

        if name_filter:
            escaped_filter = name_filter.replace('_', r'\_')
            stmt = stmt.where(ClientJob.jobname.like(f'%{escaped_filter}%', escape='\\'))

        with self.arcdb.Session.begin() as session:
            jobs = session.execute(stmt).all()

        # assemble results
        for c_id, jobname, a_id, JobID in jobs:
            try:
                srcdir = self.getJobOutputDir(JobID)
            except NoJobDirectoryError:
                srcdir = None
            results.arcIDs.append(a_id)
            results.clientIDs.append(c_id)
            results.jobdicts.append({
                'id': c_id,
                'name': jobname,
                'dir': srcdir
            })
        return results

    def killJobs(self, proxyid, jobids=[], state_filter='', name_filter=''):
        """
        Kill jobs that match optional filters.

        Kill operation is done by setting job's ARC state in ARC engine's
        table to 'tocancel'. From there, it is picked up and handled by ARC
        engine. By using left join, the jobs that haven't been submitted yet
        or that are in inconsistent state, are cleaned as well.

        Jobs that are waiting for submission can also be killed, which deletes
        them immediately.

        Args:
            proxyid: An integer ID of proxy.
            jobids: A list of job ID integers.
            state_filter: A string with state that jobs should match.
            name_filter: A string that job names should match.

        Returns:
            A list of job dictionaries.
        """
        # wrong state filter, return immediately
        valid_states = ('submitted', 'running', '', 'tosubmit', 'submitting')
        if state_filter not in valid_states:
            return []
        
        stmt = select(ClientJob.id, ArcJob.id, ArcJob.arcstate).outerjoin(ClientJob.arcjob).where(ClientJob.proxyid == proxyid)

        if state_filter:
            stmt = stmt.where(ArcJob.arcstate==state_filter)
        else:
            stmt = stmt.where(or_(ArcJob.arcstate.in_(valid_states), ArcJob.arcstate.is_(None)))

        if jobids:
            stmt = stmt.where(ClientJob.id.in_(jobids))

        if name_filter:
            escaped_filter = name_filter.replace('_', r'\_')
            stmt = stmt.where(ClientJob.jobname.like(f'%{escaped_filter}%', escape='\\'))

        stmt = stmt.with_for_update(of=ArcJob)#, skip_locked=True) mariadb 10.6+

        with self.arcdb.Session.begin() as session:
            jobs = session.execute(stmt).all()

            if not jobs:
                return []
            
            arc_ids = []
            client_ids = []
            for c_id, a_id, arcstate in jobs:
                if a_id is None:
                    # If id from arcjobs is NULL, then job is either waiting or in
                    # inconsistent state. The job has to be deleted.
                    client_ids.append(c_id)
                elif arcstate == 'tosubmit':
                    # 'tosubmit' jobs cannot be set to tocancel, they have to be deleted
                    # immediately.
                    client_ids.append(c_id)
                    session.execute(delete(ArcJob).where(ArcJob.id==a_id))
                else:
                    # If there is entry in arcjobs, the job can be killed by
                    # setting its state to 'tocancel'
                    arc_ids.append(a_id)

            if arc_ids:
                stmt = update(ArcJob).where(ArcJob.id.in_(arc_ids)).values(arcstate='tocancel', tarcstate=self.arcdb.getTimeStamp())
                session.execute(stmt)
            if client_ids:
                stmt = delete(ClientJob).where(ClientJob.id.in_(client_ids))
                session.execute(stmt)

        return [{"c_id": c, "a_id": a, "a_arcstate": s} for c, a, s in jobs]

    def resubmitJobs(self, proxyid, jobids=[], name_filter=''):
        """
        Assign given jobs that match optional filter for resubmission.

        Resubmit operation is done by setting ARC state in ARC engine's table
        to 'toresubmit' from where on it's picked up by ARC engine.

        Args:
            proxyid: An integer ID of proxy.
            jobids: A list of job ID integers.
            name_filter: A string that job names should match.

        Returns:
            A list of IDs of jobs that will be resubmitted.
        """
        # create query with filters
        stmt = select(ClientJob.id, ArcJob.id).join(ClientJob.arcjob).where(ArcJob.arcstate.in_(['failed', 'donefailed']), ClientJob.proxyid==proxyid)

        if jobids:
            stmt = stmt.where(ClientJob.id.in_(jobids))

        if name_filter:
            escaped_filter = name_filter.replace('_', r'\_')
            stmt = stmt.where(ClientJob.jobname.like(f'%{escaped_filter}%', escape='\\'))

        with self.arcdb.Session.begin() as session:
            jobs = session.execute(stmt).all()

            if not jobs:
                return[]
            
            #set job state for resubmittion
            stmt = update(ArcJob).where(ArcJob.id.in_([job.arcjob.id for job in jobs])).values(arcstate='toresubmit', tarcstate=self.arcdb.getTimeStamp())
            session.execute(stmt)

        return [job.id for job in jobs]

    def getJobStats(self, proxyid, jobids=[], state_filter='', name_filter='', clicols=[], arccols=[], jobname=''):
        """
        Return info for jobs that match optional filters.

        Job information is fetched from both ARC and client engines
        (their tables). This is done by using inner or left table join.
        Left join is needed to get information for jobs that are not in ARC
        engine yet and therefore cannot be fetched by inner join.

        Args:
            proxyid: An integer ID of proxy.
            jobids: A list of job ID integers.
            state_filter: A string with state that jobs should match.
            name_filter: A string that job names should match.
            clicols: A list of fields from client engine's table that will
                be fetched.
            arccols: A list of fields from arc engine's table that will
                be fetched.

        Returns:
            A list of dictionaries with column_name:value. Column names
            will have 'c_' prepended for columns from client engine's table
            and 'a_' for columns from ARC engine's table.
        """
        if state_filter:
            state_filter = [state_filter]
        with self.arcdb.Session() as session:
            result = self.make_select(proxyid, jobids, state_filter, name_filter, clicols, arccols, jobname, session)

        jobs = [dict(row._mapping) for row in result]
        return jobs
    
    def make_select(self, proxyid, jobids, state_filter, name_filter, clicols, arccols, jobname, session):
        selected_columns = []
        for colname in clicols:
            col = getattr(ClientJob, colname)
            selected_columns.append(col.label(f'c_{colname}'))
        for colname in arccols:
            col = getattr(ArcJob, colname)
            selected_columns.append(col.label(f'a_{colname}'))

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

        return session.execute(stmt).all()


    def getJobOutputDir(self, arcid):
        """
        Return job's directory name in aCT.

        Directory name is the same as job's ARC ID, which is stored in
        ARC engine.

        Args:
            arcid: A string with job's ARC ID.

        Raises:
            ConfigError: tmp directory is not configured in aCT.
            NoJobDirectoryError: Job directory does not exist in aCT.
        """
        tmpdir = self.arcconf.tmp.dir
        if not tmpdir:
            self.log.error('tmp directory is not in config')
            raise ConfigError("config/tmp/dir")
        jobdir = arcid.rsplit('/', 1)[-1]
        actJobDir = os.path.join(tmpdir, jobdir)
        if os.path.isdir(actJobDir):
            return actJobDir
        else:
            raise NoJobDirectoryError(actJobDir)

    def getJobDataDir(self, jobid):
        datapath = self.appconf.user.datman or None
        if not datapath:
            raise ConfigError("config/actlocation/datman")
        return os.path.join(datapath, str(jobid))

    def _createMysqlIntList(self, integers):
        """
        Create string with integers separated by comma and space.

        Used for creating MySQL queries with job IDs.

        Args:
            integers: A list of job ID integers.

        Returns:
            A string of integers separated by comma and space.
        """
        where = ''
        if integers:
            for integer in integers:
                where += f'{integer}, '
            where = where.rstrip(', ')
        return where

    def _addNameFilter(self, name_filter='', where='', where_params=[]):
        if name_filter:
            where += " c.jobname LIKE BINARY %s AND "
            escaped_filter = name_filter.replace('_', r'\_')
            where_params.append('%' + escaped_filter + '%')
        return where, where_params

    def _addIDFilter(self, ids=[], where='', where_params=[]):
        if ids:
            #if len(ids) == 1:
            #    where += ' c.id = %s '
            #    where_params.append(ids[0])
            #else:
            #    where += ' c.id IN ({}) AND '.format(createMysqlEscapeList(len(ids)))
            #    where_params.extend(ids)
            where += f' c.id IN ({createMysqlEscapeList(len(ids))}) AND '
            where_params.extend(ids)
        return where, where_params

    def checkJobExists(self, proxyid, jobid):
        """Returns given jobid if job exists or None if not."""
        jobdicts = self.getJobStats(proxyid, [jobid], "", "", ["id"], [], "")
        if not jobdicts:
            return None
        else:
            return jobdicts[0]["c_id"]


class JobGetResults(object):
    """
    Object with results from get job operation.

    Getting jobs requires two steps: fetching job info and after client
    copies job results, cleaning jobs. To avoid filtering jobs in second
    step, :meth:`JobManager.getJobs` returns filtered jobs that should be
    deleted in two lists, one for ARC engine and one for client engine.

    These are multiple values and it is probably the best idea to put them
    in a dedicated data structure rather than pack them up in a tuple.

    Attributes:
        jobdicts: Dictionaries with job information, keys are fields from
            tables and values values from tables.
        arcIDs: A list of ID integers of entries from ARC engine's table.
        clientIDs: A list of ID integers of entries from client engine's table.
    """

    def __init__(self):
        self.jobdicts = []
        self.arcIDs = []
        self.clientIDs = []


def checkJobDesc(jobdesc):
    """
    Check if job description is valid.

    This part is taken from aCTDBArc.py and should be kept updated.

    Args:
        jobdesc: A string with job xRSL job description.

    Raises:
        InvalidJobDescriptionError: Job description is invalid.
    """
    jobdescs = arc.JobDescriptionList()
    if not arc.JobDescription.Parse(str(jobdesc), jobdescs):
        logger.error('Job description is not valid')
        raise InvalidJobDescriptionError()


def checkSite(siteName, confpath='/etc/act/sites.json'):
    """
    Check if site is configured.

    Function also logs and reraises configuration related problems.

    Args:
        siteName: A string with name of site in config.
        confpath: A string with path to configuration file.

    Raises:
        NoSuchSiteError: Site is not in configuration.
    """
    try:
        sites = readSites()
        for site in sites:
            if site == siteName:
                return
    except Exception as exc:
        logger.error(f'Problem reading configuration: {exc}')
        raise

    raise NoSuchSiteError(siteName)


def getIDsFromList(listStr):
    """
    Return a list of IDs from comma separated list of job IDs.

    Parsing logic is very simple, the list is first split by commas.
    Then every substring is checked whether it can be also split by
    dash. Exception is raised as soon as problem is encountered.

    Args:
        listStr: A string with ID list that should be parsed.

    Returns:
        A list of job ID integers.

    Raises:
        InvalidJobRangeError: One of the ranges in list is invalid.
        InvalidJobIDError: One of the job IDs is not an integer.
    """
    groups = listStr.split(',')
    ids = []
    for group in groups:
        try:
            group.index('-')
        except ValueError:
            isRange = False
        else:
            isRange = True

        if isRange:
            try:
                firstIx, lastIx = group.split('-')
            except ValueError: # if there is more than one dash
                raise InvalidJobRangeError(group)
            try:
                firstIx = int(firstIx)
            except ValueError:
                raise InvalidJobIDError(firstIx)
            try:
                lastIx = int(lastIx)
            except ValueError:
                raise InvalidJobIDError(lastIx)
            ids.extend(range(int(firstIx), int(lastIx) + 1))
        else:
            try:
                ids.append(int(group))
            except ValueError:
                raise InvalidJobIDError(group)
    return ids


