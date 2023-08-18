"""
This is a module that provides job management functionality.
"""
# TODO: Check if all methods are still used after changes.

import logging
import shutil
import os

import arc
from act.arc.aCTDBArc import aCTDBArc
from act.common.aCTConfig import aCTConfigARC, aCTConfigAPP
from act.client.clientdb import ClientDB, createMysqlEscapeList
from act.client.errors import NoSuchProxyError, NoJobDirectoryError
from act.client.errors import ConfigError, InvalidJobDescriptionError
from act.client.errors import NoSuchSiteError, InvalidJobRangeError
from act.client.errors import InvalidJobIDError
from act.client.common import readSites


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
        self.logger = logging.getLogger(__name__)
        self.arcdb = aCTDBArc(self.logger, db=db)
        self.clidb = ClientDB(self.logger, db=db)

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
        if not self.arcdb.getProxy(proxyid):
            raise NoSuchProxyError(proxyid, None)

    def getClientColumns(self):
        """Return a list of column names from client engine's table."""
        # TODO: hardcoded
        return self.clidb.getColumns('clientjobs')

    def getArcColumns(self):
        """Return a list of column names from ARC engine's table."""
        # TODO: hardcoded
        return self.clidb.getColumns('arcjobs')

    # TODO: return a list of IDs rather than number
    def cleanJobs(self, proxyid, jobids=[], state_filter='', name_filter=''):
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

        # create filters in query
        where = ' c.proxyid = %s AND '
        where_params = [proxyid]
        if state_filter:
            where += " a.arcstate = %s AND "
            where_params.append(state_filter)
        else:
            # otherwise, get all jobs that can be cleaned
            where += " a.arcstate IN ( 'done', 'donefailed', 'cancelled', 'failed', 'lost' ) AND "
        where, where_params = self._addNameFilter(name_filter, where, where_params)
        where, where_params = self._addIDFilter(jobids, where, where_params)
        where = where.rstrip('AND ')

        jobs = self.clidb.getJoinJobsInfo(
                clicols=['id'],
                arccols=['arcstate', 'JobID', 'id'],
                where=where,
                where_params=where_params)

        if not jobs:
            return []

        # create where clauses for removal and remove tmp dirs
        arc_where = ''
        client_params = []
        deletedIDs = []
        for job in jobs:
            # remove results folder; if none, just log and don't notify user
            if job['a_arcstate'] in ('done', 'donefailed'):
                try:
                    jobdir = self.getJobOutputDir(job['a_JobID'])
                    shutil.rmtree(jobdir, ignore_errors=True)
                except OSError:
                    # just log this problem, user doesn't need results anyway
                    self.logger.error(f'Could not clean job results in {jobdir}')
                except NoJobDirectoryError:
                    # just log this problem, user doesn't need results anyway
                    self.logger.info(f'Job {job["c_id"]} has no job results to clean')

            # add job to removal query
            arc_where += f'{job["a_id"]}, '
            client_params.append(int(job['c_id']))

            deletedIDs.append(job['c_id'])

        # delete jobs from tables
        if deletedIDs:
            arc_where = arc_where.rstrip(', ')
            arc_where = f' id IN ({arc_where})'
            client_where = f' id IN ({createMysqlEscapeList(len(client_params))})'
            patch = {'arcstate': 'toclean', 'tarcstate': self.arcdb.getTimeStamp()}
            self.arcdb.updateArcJobs(patch, arc_where)
            self.clidb.deleteJobs(client_where, client_params)

        return deletedIDs

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
        if results.arcIDs: # jobs are cleaned from ARC by setting their state
            arc_where = f' id in ({self._createMysqlIntList(results.arcIDs)})'
            patch = {'arcstate': 'toclean', 'tarcstate': self.arcdb.getTimeStamp()}
            self.arcdb.updateArcJobs(patch, arc_where)
        if results.clientIDs:
            client_where = f' id in ({createMysqlEscapeList(len(results.clientIDs))})'
            self.clidb.deleteJobs(client_where, results.clientIDs)

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
        # create query with filtering
        where = " a.arcstate = 'failed' AND c.proxyid = %s AND "
        where_params = [proxyid]
        where, where_params = self._addNameFilter(name_filter, where, where_params)
        where, where_params = self._addIDFilter(jobids, where, where_params)
        where = where.rstrip('AND ')

        jobs = self.clidb.getJoinJobsInfo(
                clicols=['id'],
                arccols=['id'],
                where=where,
                where_params=where_params)

        if not jobs:
            return []

        # update jobs' state for fetching
        where = f' id IN ({self._createMysqlIntList([job["a_id"] for job in jobs])})'
        patch = {'arcstate': 'tofetch', 'tarcstate': self.arcdb.getTimeStamp()}
        self.arcdb.updateArcJobs(patch, where)

        return [job['c_id'] for job in jobs]

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
        where = ' c.proxyid = %s AND '
        where_params = [proxyid]
        where += " a.arcstate IN ( 'done', 'donefailed', 'failed' ) AND "
        where, where_params = self._addNameFilter(name_filter, where, where_params)
        where, where_params = self._addIDFilter(jobids, where, where_params)
        where = where.rstrip('AND ')

        jobs = self.clidb.getJoinJobsInfo(
                clicols=['id'],
                arccols=['arcstate', 'JobID', 'id'],
                where=where,
                where_params=where_params)

        if not jobs:
            return []

        tstamp = self.arcdb.getTimeStamp()
        for job in jobs:
            if job['a_arcstate'] == 'failed':
                patch = {'arcstate': 'tofetch'}
            else:
                try:
                    jobdir = self.getJobOutputDir(job['a_JobID'])
                    shutil.rmtree(jobdir, ignore_errors=True)
                except OSError:
                    # just log this problem, user doesn't need results anyway
                    self.logger.exception(f'Could not clean job results in {jobdir}')
                except NoJobDirectoryError as e:
                    # just log this problem, user doesn't need results anyway
                    self.logger.exception(f'Could not clean job results in {e.jobdir}')

                # finished jobs become done, tofetch jobs become donefailed;
                # the job status should be preserved
                if job['a_arcstate'] == 'done':
                    patch = {'arcstate': 'finished'}
                else:
                    patch = {'arcstate': 'tofetch'}

            where = f' id = {job["a_id"]}'
            patch['tarcstate'] = tstamp
            self.arcdb.updateArcJobs(patch, where)

        return [job['c_id'] for job in jobs]

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
        where = ' c.proxyid = %s AND '
        where_params = [proxyid]
        if state_filter:
            where += " a.arcstate = %s AND "
            where_params.append(state_filter)
        else:
            # otherwise, get all jobs that can be "getted"
            where += " a.arcstate IN ( 'done', 'donefailed' ) AND "
        where, where_params = self._addNameFilter(name_filter, where, where_params)
        where, where_params = self._addIDFilter(jobids, where, where_params)
        where = where.rstrip('AND ')

        jobs = self.clidb.getJoinJobsInfo(
                clicols=['jobname', 'id'],
                arccols=['JobID', 'id'],
                where=where,
                where_params=where_params)

        # assemble results
        for job in jobs:
            try:
                srcdir = self.getJobOutputDir(job['a_JobID'])
            except NoJobDirectoryError:
                srcdir = None
            results.arcIDs.append(job['a_id'])
            results.clientIDs.append(int(job['c_id']))
            results.jobdicts.append({
                'id': job['c_id'],
                'name': job['c_jobname'],
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
        if state_filter not in ('', 'submitted', 'running', 'tosubmit', 'submitting'):
            return []
        where = ' c.proxyid = %s AND '
        where_params = [proxyid]
        if state_filter:
            where += " a.arcstate = %s AND "
            where_params.append(state_filter)
        else:
            # otherwise, get all jobs that can be "getted"
            where += " (a.arcstate IN ( 'submitted', 'running', '', 'tosubmit', 'submitting' ) OR a.arcstate IS NULL) AND "
        where, where_params = self._addNameFilter(name_filter, where, where_params)
        where, where_params = self._addIDFilter(jobids, where, where_params)
        where = where.rstrip('AND ')

        # This lock is for race with client2arc.
        # Locking could otherwise be handled more systematically, elegantly and
        # granularly for the entire aCT.
        res = self.arcdb.db.getMutexLock('nulljobs', timeout=10)
        if not res:
            raise Exception("Could not lock null jobs")

        res = self.arcdb.db.getMutexLock('arcjobs', timeout=10)
        if not res:
            raise Exception("Could not lock table for killing jobs")

        jobs = self.clidb.getLeftJoinJobsInfo(
                clicols=['id'],
                arccols=['id', 'arcstate'],
                where=where,
                where_params=where_params)

        if not jobs:
            return []

        arc_ids = []
        client_ids = []
        for job in jobs:
            if job['a_id'] == None:
                # If id from arcjobs is NULL, then job is either waiting or in
                # inconsistent state. The job has to be deleted.
                client_ids.append(int(job['c_id']))
            elif job['a_arcstate'] == 'tosubmit':
                # 'tosubmit' jobs cannot be set to tocancel, they have to be deleted
                # immediately.
                client_ids.append(int(job['c_id']))
                self.arcdb.deleteArcJob(job['a_id'])
            else:
                # If there is entry in arcjobs, the job can be killed by
                # setting its state to 'tocancel'
                arc_ids.append(int(job['a_id']))

        if arc_ids:
            arc_where = f' id IN ({self._createMysqlIntList(arc_ids)})'

            patch = {'arcstate': 'tocancel', 'tarcstate': self.arcdb.getTimeStamp()}
            self.arcdb.updateArcJobs(patch, arc_where)
        if client_ids:
            client_where = f' id IN ({createMysqlEscapeList(len(client_ids))})'
            self.clidb.deleteJobs(client_where, client_ids)

        res = self.arcdb.db.releaseMutexLock('arcjobs')
        if not res:
            raise Exception("Could not release lock after killing jobs")

        res = self.arcdb.db.releaseMutexLock('nulljobs')
        if not res:
            raise Exception("Could not release lock for null jobs")

        return jobs

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
        where = " (a.arcstate = 'failed' OR a.arcstate = 'donefailed') AND c.proxyid = %s AND "
        where_params = [proxyid]
        where, where_params = self._addNameFilter(name_filter, where, where_params)
        where, where_params = self._addIDFilter(jobids, where, where_params)
        where = where.rstrip('AND ')

        if where:
            jobs = self.clidb.getJoinJobsInfo(
                    clicols=['id'],
                    arccols=['id'],
                    where=where,
                    where_params=where_params)

        if not jobs:
            return []

        # set job state for resubmittion
        where = f' id IN ({self._createMysqlIntList([job["a_id"] for job in jobs])})'

        patch = {'arcstate': 'toresubmit', 'tarcstate': self.arcdb.getTimeStamp()}
        self.arcdb.updateArcJobs(patch, where)

        return [job['c_id'] for job in jobs]

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
        # create query with filters
        where = ' c.proxyid = %s AND '
        where_params = [proxyid]
        if state_filter:
            where += " a.arcstate = %s AND "
            where_params.append(state_filter)
        if jobname:
            where += " c.jobname = %s AND "
            where_params.append(jobname)
        elif name_filter:
            where, where_params = self._addNameFilter(name_filter, where, where_params)
        where, where_params = self._addIDFilter(jobids, where, where_params)
        where = where.rstrip('AND ')

        # state filter condition is for right table which might turn
        # left join into inner join?
        if state_filter:
            jobs = self.clidb.getJoinJobsInfo(
                    clicols=clicols,
                    arccols=arccols,
                    where=where,
                    where_params=where_params)
        else:
            jobs = self.clidb.getLeftJoinJobsInfo(
                    clicols=clicols,
                    arccols=arccols,
                    where=where,
                    where_params=where_params)

        return jobs

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
            self.logger.error('tmp directory is not in config')
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
    if not arc.JobDescription_Parse(str(jobdesc), jobdescs):
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
    except Exception:
        logger.exception('Problem reading configuration')
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


