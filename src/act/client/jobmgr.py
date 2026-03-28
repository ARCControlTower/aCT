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
from act.client.clientdb import ClientDB
from act.client.errors import NoSuchProxyError, NoJobDirectoryError
from act.client.errors import ConfigError, InvalidJobDescriptionError
from act.client.errors import NoSuchSiteError, InvalidJobRangeError
from act.client.errors import InvalidJobIDError, UnknownClusterError
from act.client.common import readSites
from act.arc.dbModels import ArcJob
from act.client.dbModels import ClientJob
from urllib.parse import urlparse
from pyarcrest.arc import isLocalInputFile


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
        self.db: ClientDB = db

        # TODO: if and when sites from arc config are used, move everything
        # that uses arc config to this class
        self.arcconf = aCTConfigARC()
        self.appconf = aCTConfigAPP()
        self.clusters = self.parseClusters()

    def parseClusters(self):
        clusters = []
        for cluster in self.appconf.user.clusters:
            try:
                parts = urlparse(cluster, scheme="https")
            except Exception as exc:
                raise Exception(f"Error parsing cluster URL {cluster}: {exc}")

            scheme = parts.scheme
            host = parts.hostname
            port = parts.port
            path = parts.path

            if scheme != "https":
                raise Exception(f"Cluster URL {cluster} not using HTTPS")
            if host is None:
                raise Exception(f"Cluster URL {cluster} has no host")
            if port is None:
                port = 443

            clusters.append(f"https://{host}:{port}{path}")

        return clusters
    
    def checkClusters(self, clusterlist):
        clist = []
        for cluster in clusterlist:
            try:
                parts = urlparse(cluster, scheme="https")
            except Exception:
                raise UnknownClusterError(cluster)

            if parts.scheme != "https":
                raise UnknownClusterError(cluster)

            host = parts.hostname
            port = parts.port
            if port is None:
                port = 443

            url = f"https://{host}:{port}{parts.path}"

            if url not in self.clusters:
                raise UnknownClusterError(cluster)

            clist.append(url)
        return clist

    def cleanJobs(self, proxyid, jobids=[], state_filter=None, name_filter=None, **_):
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
        if state_filter not in (None, 'done', 'donefailed', 'cancelled', 'failed', 'lost'):
            return []
        if state_filter:
            state_filter = [state_filter]
        else:
            state_filter = ['done', 'donefailed', 'cancelled', 'failed', 'lost']

        with self.db.Session.begin() as session:
            jobs = self.db.getJoinJobsInfo(proxyid, session,
                                    jobids=jobids, state_filter=state_filter, name_filter=name_filter,
                                    clicols=['id'], arccols=['id', 'arcstate', 'JobID'])
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
                jobdir = self.getJobDataDir(c_id)
                shutil.rmtree(self.getJobDataDir(jobdir), ignore_errors=True)

                client_ids.append(c_id)
                arc_ids.append(a_id)

            if client_ids:
                self.db.updateArcstate(session=session, jobids=arc_ids, arcstate='toclean')
                self.db.deleteJobs(session=session, jobids=client_ids, table=ClientJob)

        return client_ids
    

    def fetchJobs(self, proxyid, jobids=[], name_filter='', **_):
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
        with self.db.Session.begin() as session:
            jobs = self.db.getJoinJobsInfo(proxyid, session, jobids=jobids,
                                    state_filter=['failed'], name_filter=name_filter,
                                    clicols=['id'], arccols=['id'])

            if not jobs:
                return []
            c_ids = [c_id for c_id, _ in jobs]
            a_ids = [a_id for _, a_id in jobs]

            self.db.updateArcstate(session=session, jobids=a_ids, arcstate='tofetch')
        return c_ids

    def getJobs(self, proxyid, jobids=None, state_filter=None, name_filter=None):
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
        # create query with filters
        with self.db.Session() as session:
            jobs = self.db.getJoinJobsInfo(proxyid, session, jobids=jobids, state_filter=state_filter, name_filter=name_filter, clicols=['id', 'jobname'], arccols=['id', 'JobID'])

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

    def killJobs(self, proxyid, jobids=None, state_filter=None, name_filter=None, **_):
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
        valid_states = (None, 'submitted', 'running', 'tosubmit', 'submitting')
        if state_filter not in valid_states:
            return []
        if state_filter:
            state_filter = [state_filter]

        with self.db.Session.begin() as session:
            jobs = self.db.getJoinJobsInfo(proxyid, session, jobids=jobids,
                                    state_filter=state_filter, name_filter=name_filter,
                                    clicols=['id'], arccols=['id', 'arcstate'], forupdate=True)

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
                    self.db.deleteJobs(session=session, jobids=[a_id], table=ArcJob)
                else:
                    # If there is entry in arcjobs, the job can be killed by
                    # setting its state to 'tocancel'
                    arc_ids.append(a_id)

            if arc_ids:
                self.db.updateArcstate(session=session, jobids=arc_ids, arcstate='tocancel')
            if client_ids:
                self.db.deleteJobs(session=session, jobids=client_ids, table=ClientJob)

        # One state in which a job can be killed is before it is passed
            # to ARC. Such jobs have None as arcid. Data dirs for jobs are
            # otherwise cleaned by cleaning operation but this is one exception
            # where killing destroys the job immediately and has to remove the
            # data dir as well.
        for c_id in client_ids:
            datadir = self.getJobDataDir(c_id)
            shutil.rmtree(self.getJobDataDir(datadir), ignore_errors=True)

        return [{"c_id": c, "a_id": a, "a_arcstate": s} for c, a, s in jobs]

    def resubmitJobs(self, proxyid, jobids=[], name_filter='', **_):
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

        with self.db.Session.begin() as session:
            jobs = self.db.getJoinJobsInfo(proxyid, session, jobids=jobids, 
                                    state_filter=['failed', 'donefailed'], name_filter=name_filter,
                                    clicols=['id'], arccols=['id'])

            if not jobs:
                return[]
            #set job state for resubmittion
            self.db.updateArcstate(session=session, jobids=[job.a_id for job in jobs], arcstate='toresubmit')

        return [job.c_id for job in jobs]

    def getJobStats(self, proxyid, jobids=None, state_filter=None, name_filter=None, clicols=[], arccols=[], jobname=None, **_):
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
        with self.db.Session() as session:
            result = self.db.getJoinJobsInfo(proxyid, session, jobids=jobids,
                                      state_filter=state_filter, name_filter=name_filter,
                                      clicols=clicols, arccols=arccols, jobname=jobname)

        jobs = [dict(row._mapping) for row in result]
        return jobs
    
    def createJobs(self, proxyid, jobs, errpref):
        results = []
        with self.db.Session.begin() as session:
            for job in jobs:
                result = {}
                results.append(result)

                try:
                    # check clusters
                    if 'clusterlist' not in job or not job['clusterlist']:
                        print(f'{errpref}No clusters given')
                        result['msg'] = 'No clusters given'
                        continue
                    clusterlist = self.checkClusters(job['clusterlist'])

                    # insert job
                    jobid = self.db.insertJob(proxyid=proxyid, session=session, clusterlist=','.join(clusterlist))
                except UnknownClusterError as e:
                    print(f'{errpref}Unknown cluster {e.name}')
                    result['msg'] = f'Unknown cluster {e.name}'
                    continue
                except Exception as e:
                    print(f'{errpref}{e}')
                    result['msg'] = 'Server error'
                    continue

                result['id'] = jobid
        return results

    
    def confirmJobs(self, proxyid, submissions, errpref):
        jobs = []
        jobids = []
        tocheck = []
        for submission in submissions:
            job = {}
            jobs.append(job)
            if not isinstance(submission, dict):
                print(f'{errpref}Job element is not an object: {submission}')
                job['msg'] = f'Job element is not an object: {submission}'
            elif 'id' not in submission:
                print(f'{errpref}No job ID given')
                job['msg'] = 'No job ID given'
            else:
                job.update(submission)
                jobids.append(job['id'])
                tocheck.append(job)

        # get info for all jobs and check which ones don't exist
        tosubmit = []
        with self.db.Session() as session:
            stats = self.db.checkClientJobs(proxyid, session=session, jobids=jobids)
        for job in tocheck:
            inStats = False
            for stat in stats:
                if stat.id == job['id']:
                    inStats = True
            if not inStats:
                print(f'{errpref}Job ID {job["id"]} does not exist')
                job['msg'] = f'Job ID {job["id"]} does not exist'
            else:
                tosubmit.append(job)

        jobdescs = arc.JobDescriptionList()

        with self.db.Session.begin() as session:
            for job in tosubmit:

                # parse job description
                if 'desc' not in job:
                    print(f'{errpref}No job description given')
                    job['msg'] = 'No job description given'
                    continue
                if not arc.JobDescription.Parse(job['desc'], jobdescs):
                    print(f'{errpref}Invalid job description')
                    job['msg'] = 'Invalid job description'
                    continue

                job['name'] = jobdescs[-1].Identification.JobName

                # get job's data directory
                try:
                    jobDataDir = self.getJobDataDir(job['id'])
                except ConfigError as e:
                    print(f'{errpref}{e}')
                    job['msg'] = 'Server error'
                    continue

                # modify job description for local input files
                #
                # InputFiles need to be accessed through index otherwise
                # the changes do not survive outside of for loop.
                for i in range(len(jobdescs[-1].DataStaging.InputFiles)):
                    filename = jobdescs[-1].DataStaging.InputFiles[i].Name
                    filepath = isLocalInputFile(
                        jobdescs[-1].DataStaging.InputFiles[i].Name,
                        jobdescs[-1].DataStaging.InputFiles[i].Sources[0].fullstr()
                    )
                    if not filepath:  # remote file
                        continue

                    path = os.path.abspath(os.path.join(jobDataDir, filename))
                    if not os.path.isfile(path):
                        job['msg'] = f'Input file {filepath} missing'
                        break

                    jobdescs[-1].DataStaging.InputFiles[i].Sources[0].ChangeFullPath(path)

                # errors on missing input files
                if 'msg' in job:
                    print(f'{errpref}{job["msg"]}')
                    continue

                # TODO: ADL unparsing works but it doesn't unparse modified
                # input files
                desc = jobdescs[-1].UnParse('nordugrid:xrsl')[1]
                #desc = jobdescs[0].UnParse('emies:adl')[1]
                if not arc.JobDescription.Parse(desc, jobdescs):
                    print(f'{errpref}Invalid modified job description')
                    job['msg'] = 'Server error'
                    continue

                # update job entry and confirm job for submission
                try:
                    self.db.updateJob(proxyid=proxyid, session=session, jobid=job['id'], values_dict={'jobdesc':desc, 'jobname':job['name']})
                except Exception as e:
                    print(f'{errpref}{e}')
                    job['msg'] = 'Server error'
                    continue

                del job['desc']  # don't want to return description in result

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


