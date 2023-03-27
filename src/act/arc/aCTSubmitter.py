import datetime
import os
from http.client import HTTPException
from json import JSONDecodeError
from random import shuffle
from ssl import SSLError
from urllib.parse import urlparse

from act.arc.aCTARCProcess import aCTARCProcess
from act.arc.aCTStatus import ARC_STATE_MAPPING
from act.common.aCTJob import ACTJob
from pyarcrest.arc import ARCRest
from pyarcrest.errors import (ARCError, ARCHTTPError, DescriptionParseError,
                              DescriptionUnparseError, InputFileError,
                              MatchmakingError, NoValueInARCResult)


class aCTSubmitter(aCTARCProcess):

    def setup(self):
        super().setup()
        # parse queue from cluster URL
        try:
            url = urlparse(self.cluster)
        except ValueError as exc:
            self.log.error(f"Error parsing cluster URL {url}: {exc}")
            raise
        self.queue = url.path.split("/")[-1]
        self.hostname = url.hostname
        self.port = url.port

    # TODO: refactor to some library aCT job operation
    def submit(self):
        """
        Submit a batch of jobs to the ARC cluster.

        Signal handling strategy:
        - termination condition is checked for every proxyid batch
        """
        clustermaxjobs = 999999
        # check for any site-specific limits or status
        for site, info in self.conf.sites:
            if info.endpoint == self.cluster:
                if info.status == 'offline':
                    self.log.info('Site status is offline')
                    return
                if isinstance(info.maxjobs, int):
                    clustermaxjobs = info.maxjobs

        nsubmitted = self.db.getNArcJobs(f"cluster='{self.cluster}'")
        if nsubmitted >= clustermaxjobs:
            self.log.info(f'{nsubmitted} submitted jobs is greater than or equal to max jobs {clustermaxjobs}')
            return

        # Apply fair-share
        if self.cluster:
            fairshares = self.db.getArcJobsInfo(f"arcstate='tosubmit' and clusterlist like '%{self.cluster}%'", ['fairshare', 'proxyid'])
        else:
            fairshares = self.db.getArcJobsInfo("arcstate='tosubmit' and clusterlist=''", ['fairshare', 'proxyid'])

        if not fairshares:
            self.log.info('Nothing to submit')
            return

        # split by proxy for GU queues
        fairshares = list(set([(p['fairshare'], p['proxyid']) for p in fairshares]))
        # For proxy bug - see below
        shuffle(fairshares)

        limit = min(clustermaxjobs - nsubmitted, 100)

        # Divide limit among fairshares, unless exiting after first loop due to
        # proxy bug, but make sure at least one job is submitted
        if len(self.db.getProxiesInfo('TRUE', ['id'])) == 1:
            limit = max(limit // len(fairshares), 1)

        for fairshare, proxyid in fairshares:

            if self.mustExit:
                self.log.info(f"Exiting early due to requested shutdown")
                self.stopWithException()

            # Exit loop if above limit
            if nsubmitted >= clustermaxjobs:
                self.log.info(f"CE is at limit of {clustermaxjobs} submitted jobs, exiting")
                break

            try:
                # catch any exceptions here to avoid leaving lock
                if self.cluster:
                    # Lock row for update in case multiple clusters are specified
                    #jobs=self.db.getArcJobsInfo("arcstate='tosubmit' and ( clusterlist like '%{0}' or clusterlist like '%{0},%' ) and fairshare='{1}' order by priority desc limit 10".format(self.cluster, fairshare),
                    jobs = self.db.getArcJobsInfo(
                        "arcstate='tosubmit' and ( clusterlist like '%{0}' or clusterlist like '%{0},%' ) and fairshare='{1}' and proxyid='{2}' limit {3}".format(self.cluster, fairshare, proxyid, limit),
                        columns=["id", "jobdesc", "appjobid", "priority", "proxyid", "clusterlist"], lock=True
                    )
                    if jobs:
                        self.log.debug(f"started lock for writing {len(jobs)} jobs")
                else:
                    jobs = self.db.getArcJobsInfo(
                        "arcstate='tosubmit' and clusterlist='' and fairshare='{0} and proxyid={1}' limit {2}".format(fairshare, proxyid, limit),
                        columns=["id", "jobdesc", "appjobid", "priority", "proxyid", "clusterlist"]
                    )
                # mark submitting in db
                jobs_taken = []
                for j in jobs:
                    jd = {'cluster': self.cluster, 'arcstate': 'submitting', 'tarcstate': self.db.getTimeStamp()}
                    self.db.updateArcJob(j['id'], jd)
                    jobs_taken.append(j)
                jobs = jobs_taken
            finally:
                try:
                    self.db.Commit(lock=True)
                    self.log.debug("ended lock")
                except:
                    self.log.warning("Failed to release DB lock")

            if len(jobs) == 0:
                #self.log.debug("No jobs to submit")
                continue

            self.log.info(f"Submitting {len(jobs)} jobs for fairshare {fairshare} and proxyid {proxyid}")

            ## Set UserConfig credential for querying infosys
            #proxystring = str(self.db.getProxy(proxyid))
            #self.uc.CredentialString(proxystring)
            #global usercred
            #usercred = self.uc

            # Filter only sites for this process
            qjobs = self.db.getArcJobsInfo(f"cluster='{self.cluster}' and  arcstate='submitted' and fairshare='{fairshare}'", ['id','priority'])
            rjobs = self.db.getArcJobsInfo(f"cluster='{self.cluster}' and  arcstate='running' and fairshare='{fairshare}'", ['id'])

            # max waiting priority
            try:
                maxpriowaiting = max(jobs, key=lambda x: x['priority'])['priority']
            except:
                maxpriowaiting = 0
            self.log.info(f"Maximum priority of waiting jobs: {maxpriowaiting}")


            # max queued priority
            try:
                maxprioqueued = max(qjobs, key=lambda x: x['priority'])['priority']
            except:
                maxprioqueued = 0
            self.log.info(f"Max priority queued: {maxprioqueued}")

            #qfraction = self.conf.jobs.get("queuefraction", 0.15) / 100.0
            #qoffset = self.conf.jobs.get("queueoffset", 100)

            ##################################################################
            #
            # New REST submission code
            #
            ##################################################################

            # read job descriptions from DB
            actjobs = []
            arcjobs = []
            for dbjob in jobs:
                job = ACTJob()
                job.loadARCDBJob(dbjob)
                job.arcjob.descstr = str(self.db.getArcJobDescription(str(dbjob["jobdesc"])))

                actjobs.append(job)
                arcjobs.append(job.arcjob)

            proxypath = os.path.join(self.db.proxydir, f"proxiesid{proxyid}")

            # submit jobs to ARC
            try:
                arcrest = ARCRest.getClient(self.cluster, proxypath=proxypath, logger=self.log, version="1.0")
                arcrest.submitJobs(self.queue, arcjobs, uploadData=False)
            except JSONDecodeError as exc:
                self.log.error(f"Invalid JSON response from ARC: {exc}")
                self.setJobsArcstate(jobs, "tosubmit")
                continue
            except MatchmakingError as exc:
                self.log.error(str(exc))
                self.setJobsArcstate(jobs, "cancelled")
                continue
            except (HTTPException, ConnectionError, SSLError, ARCError, ARCHTTPError, TimeoutError, OSError, ValueError) as exc:
                self.log.error(f"Error submitting jobs to ARC: {exc}")
                self.setJobsArcstate(jobs, "tosubmit")
                continue
            else:
                alreadySubmitted = True

            # upload jobs' local input files
            arcrest.uploadJobFiles([job for job in arcjobs if job.state == "ACCEPTING"])

            # log submission results and set job state
            for job in actjobs:
                if not job.arcjob.errors:
                    job.state = "submitted"
                    self.log.debug(f"Submission successfull for job {job.appid} {job.arcid} {job.arcjob.name}: {job.arcjob.id}")
                else:
                    for error in job.arcjob.errors:
                        if type(error) in (InputFileError, DescriptionParseError, DescriptionUnparseError, MatchmakingError):
                            job.state = "cancelled"
                        else:
                            job.state = "tosubmit"
                        self.log.debug(f"Error submitting job {job.appid} {job.arcid} {job.arcjob.name}: {error}")

            tstamp = self.db.getTimeStamp()

            # update job records in DB
            for job in actjobs:
                jobdict = {}
                jobdict["arcstate"] = job.state
                jobdict["tarcstate"] = tstamp
                jobdict["tstate"] = tstamp
                jobdict["cluster"] = self.cluster
                jobdict["ExecutionNode"] = ""
                jobdict["UsedTotalWallTime"] = 0
                jobdict["UsedTotalCPUTime"] = 0
                jobdict["RequestedTotalWallTime"] = 0
                jobdict["RequestedTotalCPUTime"] = 0
                jobdict["RequestedSlots"] = -1
                jobdict["Error"] = ""
                if job.arcjob.delegid:
                    jobdict["DelegationID"] = job.arcjob.delegid
                if job.arcjob.id:
                    jobdict["IDFromEndpoint"] = job.arcjob.id
                    host = self.hostname
                    if self.port is not None:
                        host = f"{host}:{self.port}"
                    path = arcrest.getAPIPath()
                    jobdict["JobID"] = f"https://{host}{path}/jobs/{job.arcjob.id}"
                if job.arcjob.state:
                    jobdict["State"] = ARC_STATE_MAPPING[job.arcjob.state]

                self.db.updateArcJob(job.arcid, jobdict)

            nsubmitted += limit

        self.log.info("Done")

    def setJobsArcstate(self, jobs, arcstate):
        self.log.debug(f"Setting arcstate of jobs to {arcstate}")
        tstamp = self.db.getTimeStamp()
        for job in jobs:
            updateDict = {"arcstate": arcstate, "tarcstate": tstamp}
            self.db.updateArcJob(job["id"], updateDict)

    # TODO: refactor to some library aCT job operation
    def checkFailedSubmissions(self):
        """
        Cancel jobs that are too long in submitting.

        Signal handling strategy:
        - termination is checked before handling every job
        """
        dbjobs = self.db.getArcJobsInfo(f"arcstate='tosubmit' and cluster='{self.cluster}'", ["id", "appjobid", "created"])
        for dbjob in dbjobs:
            if self.mustExit:
                self.log.info(f"Exiting early due to requested shutdown")
                self.stopWithException()
            job = ACTJob()
            job.loadARCDBJob(dbjob)
            # TODO: HARDCODED
            if job.tcreated + datetime.timedelta(hours=1) < datetime.datetime.utcnow():
                self.log.debug(f"Cancelling job {job.appid} {job.arcid} for being too long in tosubmit")
                self.db.updateArcJob(job.arcid, {"arcstate": "tocancel", "tarcstate": self.db.getTimeStamp()})

    # TODO: refactor to some library aCT job operation
    def processToCancel(self):
        """
        Cancel jobs in ARC.

        Signal handling strategy:
        - termination is checked before handling every proxyid job batch
        """
        COLUMNS = ["id", "appjobid", "proxyid", "IDFromEndpoint"]

        # fetch jobs from DB for this cluster and also jobs that
        # don't have cluster assigned
        jobstocancel = self.db.getArcJobsInfo(
            f"arcstate='tocancel' and cluster='{self.cluster}'",
            COLUMNS
        )
        if not jobstocancel:
            return

        self.log.info(f"Cancelling {len(jobstocancel)} jobs")

        # aggregate jobs by proxyid
        jobsdict = {}
        for row in jobstocancel:
            if not row["proxyid"] in jobsdict:
                jobsdict[row["proxyid"]] = []
            jobsdict[row["proxyid"]].append(row)

        for proxyid, dbjobs in jobsdict.items():

            if self.mustExit:
                self.log.info(f"Exiting early due to requested shutdown")
                self.stopWithException()

            # create jobdicts for kill operation
            tokill = []  # all jobs to be killed
            toARCKill = []  # jobs to be killed in ARC
            for dbjob in dbjobs:
                job = ACTJob()
                job.loadARCDBJob(dbjob)
                tokill.append(job)
                if job.arcjob.id:
                    toARCKill.append(job.arcjob)

            proxypath = os.path.join(self.db.proxydir, f"proxiesid{proxyid}")

            arcrest = None
            try:
                arcrest = ARCRest.getClient(self.cluster, proxypath=proxypath, logger=self.log, version="1.0")
                arcrest.killJobs(toARCKill)
            except JSONDecodeError as exc:
                self.log.error(f"Invalid JSON response from ARC: {exc}")
            except (HTTPException, ConnectionError, SSLError, ARCError, ARCHTTPError, TimeoutError, OSError, ValueError) as exc:
                self.log.error(f"Error killing jobs in ARC: {exc}")
            finally:
                if arcrest:
                    arcrest.close()

            tstamp = self.db.getTimeStamp()

            # log results and update DB
            for job in tokill:
                state = None
                if not job.arcjob.errors:
                    if job.arcjob.id:
                        state = "cancelling"
                        self.log.debug(f"ARC will cancel job {job.appid} {job.arcid}")
                    else:
                        state = "cancelled"
                        self.log.debug(f"Job {job.appid} {job.arcid} cancelled")
                else:
                    for error in job.arcjob.errors:
                        if isinstance(error, ARCHTTPError):
                            if error.status == 404:
                                state = "cancelled"
                                self.log.error(f"Job {job.appid} {job.arcid} not found, setting to cancelled")
                                continue
                        self.log.error(f"Error killing job {job.appid} {job.arcid}: {error}")

                if state:
                    self.db.updateArcJob(
                        job.arcid,
                        {"arcstate": state, "tarcstate": tstamp}
                    )

    # TODO: refactor to some library aCT job operation
    def processToResubmit(self):
        """
        Resubmit jobs to ARC.

        Resubmission requires cleaning of existing ARC jobs and then setting
        the jobs to tosubmit to be submitted normally again.

        Signal handling strategy:
        - termination is checked before handling every proxyid job batch
        """
        COLUMNS = ["id", "appjobid", "proxyid", "IDFromEndpoint"]

        # fetch jobs from DB
        jobstoresubmit = self.db.getArcJobsInfo(
            f"arcstate='toresubmit' and cluster='{self.cluster}'",
            COLUMNS
        )
        if not jobstoresubmit:
            return

        self.log.info(f"Resubmitting {len(jobstoresubmit)} jobs")

        # aggregate jobs by proxyid
        jobsdict = {}
        for row in jobstoresubmit:
            if not row["proxyid"] in jobsdict:
                jobsdict[row["proxyid"]] = []
            jobsdict[row["proxyid"]].append(row)

        for proxyid, dbjobs in jobsdict.items():

            if self.mustExit:
                self.log.info(f"Exiting early due to requested shutdown")
                self.stopWithException()

            # create job dicts for clean operation
            toresubmit = []
            toARCClean = []
            for dbjob in dbjobs:
                job = ACTJob()
                job.loadARCDBJob(dbjob)
                toresubmit.append(job)
                if job.arcjob.id:
                    toARCClean.append(job.arcjob)

            proxypath = os.path.join(self.db.proxydir, f"proxiesid{proxyid}")

            # clean jobs from ARC
            arcrest = None
            try:
                arcrest = ARCRest.getClient(self.cluster, proxypath=proxypath, logger=self.log, version="1.0")
                arcrest.cleanJobs(toARCClean)
            except JSONDecodeError as exc:
                self.log.error(f"Invalid JSON response from ARC: {exc}")
            except (HTTPException, ConnectionError, SSLError, ARCError, ARCHTTPError, TimeoutError, OSError, ValueError) as exc:
                self.log.error(f"Error killing jobs in ARC: {exc}")
            finally:
                if arcrest:
                    arcrest.close()

            # log results
            for job in toresubmit:
                if not job.arcjob.errors:
                    if job.arcjob.id:
                        self.log.debug(f"Successfully cleaned job {job.appid} {job.arcid}")
                else:
                    for error in job.arcjob.errors:
                        self.log.error(f"Error cleaning job {job.appid} {job.arcid}: {error}")

            # update DB
            for job in toresubmit:
                tstamp = self.db.getTimeStamp()
                # "created" needs to be reset so that it doesn't get understood
                # as failing to submit since first insertion.
                jobdict = {"arcstate": "tosubmit", "tarcstate": tstamp, "created": tstamp}
                self.db.updateArcJob(job.arcid, jobdict)

    # TODO: refactor to some library aCT job operation
    def processToRerun(self):
        """
        Rerun jobs in ARC.

        Signal handling strategy:
        - termination is checked before handling every proxyid job batch
        """
        COLUMNS = ["id", "appjobid", "proxyid", "IDFromEndpoint"]

        # fetch jobs from DB
        jobstorerun = self.db.getArcJobsInfo(
            f"arcstate='torerun' and cluster='{self.cluster}'",
            COLUMNS
        )
        if not jobstorerun:
            return

        self.log.info(f"Resuming {len(jobstorerun)} jobs")

        # aggregate jobs by proxyid
        jobsdict = {}
        for row in jobstorerun:
            if not row["proxyid"] in jobsdict:
                jobsdict[row["proxyid"]] = []
            jobsdict[row["proxyid"]].append(row)

        for proxyid, dbjobs in jobsdict.items():

            if self.mustExit:
                self.log.info(f"Exiting early due to requested shutdown")
                self.stopWithException()

            # create a list of jobdicts to be rerun
            torerun = []
            for dbjob in dbjobs:
                job = ACTJob()
                job.loadARCDBJob(dbjob)
                torerun.append(job)

            proxypath = os.path.join(self.db.proxydir, f"proxiesid{proxyid}")

            arcrest = None
            try:
                arcrest = ARCRest.getClient(self.cluster, proxypath=proxypath, logger=self.log, version="1.0")

                # get delegations for jobs
                # AF BUG
                try:
                    arcrest.getJobsDelegations([job.arcjob for job in torerun], self.log)
                except:
                    self.log.error("GET JOBS DELEGATIONS EXCEPTION")
                    import traceback
                    self.log.debug(traceback.format_exc())

                # renew delegations
                torestart = []
                for job in torerun:
                    if not job.arcjob.errors:
                        try:
                            arcrest.renewDelegation(job.arcjob.delegid)
                        except Exception as exc:
                            job.arcjob.errors.append(exc)
                        else:
                            torestart.append(job.arcjob)

                # restart jobs
                arcrest.restartJobs(torestart)

            except JSONDecodeError as exc:
                self.log.error(f"Invalid JSON response from ARC: {exc}")
                continue
            except (HTTPException, ConnectionError, SSLError, ARCError, ARCHTTPError, TimeoutError, OSError, ValueError) as exc:
                self.log.error(f"Error rerunning jobs in ARC: {exc}")
                continue
            finally:
                if arcrest:
                    arcrest.close()

            tstamp = self.db.getTimeStamp()

            # log results and update DB
            for job in torerun:
                if job.arcjob.errors:
                    cannotRerun = False
                    for error in job.arcjob.errors:
                        if isinstance(error, ARCHTTPError):
                            if error.status == 505 and error.text == "No more restarts allowed":
                                self.log.error(f"Restart of job {job.appid} {job.arcid} not allowed, setting to failed")
                                self.db.updateArcJob(job.arcid, {"arcstate": "failed", "State": "Failed", "tarcstate": tstamp, "tstate": tstamp})
                                cannotRerun = True
                            elif error.status == 505 and error.text == "Job has not failed":
                                self.log.error(f"Job {job.appid} {job.arcid} has not failed, setting to submitted")
                                self.db.updateArcJob(job.arcid, {"arcstate": "submitted", "tarcstate": tstamp})
                                cannotRerun = True
                            elif error.status == 404:
                                self.log.error(f"Job {job.appid} {job.arcid} not found, cancelling")
                                self.db.updateArcJob(job.arcid, {"arcstate": "tocancel", "tarcstate": tstamp})
                                cannotRerun = True
                            else:
                                # TODO: is just using error.__str__() good enough?
                                self.log.error(f"Error rerunning job {job.appid} {job.arcid}: {error.status} {error.text}")
                        elif isinstance(error, NoValueInARCResult):
                            self.log.error(f"Error rerunning job {job.appid} {job.arcid}: {error}")
                            self.db.updateArcJob(job.arcid, {"arcstate": "tocancel", "tarcstate": tstamp})
                            cannotRerun = True
                        else:
                            self.log.error(f"Error rerunning job {job.appid} {job.arcid}: {error}")
                    if not cannotRerun:
                        self.db.updateArcJob(job.arcid, {"arcstate": "torerun", "tarcstate": tstamp})
                else:
                    self.log.info(f"Successfully rerun job {job.appid} {job.arcid}")
                    self.db.updateArcJob(job.arcid, {"arcstate": "submitted", "tarcstate": tstamp})

    def process(self):
        # process jobs which have to be cancelled
        self.processToCancel()
        # process jobs which have to be resubmitted
        self.processToResubmit()
        # process jobs which have to be rerun
        self.processToRerun()
        # submit new jobs
        self.submit()
        # check jobs which failed to submit
        self.checkFailedSubmissions()
