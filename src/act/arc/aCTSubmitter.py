import datetime
from json import JSONDecodeError
from random import shuffle
from urllib.parse import urlparse

from act.arc.aCTARCProcess import aCTARCProcess
from act.arc.aCTStatus import ARC_STATE_MAPPING
from pyarcrest.errors import (ARCError, ARCHTTPError, DescriptionParseError,
                              DescriptionUnparseError, InputFileError,
                              InputUploadError, MatchmakingError,
                              NoValueInARCResult)


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
            self.stopOnFlag()

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
                tstamp = self.db.getTimeStamp()
                jobs_taken = []
                for j in jobs:
                    jd = {'cluster': self.cluster, 'arcstate': 'submitting', 'tarcstate': tstamp}
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
            descs = []
            for job in jobs:
                descs.append(str(self.db.getArcJobDescription(str(job["jobdesc"]))))

            # get REST client
            arcrest = self.getARCClient(proxyid)
            if not arcrest:
                self.setJobsArcstate(jobs, "tosubmit")
                continue

            # submit jobs to ARC
            try:
                delegationID = arcrest.createDelegation()
                results = arcrest.submitJobs(
                    descs,
                    self.queue,
                    delegationID,
                    workers=self.conf.rest.upload_workers or 10,
                    sendsize=self.conf.rest.upload_size or 8388608,  # 8MB
                    timeout=self.conf.rest.timeout or 60,
                )
            except JSONDecodeError as exc:
                self.setJobsArcstate(jobs, "tosubmit")
                self.log.error(f"Invalid JSON response from ARC: {exc}")
                continue
            except MatchmakingError as exc:
                self.setJobsArcstate(jobs, "cancelled")
                self.log.error(str(exc))
                continue
            except Exception as exc:
                self.setJobsArcstate(jobs, "tosubmit")
                self.log.error(f"Error submitting jobs to ARC: {exc}")
                continue
            finally:
                arcrest.close()

            tstamp = self.db.getTimeStamp()

            # log submission results and set job state
            for job, result in zip(jobs, results):
                jobdict = {}
                if isinstance(result, ARCError):
                    if type(result) in (InputFileError, DescriptionParseError, DescriptionUnparseError, MatchmakingError):
                        jobdict["arcstate"] = "cancelled"
                        self.log.error(f"Error submitting appjob({job['appjobid']}): {result}")
                    elif isinstance(result, InputUploadError):
                        jobdict["arcstate"] = "tocancel"
                        jobdict["cluster"] = self.cluster
                        jobdict["IDFromEndpoint"] = result.jobid
                        for exc in result.errors:
                            self.log.error(f"Error uploading input files for appjob({job['appjobid']}): {exc}")
                        self.log.info(f"Cancelling appjob({job['appjobid']}) due to upload errors")
                    else:
                        jobdict["arcstate"] = "tosubmit"
                        self.log.error(f"Error submitting appjob({job['appjobid']}): {result}")
                else:
                    jobid, state = result
                    jobdict["arcstate"] = "submitted"
                    jobdict["tstate"] = tstamp
                    jobdict["ExecutionNode"] = ""
                    jobdict["UsedTotalWallTime"] = 0
                    jobdict["UsedTotalCPUTime"] = 0
                    jobdict["RequestedTotalWallTime"] = 0
                    jobdict["RequestedTotalCPUTime"] = 0
                    jobdict["RequestedSlots"] = -1
                    jobdict["Error"] = ""
                    jobdict["DelegationID"] = delegationID
                    jobdict["IDFromEndpoint"] = jobid
                    host = self.hostname
                    if self.port is not None:
                        host = f"{host}:{self.port}"
                    path = arcrest.apiPath
                    jobdict["JobID"] = f"https://{host}{path}/jobs/{jobid}"
                    jobdict["State"] = ARC_STATE_MAPPING[state]
                    self.log.info(f"Submission successfull for appjob({job['appjobid']}): {jobid}")

                jobdict["tarcstate"] = tstamp
                self.db.updateArcJob(job["id"], jobdict)

            nsubmitted += limit

        self.log.info("Done")

    def setJobsArcstate(self, jobs, arcstate):
        self.log.info(f"Setting arcstate of jobs to {arcstate}")
        tstamp = self.db.getTimeStamp()
        for job in jobs:
            updateDict = {"arcstate": arcstate, "tarcstate": tstamp}
            self.db.updateArcJob(job["id"], updateDict)

    def checkFailedSubmissions(self):
        """
        Cancel jobs that are too long in submitting.

        Signal handling strategy:
        - termination is checked before handling every job
        """
        dbjobs = self.db.getArcJobsInfo(f"arcstate='tosubmit' and cluster='{self.cluster}'", ["id", "appjobid", "created"])
        tstamp = self.db.getTimeStamp()
        for job in dbjobs:
            self.stopOnFlag()
            # TODO: HARDCODED
            if job["created"] + datetime.timedelta(hours=1) < datetime.datetime.utcnow():
                self.db.updateArcJob(job["id"], {"arcstate": "tocancel", "tarcstate": tstamp})
                self.log.warning(f"Cancelling appjob({job['appjobid']}) for being too long in tosubmit")

    def processToCancel(self):
        """
        Cancel jobs in ARC.

        Signal handling strategy:
        - termination is checked before handling every proxyid job batch
        """
        COLUMNS = ["id", "appjobid", "proxyid", "IDFromEndpoint", "tarcstate"]
        jobstocancel = self.db.getArcJobsInfo(
            f"arcstate='tocancel' and cluster='{self.cluster}'",
            COLUMNS
        )
        if not jobstocancel:
            return

        # make jobs that are taking too long cancelled
        now = datetime.datetime.utcnow()
        tstamp = self.db.getTimeStamp()
        # TODO: HARDCODED
        limit = datetime.timedelta(hours=1)
        tocancel = []
        for job in jobstocancel:
            if job["tarcstate"] + limit < now:
                self.db.updateArcJob(job["id"], {"arcstate": "cancelled", "tarcstate": tstamp})
                self.log.warning(f"Could not cancel appjob({job['appjobid']}) in time, setting to cancelled")
            else:
                tocancel.append(job)

        self.log.info(f"Cancelling {len(tocancel)} jobs")

        # aggregate jobs by proxyid
        jobsdict = {}
        for row in tocancel:
            if not row["proxyid"] in jobsdict:
                jobsdict[row["proxyid"]] = []
            jobsdict[row["proxyid"]].append(row)

        for proxyid, dbjobs in jobsdict.items():
            self.stopOnFlag()

            # partition the jobs based on whether they are in ARC; ARC jobs
            # need to be killed in ARC first, others can be set to cancelled
            # directly
            toARCKill = []
            arcids = []
            cancelled = []
            for dbjob in dbjobs:
                if dbjob.get("IDFromEndpoint", None):
                    toARCKill.append(dbjob)
                    arcids.append(dbjob["IDFromEndpoint"])
                else:
                    cancelled.append(dbjob)

            # get REST client
            arcrest = self.getARCClient(proxyid)
            if not arcrest:
                continue

            # kill jobs in ARC
            try:
                results = arcrest.killJobs(arcids)
            except JSONDecodeError as exc:
                self.log.error(f"Invalid JSON response from ARC: {exc}")
                continue
            except Exception as exc:
                self.log.error(f"Error killing jobs in ARC: {exc}")
                continue
            finally:
                arcrest.close()

            tstamp = self.db.getTimeStamp()

            # log ARC results and update DB
            for job, result in zip(toARCKill, results):
                if isinstance(result, ARCHTTPError):
                    state = "cancelled"
                    if result.status == 404:
                        self.log.warning(f"appjob({job['appjobid']}) missing in ARC, setting to cancelled")
                    else:
                        self.log.error(f"Error killing appjob({job['appjobid']}): {result.status} {result.text}")
                else:
                    state = "cancelling"
                    self.log.info(f"ARC will cancel appjob({job['appjobid']})")
                self.db.updateArcJob(job["id"], {"arcstate": state, "tarcstate": tstamp})

            # update DB for jobs not in ARC
            for job in cancelled:
                self.db.updateArcJob(job["id"], {"arcstate": "cancelled", "tarcstate": tstamp})
                self.log.info(f"appjob({job['appjobid']}) not in ARC, setting to cancelled directly")

    def processToResubmit(self):
        """
        Resubmit jobs to ARC.

        Resubmission requires cleaning of existing ARC jobs and then setting
        the jobs to tosubmit to be submitted normally again.

        Signal handling strategy:
        - termination is checked before handling every proxyid job batch
        """
        COLUMNS = ["id", "appjobid", "proxyid", "IDFromEndpoint", "tarcstate"]

        # fetch jobs from DB
        jobstoresubmit = self.db.getArcJobsInfo(
            f"arcstate='toresubmit' and cluster='{self.cluster}'",
            COLUMNS
        )
        if not jobstoresubmit:
            return

        # fail jobs that are taking too long
        now = datetime.datetime.utcnow()
        tstamp = self.db.getTimeStamp()
        # TODO: HARDCODED
        limit = datetime.timedelta(hours=1)
        toresubmit = []
        for job in jobstoresubmit:
            if job["tarcstate"] + limit < now:
                self.db.updateArcJob(job["id"], {"arcstate": "failed", "tarcstate": tstamp, "attemptsleft": 0})
                self.log.warning(f"Could not resubmit appjob({job['appjobid']}) in time, setting to failed")
            else:
                toresubmit.append(job)

        self.log.info(f"Resubmitting {len(toresubmit)} jobs")

        # aggregate jobs by proxyid
        jobsdict = {}
        for row in toresubmit:
            if not row["proxyid"] in jobsdict:
                jobsdict[row["proxyid"]] = []
            jobsdict[row["proxyid"]].append(row)

        for proxyid, dbjobs in jobsdict.items():
            self.stopOnFlag()

            # create a list of jobs that need to be cleaned in ARC
            toARCClean = []
            arcids = []
            for dbjob in dbjobs:
                if dbjob.get("IDFromEndpoint", None):
                    toARCClean.append(dbjob)
                    arcids.append(dbjob["IDFromEndpoint"])

            # get REST client
            arcrest = self.getARCClient(proxyid)
            if not arcrest:
                continue

            # clean jobs from ARC
            try:
                results = arcrest.cleanJobs(arcids)
            except JSONDecodeError as exc:
                self.log.error(f"Invalid JSON response from ARC: {exc}")
                continue
            except Exception as exc:
                self.log.error(f"Error cleaning jobs to resubmit in ARC: {exc}")
                continue
            finally:
                arcrest.close()

            # log results
            for job, result in zip(toARCClean, results):
                if isinstance(result, ARCHTTPError):
                    self.log.error(f"Error cleaning appjob({job['appjobid']}): {result.status} {result.text}")
                else:
                    self.log.info(f"Successfully cleaned appjob({job['appjobid']})")

            tstamp = self.db.getTimeStamp()

            # set jobs for resubmission in DB
            for job in dbjobs:
                # "created" needs to be reset so that it doesn't get understood
                # as failing to submit since first insertion.
                jobdict = {"arcstate": "tosubmit", "tarcstate": tstamp, "created": tstamp}
                self.db.updateArcJob(job["id"], jobdict)

    def processToRerun(self):
        """
        Rerun jobs in ARC.

        Signal handling strategy:
        - termination is checked before handling every proxyid job batch
        """
        COLUMNS = ["id", "appjobid", "proxyid", "IDFromEndpoint", "tarcstate"]

        # fetch jobs from DB
        jobstorerun = self.db.getArcJobsInfo(
            f"arcstate='torerun' and cluster='{self.cluster}'",
            COLUMNS
        )
        if not jobstorerun:
            return

        # fail jobs that are taking too long
        now = datetime.datetime.utcnow()
        tstamp = self.db.getTimeStamp()
        # TODO: HARDCODED
        limit = datetime.timedelta(hours=1)
        torerun = []
        for job in jobstorerun:
            if job["tarcstate"] + limit < now:
                self.db.updateArcJob(job["id"], {"arcstate": "failed", "tarcstate": tstamp})
                self.log.warning(f"Could not restart appjob({job['appjobid']}) in time, setting to failed")
            else:
                torerun.append(job)

        self.log.info(f"Resuming {len(torerun)} jobs")

        # aggregate jobs by proxyid
        jobsdict = {}
        for row in torerun:
            if not row["proxyid"] in jobsdict:
                jobsdict[row["proxyid"]] = []
            jobsdict[row["proxyid"]].append(row)

        for proxyid, dbjobs in jobsdict.items():
            self.stopOnFlag()

            # get REST client
            arcrest = self.getARCClient(proxyid)
            if not arcrest:
                continue

            # get job delegations
            arcids = [job["IDFromEndpoint"] for job in dbjobs]
            try:
                results = arcrest.getJobsDelegations(arcids)
            except Exception as exc:
                self.log.error(f"Error getting delegations for jobs: {exc}")
                arcrest.close()
                continue

            # renew successfully fetched delegations
            torestart = []
            arcids = []
            renewed = set()  # performance and duplicate prevention
            for job, result in zip(dbjobs, results):
                if isinstance(result, ARCHTTPError):
                    self.log.error(f"Error getting delegations for appjob({job['appjobid']}): {result.status} {result.text}")
                elif isinstance(result, NoValueInARCResult):
                    self.log.error(f"NO VALUE IN SUCCESSFUL FETCH OF DELEGATIONS FOR appjob({job['appjobid']})")
                else:
                    try:
                        # renewing the first delegation from the list works
                        # for aCT use case
                        if result[0] not in renewed:
                            arcrest.refreshDelegation(result[0])
                            renewed.add(result[0])
                    except Exception as exc:
                        self.log.error(f"Failed to renew delegation for appjob({job['appjobid']}): {exc}")
                    else:
                        self.log.info(f"Successfully renewed delegation {result[0]} for appjob({job['appjobid']})")
                        torestart.append(job)
                        arcids.append(job["IDFromEndpoint"])

            # restart jobs
            try:
                results = arcrest.restartJobs(arcids)
            except JSONDecodeError as exc:
                self.log.error(f"Invalid JSON response from ARC: {exc}")
                continue
            except Exception as exc:
                self.log.error(f"Error rerunning jobs in ARC: {exc}")
                continue
            finally:
                arcrest.close()

            tstamp = self.db.getTimeStamp()

            # log results and update DB
            for job, result in zip(torestart, results):
                if isinstance(result, ARCHTTPError):
                    error = result
                    if error.status == 505 and error.text == "No more restarts allowed":
                        self.db.updateArcJob(job["id"], {"arcstate": "failed", "State": "Failed", "tarcstate": tstamp, "tstate": tstamp})
                        self.log.error(f"Restart of appjob({job['appjobid']}) not allowed, setting to failed")
                    elif error.status == 505 and error.text == "Job has not failed":
                        self.db.updateArcJob(job["id"], {"arcstate": "submitted", "tarcstate": tstamp})
                        self.log.warning(f"appjob({job['appjobid']}) has not failed, setting to submitted")
                    elif error.status == 404:
                        self.db.updateArcJob(job["id"], {"arcstate": "tocancel", "tarcstate": tstamp})
                        self.log.warning(f"appjob({job['appjobid']}) not found, cancelling")
                    else:
                        self.db.updateArcJob(job["id"], {"arcstate": "torerun", "tarcstate": tstamp})
                        self.log.error(f"Error rerunning appjob({job['appjobid']}): {error.status} {error.text}")
                else:
                    self.db.updateArcJob(job["id"], {"arcstate": "submitted", "tarcstate": tstamp})
                    self.log.info(f"Successfully rerun appjob({job['appjobid']})")

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
