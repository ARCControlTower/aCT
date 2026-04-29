import datetime
from json import JSONDecodeError
from random import shuffle
from urllib.parse import urlparse
from collections import defaultdict

from act.arc.aCTARCProcessNEW import aCTARCProcess
from act.arc.aCTStatus import ARC_STATE_MAPPING
from act.arc.dbModels import ArcJob, JobDescription
from pyarcrest.errors import (ARCError, ARCHTTPError, DescriptionParseError,
                              DescriptionUnparseError, InputFileError,
                              InputUploadError, MatchmakingError,
                              NoValueInARCResult)
from sqlalchemy import select, or_, update
from sqlalchemy.sql import func


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

        with self.db.Session() as session:
            nsubmitted = session.execute(select(func.count()).select_from(ArcJob).where(ArcJob.cluster==self.cluster)).scalar()

        if nsubmitted >= clustermaxjobs:
            self.log.info(f'{nsubmitted} submitted jobs is greater than or equal to max jobs {clustermaxjobs}')
            return

        # Apply fair-share
        with self.db.Session() as session:
            if self.cluster:
                fairshares = session.execute(select(ArcJob.fairshare, ArcJob.proxyid).where(ArcJob.arcstate=='tosubmit', ArcJob.clusterlist.like(f'%{self.cluster}%'))).all()
            else:
                fairshares = session.execute(select(ArcJob.fairshare, ArcJob.proxyid).where(ArcJob.arcstate=='tosubmit', ArcJob.clusterlist=='')).all()

        if not fairshares:
            self.log.info('Nothing to submit')
            return

        # split by proxy for GU queues
        fairshares = list(set([(p.fairshare, p.proxyid) for p in fairshares]))
        # For proxy bug - see below
        shuffle(fairshares)

        limit = min(clustermaxjobs - nsubmitted, 100)

        # Divide limit among fairshares, unless exiting after first loop due to
        # proxy bug, but make sure at least one job is submitted
        with self.db.Session() as session:
            if len(self.db.getProxiesInfo(session, {}, ['id'])) == 1:
                limit = max(limit // len(fairshares), 1)

        for fairshare, proxyid in fairshares:
            self.stopOnFlag()

            # Exit loop if above limit
            if nsubmitted >= clustermaxjobs:
                self.log.info(f"CE is at limit of {clustermaxjobs} submitted jobs, exiting")
                break

            # Get jobs to submit and set them to "submitting". Lock is required
            # for race with other submitters and act.client.jobmgr.killJobs().
            with self.db.Session.begin() as session:
                stmt = select(ArcJob.id,
                              ArcJob.jobdesc,
                              ArcJob.appjobid,
                              ArcJob.priority,
                              ArcJob.proxyid,
                              JobDescription.jobdescription) \
                    .where(ArcJob.arcstate=='tosubmit',
                            ArcJob.fairshare==fairshare,
                            ArcJob.proxyid==proxyid,
                            or_(ArcJob.clusterlist.like(f'%{self.cluster}'), ArcJob.clusterlist.like(f'%{self.cluster},%'))) \
                    .join(ArcJob.jobdescobj) \
                    .limit(limit) \
                    .with_for_update(skip_locked=True)
                jobs = session.execute(stmt).all()
                if jobs:
                    session.execute(update(ArcJob).where(ArcJob.id.in_([job.id for job in jobs])).values(arcstate='submitting', tarcstate=self.db.getTimeStamp(), cluster=self.cluster))

            if not jobs:
                self.log.debug("No jobs to submit")
                continue

            self.log.info(f"Submitting {len(jobs)} jobs for fairshare {fairshare} and proxyid {proxyid}")

            ## Set UserConfig credential for querying infosys
            #proxystring = str(self.db.getProxy(proxyid))
            #self.uc.CredentialString(proxystring)
            #global usercred
            #usercred = self.uc

            # Filter only sites for this process
            with self.db.Session() as session:
                qjobs = session.execute(select(ArcJob.id, ArcJob.priority).where(ArcJob.arcstate=='submitted', ArcJob.cluster==self.cluster, ArcJob.fairshare==fairshare)).all()
                rjobs = session.execute(select(ArcJob.id, ArcJob.priority).where(ArcJob.arcstate=='running', ArcJob.cluster==self.cluster, ArcJob.fairshare==fairshare)).all()

            # max waiting priority
            try:
                maxpriowaiting = max(jobs, key=lambda x: x.priority).priority
            except:
                maxpriowaiting = 0
            self.log.info(f"Maximum priority of waiting jobs: {maxpriowaiting}")


            # max queued priority
            try:
                maxprioqueued = max(qjobs, key=lambda x: x.priority).priority
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
            descs = [job.jobdescription for job in jobs]

            # get REST client
            jobids = [job.id for job in jobs]
            arcrest = self.getARCClient(proxyid)

            with self.db.Session.begin() as session:
                if not arcrest:
                    session.execute(self.db.setJobsArcstate(jobids, 'tosubmit'))
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
                    session.execute(self.db.setJobsArcstate(jobids, 'tosubmit'))
                    self.log.error(f"Invalid JSON response from ARC: {exc}")
                    continue
                except MatchmakingError as exc:
                    session.execute(self.db.setJobsArcstate(jobids, 'cancelled'))
                    self.log.error(str(exc))
                    continue
                except Exception as exc:
                    session.execute(self.db.setJobsArcstate(jobids, 'tosubmit'))
                    self.log.error(f"Error submitting jobs to ARC: {exc}", exc_info=True, stack_info=True)
                    #self.log.error(f"Error submitting jobs to ARC: {exc}")
                    continue
                finally:
                    arcrest.close()

                tstamp = self.db.getTimeStamp()

                # log submission results and set job state
                for job, result in zip(jobs, results):
                    jobdict = {}
                    if result.error:
                        error = result.value
                        if isinstance(error, ARCError):
                            if type(error) in (InputFileError, DescriptionParseError, DescriptionUnparseError, MatchmakingError):
                                jobdict["arcstate"] = "cancelled"
                                self.log.error(f"Error submitting appjob({job.appjobid}): {error}")
                            elif isinstance(error, InputUploadError):
                                jobdict["arcstate"] = "tocancel"
                                jobdict["cluster"] = self.cluster
                                jobdict["IDFromEndpoint"] = error.jobid
                                for exc in error.errors:
                                    self.log.error(f"Error uploading input files for appjob({job.appjobid}): {exc}")
                                self.log.info(f"Cancelling appjob({job.appjobid}) due to upload errors")
                            else:
                                jobdict["arcstate"] = "tosubmit"
                                self.log.error(f"Error submitting appjob({job.appjobid}): {error}")
                    else:
                        jobid, state = result.value
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
                        self.log.info(f"Submission successfull for appjob({job.appjobid}): {jobid}")

                    jobdict["tarcstate"] = tstamp
                    session.execute(update(ArcJob).where(ArcJob.id==job.id).values(**jobdict))

            nsubmitted += limit

        self.log.info("Done")

    def checkFailedSubmissions(self):
        """
        Cancel jobs that are too long in submitting.

        Signal handling strategy:
        - termination is checked before handling every job
        """
        with self.db.Session.begin() as session:
            tstamp = self.db.getTimeStamp()
            limit = tstamp - datetime.timedelta(hours=1)
            dbjobs = session.execute(select(ArcJob.id, ArcJob.appjobid) \
                                     .where(ArcJob.arcstate=='tosubmit', ArcJob.cluster==self.cluster, ArcJob.created<limit)
                                     .with_for_update(skip_locked=True)).all()
            if dbjobs:
                session.execute(self.db.setJobsArcstate([job.id for job in dbjobs], 'tocancel'))
                for job in dbjobs:
                    self.log.warning(f"Cancelling appjob({job.appjobid}) for being too long in tosubmit")

    def processToCancel(self):
        """
        Cancel jobs in ARC.

        Signal handling strategy:
        - termination is checked before handling every proxyid job batch
        """
        # make jobs that are taking too long cancelled
        with self.db.Session.begin() as session:
            tstamp = self.db.getTimeStamp()
            # TODO: HARDCODED
            limit = tstamp - datetime.timedelta(hours=1)
            jobstocancel = session.execute(select(ArcJob.id, ArcJob.appjobid) \
                                           .where(ArcJob.arcstate=='tocancel', ArcJob.cluster==self.cluster, ArcJob.tarcstate<limit)).all()
            if jobstocancel:
                session.execute(self.db.setJobsArcstate([job.id for job in jobstocancel], 'cancelled'))
                for job in jobstocancel:
                    self.log.warning(f"Could not cancel appjob({job.appjobid}) in time, setting to cancelled")

        # cancel remaining jobs in tocancel state
        with self.db.Session() as session:
            tocancel = session.execute(select(ArcJob.id, ArcJob.proxyid, ArcJob.appjobid, ArcJob.IDFromEndpoint) \
                                            .where(ArcJob.arcstate=='tocancel', ArcJob.cluster==self.cluster)).all()
        
        if not tocancel:
            self.log.info(f"Nothing to cancel")
            return
        self.log.info(f"Cancelling {len(tocancel)} jobs")

        # aggregate jobs by proxyid
        jobsdict = defaultdict(list)
        for job in tocancel:
            jobsdict[job.proxyid].append(job)
        
        for proxyid, dbjobs in jobsdict.items():
            self.stopOnFlag()
            # partition the jobs based on whether they are in ARC; ARC jobs
            # need to be killed in ARC first, others can be set to cancelled
            # directly
            toARCKill = []
            arcids = []
            cancelled = []
            for dbjob in dbjobs:
                if dbjob.IDFromEndpoint is not None:
                    toARCKill.append(dbjob)
                    arcids.append(dbjob.IDFromEndpoint)
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
            
            with self.db.Session.begin() as session:
                # log ARC results and update DB
                for job, result in zip(toARCKill, results):
                    if result.error:
                        error = result.value
                        if isinstance(error, ARCHTTPError):
                            state = "cancelled"
                            if error.status == 404:
                                self.log.warning(f"appjob({job.appjobid}) missing in ARC, setting to cancelled")
                            else:
                                self.log.error(f"Error killing appjob({job.appjobid}): {error.status} {error.text}")
                    else:
                        state = "cancelling"
                        self.log.info(f"ARC will cancel appjob({job.appjobid})")
                    session.execute(update(ArcJob).where(ArcJob.id==job.id).values(arcstate=state, tarcstate=tstamp))

                # update DB for jobs not in ARC
                if cancelled:
                    session.execute(self.db.setJobsArcstate([job.id for job in cancelled], 'cancelled'))
                    for job in cancelled:
                        self.log.info(f"appjob({job.appjobid}) not in ARC, setting to cancelled directly")

    def processToResubmit(self):
        """
        Resubmit jobs to ARC.

        Resubmission requires cleaning of existing ARC jobs and then setting
        the jobs to tosubmit to be submitted normally again.

        Signal handling strategy:
        - termination is checked before handling every proxyid job batch
        """
        # fail jobs that are taking too long
        with self.db.Session.begin() as session:
            tstamp = self.db.getTimeStamp()
            limit = tstamp - datetime.timedelta(hours=1)
            jobstoresubmit = session.execute(select(ArcJob.id, ArcJob.appjobid) \
                                             .where(ArcJob.arcstate=='toresubmit', ArcJob.cluster==self.cluster, ArcJob.tarcstate<limit)).all()
            if jobstoresubmit:
                session.execute(update(ArcJob).where(ArcJob.id.in_([job.id for job in jobstoresubmit])).values(arcstate='failed', tarcstate=tstamp, attemptsleft=0))
                for job in jobstoresubmit:
                    self.log.warning(f"Could not resubmit appjob({job.appjobid}) in time, setting to failed")

        # resubmit remaining jobs
        with self.db.Session() as session:
            toresubmit = session.execute(select(ArcJob.id, ArcJob.appjobid, ArcJob.proxyid, ArcJob.IDFromEndpoint) \
                                             .where(ArcJob.arcstate=='toresubmit', ArcJob.cluster==self.cluster)).all()
        if not toresubmit:
            self.log.info(f"Nothing to resubmit")
            return
        self.log.info(f"Resubmitting {len(toresubmit)} jobs")

        # aggregate jobs by proxyid
        jobsdict = defaultdict(list)
        for job in toresubmit:
            jobsdict[job.proxyid].append(job)

        for proxyid, dbjobs in jobsdict.items():
            self.stopOnFlag()

            # create a list of jobs that need to be cleaned in ARC
            toARCClean = []
            arcids = []
            for dbjob in dbjobs:
                if dbjob.IDFromEndpoint is not None:
                    toARCClean.append(dbjob)
                    arcids.append(dbjob.IDFromEndpoint)

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
                if result.error:
                    error = result.value
                    if isinstance(error, ARCHTTPError):
                        self.log.error(f"Error cleaning appjob({job.appjobid}): {error.status} {error.text}")
                else:
                    self.log.info(f"Successfully cleaned appjob({job.appjobid})")

            tstamp = self.db.getTimeStamp()

            # set jobs for resubmission in DB
            # "created" needs to be reset so that it doesn't get understood
            # as failing to submit since first insertion.
            with self.db.Session.begin() as session:
                session.execute(update(ArcJob).where(ArcJob.id.in_([job.id for job in dbjobs])).values(arcstate='tosubmit', tarcstate=tstamp, created=tstamp))

    def processToRerun(self):
        """
        Rerun jobs in ARC.

        Signal handling strategy:
        - termination is checked before handling every proxyid job batch
        """
        # fail jobs that are taking too long
        with self.db.Session.begin() as session:
            tstamp = self.db.getTimeStamp()
            limit = tstamp - datetime.timedelta(hours=1)
            jobstorerun = session.execute(select(ArcJob.id, ArcJob.appjobid) \
                                             .where(ArcJob.arcstate=='torerun', ArcJob.cluster==self.cluster, ArcJob.tarcstate<limit)).all()
            if jobstorerun:
                session.execute(self.db.setJobsArcstate([job.id for job in jobstorerun], 'failed'))
                for job in jobstorerun:
                    self.log.warning(f"Could not restart appjob({job.appjobid}) in time, setting to failed")

        # rerun remaining jobs
        with self.db.Session() as session:
            torerun = session.execute(select(ArcJob.id, ArcJob.appjobid, ArcJob.proxyid, ArcJob.IDFromEndpoint) \
                                      .where(ArcJob.arcstate=='torerun', ArcJob.cluster==self.cluster)).all()

        if not torerun:
            self.log.info(f"Nothing to rerun")
            return
        self.log.info(f"Resuming {len(torerun)} jobs")

        # aggregate jobs by proxyid
        jobsdict = defaultdict(list)
        for job in torerun:
            jobsdict[job.proxyid].append(job)

        for proxyid, dbjobs in jobsdict.items():
            self.stopOnFlag()

            # get REST client
            arcrest = self.getARCClient(proxyid)
            if not arcrest:
                continue

            # get job delegations
            arcids = [job.IDFromEndpoint for job in dbjobs]
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
                if result.error:
                    error = result.value
                    if isinstance(error, ARCHTTPError):
                        self.log.error(f"Error getting delegations for appjob({job.appjobid}): {error.status} {error.text}")
                    elif isinstance(error, NoValueInARCResult):
                        self.log.error(f"NO VALUE IN SUCCESSFUL FETCH OF DELEGATIONS FOR appjob({job.appjobid})")
                else:
                    delegations = result.value
                    try:
                        # renewing the first delegation from the list works
                        # for aCT use case
                        if delegations[0] not in renewed:
                            arcrest.refreshDelegation(delegations[0])
                            renewed.add(delegations[0])
                    except Exception as exc:
                        self.log.error(f"Failed to renew delegation for appjob({job.appjobid}): {exc}")
                    else:
                        self.log.info(f"Successfully renewed delegation {delegations[0]} for appjob({job.appjobid})")
                        torestart.append(job)
                        arcids.append(job.IDFromEndpoint)

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
            with self.db.Session.begin() as session:
                for job, result in zip(torestart, results):
                    if result.error:
                        error = result.value
                        if isinstance(error, ARCHTTPError):
                            if error.status == 505 and error.text == "No more restarts allowed":
                                session.execute(update(ArcJob).where(ArcJob.id==job.id).values(arcstate='failed', State='Failed', tarcstate=tstamp, tstate=tstamp))
                                self.log.error(f"Restart of appjob({job.appjobid}) not allowed, setting to failed")
                            elif error.status == 505 and error.text == "Job has not failed":
                                session.execute(self.db.setJobsArcstate(job.id, 'submitted'))
                                self.log.warning(f"appjob({job.appjobid}) has not failed, setting to submitted")
                            elif error.status == 404:
                                session.execute(self.db.setJobsArcstate(job.id, 'tocancel'))
                                self.log.warning(f"appjob({job.appjobid}) not found, cancelling")
                            else:
                                session.execute(self.db.setJobsArcstate(job.id, 'torerun'))
                                self.log.error(f"Error rerunning appjob({job.appjobid}): {error.status} {error.text}")
                    else:
                        session.execute(self.db.setJobsArcstate(job.id, 'submitted'))
                        self.log.info(f"Successfully rerun appjob({job.appjobid})")

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
