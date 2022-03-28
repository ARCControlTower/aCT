# aCTStatus.py
#
# Process to check the status of running ARC jobs
#
# Possible aCT job states:
# - toclean
# - tofetch
# - tocancel
# - toresubmit
# - tosubmit
# - torerun
#
# - submitted
# - donefailed
# - lost
# - cancelled
# - done
# - failed
# - finished
#
# - running
# - submitting
# - cancelling
# - finishing
# - holding
#
#
# We assume that jobs in states:
# - toclean
# - tofetch
# - tocancel
# - toresubmit
# - submitting
# - torerun
# cannot timeout because we rely on aCT handling such jobs in time.
#
# We assume that jobs in states:
# - failed
# - done
# - cancelled
# - lost
# - donefailed
# cannot timeout as they are responsibility of an app engine.
#
# We assume that jobs in states:
# - submitted
# - running
# - holding
# don't need to be checked for timeout because they will
# timeout as a result of stuck ARC state.
#
# The aCT states that need to be monitored for timeout are:
# - tosubmit (done by aCTSubmitter)
# - cancelling
# - finishing
#
# The ARC states that should not be checked for timeout (or rather, not be
# cancelled on timeout):
# - FINISHED
# - FAILED
# - KILLING
# - KILLED
# - WIPED


import time
import arc
import os
import ssl
import json
import http.client
from datetime import datetime
from urllib.parse import urlparse

from act.common.aCTProcess import aCTProcess
from act.common.config import Config
from act.arc.rest import getJobsInfo, getJobsStatus
from act.common.exceptions import ACTError


class aCTStatus(aCTProcess):
    '''
    Class for checking the status of submitted ARC jobs and updating their
    status in the DB.
    '''

    def __init__(self):

        aCTProcess.__init__(self)

        # store the last checkJobs time to avoid overloading of GIIS
        self.checktime = time.time()

    def resetJobs(self, jobstoreset):
        '''
        Empty all StringLists in jobs so that when they are updated they do not
        contain duplicate values, since ARC always appends to these lists.
        '''
        emptylist = arc.StringList()
        j = arc.Job()
        attrstoreset = [attr for attr in dir(j) if type(getattr(j, attr)) == arc.StringList]

        for jobs in jobstoreset.values():
            for job in jobs:
                for attr in attrstoreset:
                    setattr(job[2], attr, emptylist)

#    def processJobErrors(self, id, appjobid, failedjob):
#        '''
#        Examine errors of failed job and decide whether to resubmit or not
#        '''
#        errors = ";".join([joberr for joberr in failedjob.Error])
#        self.log.info("%s: Job failure for %s: %s" % (appjobid, failedjob.JobID, errors))
#
#        # First check if it was a data staging problem
#        if failedjob.RestartState == arc.JobState.PREPARING or \
#           failedjob.RestartState == arc.JobState.FINISHING:
#            # Don't retry when output list is not available
#            if 'Error reading user generated output file list' not in errors:
#                self.log.info("%s: Will rerun job %s" % (appjobid, failedjob.JobID))
#                # Reset arc job state so that next time new state will be picked up
#                failedjob.State = arc.JobState('Undefined')
#                return "torerun"
#
#        newstate = "failed"
#        # Check if any job runtime error matches any error in the toresubmit list
#        resub = [err for err in self.conf.getList(['errors','toresubmit','arcerrors','item']) if err in errors]
#        attemptsleft = int(self.db.getArcJobInfo(id, ['attemptsleft'])['attemptsleft']) - 1
#        if attemptsleft < 0:
#            attemptsleft = 0
#        self.db.updateArcJob(id, {'attemptsleft': str(attemptsleft)})
#        if resub:
#            if not attemptsleft:
#                self.log.info("%s: Job %s out of retries" % (appjobid, failedjob.JobID))
#            else:
#                self.log.info("%s: Will resubmit job %s, %i attempts left" % (appjobid, failedjob.JobID, attemptsleft))
#                failedjob.State = arc.JobState('Undefined')
#                newstate = "toresubmit"
#
#        else:
#            self.log.info("%s: Job %s has fatal errors, cannot resubmit" % (appjobid, failedjob.JobID))
#        return newstate

    def checkJobs(self):
        '''
        Query all running jobs
        '''
        COLUMNS = ["id", "appjobid", "proxyid", "IDFromEndpoint", "created",
                   "State", "JobID"]

        # minimum time between checks
        if time.time() < self.checktime + int(self.conf.get(['jobs', 'checkmintime'])):
            self.log.debug("mininterval not reached")
            return
        self.checktime = time.time()

        # check jobs which were last checked more than checkinterval ago
        tstampCond = self.db.timeStampLessThan("tarcstate", self.conf.get(['jobs', 'checkinterval']))
        jobstocheck = self.db.getArcJobsInfo(
            "arcstate in ('submitted', 'running', 'finishing', 'cancelling', "
            f"'holding') and jobid not like '' and cluster='{self.cluster}' "
            f"and {tstampCond} limit 100000",
            COLUMNS
        )
        if not jobstocheck:
            return

        self.log.info(f"{len(jobstocheck)} jobs to check")

        #self.resetJobs(jobstocheck)

        # aggregate jobs by proxyid
        jobsdict = {}
        for row in jobstocheck:
            if not row["proxyid"] in jobsdict:
                jobsdict[row["proxyid"]] = []
            jobsdict[row["proxyid"]].append(row)

        for proxyid, jobs in jobsdict.items():

            tocheck = []
            for job in jobs:
                jobdict = {
                    "arcid": job["IDFromEndpoint"],
                    "id": job["id"],
                    "appjobid": job["appjobid"],
                    "State": job["State"],
                    "JobID": job["JobID"],
                    "created": job["created"],
                }
                tocheck.append(jobdict)

            proxypath = os.path.join(self.db.proxydir, f"proxiesid{proxyid}")
            conn = None
            try:
                # create proxy authenticated connection
                context = ssl.SSLContext(ssl.PROTOCOL_TLS)
                context.load_cert_chain(proxypath, keyfile=proxypath)
                url = urlparse(self.cluster)
                conn = http.client.HTTPSConnection(url.netloc, context=context)
                self.log.debug(f"Connected to cluster {url.netloc}")

                # get jobs' status and info
                tocheck = getJobsStatus(conn, tocheck)
                tocheck = getJobsInfo(conn, tocheck)

                # process jobs and update DB
                for job in tocheck:
                    if "msg" in job:
                        self.log.error(f"Failed to get info for appjobid({job['appjobid']}), id({job['id']}): {job['msg']}")
                        continue

                    if job["State"] == job["state"]:
                        continue

                    self.log.info(f"Job {job['JobID']}: {job['State']} -> {job['state']}")

                    tstamp = self.db.getTimeStamp()
                    jobdict = {"State": job["state"], "tstate": tstamp}

                    if job["state"] in ("ACCEPTING", "ACCEPTED", "PREPARING",
                                        "PREPARED", "SUBMITTING", "QUEUING"):
                        jobdict["arcstate"] = "submitted"

                    elif job["state"] in ("RUNNING", "EXITINGLRMS",
                                          "EXECUTED"):
                        jobdict["arcstate"] = "running"

                    elif job["state"] == "HELD":
                        jobdict["arcstate"] = "holding"

                    elif job["state"] == "FINISHING":
                        jobdict["arcstate"] = "finishing"

                    elif job["state"] == "FINISHED":
                        exitCode = job["info_document"]["ComputingActivity"]["ExitCode"]
                        if exitCode == -1:
                            # missing exit code, but assume success
                            self.log.warning(f"Job {job['JobID']} FINISHED but has missing exit code, setting to zero")
                            jobdict["ExitCode"] = 0
                        else:
                            jobdict["ExitCode"] = exitCode
                        jobdict["arcstate"] = "finished"

                    elif job["state"] == "FAILED":
                        jobdict["arcstate"] = "failed"
                        self.log.info(f"Job {job['JobID']} failed, dumping info:\n{json.dumps(job['info_document'], indent=4)}")

                    elif job["state"] == "KILLED":
                        jobdict["arcstate"] = "cancelled"

                    elif job["state"] == "WIPED":  # TODO: which one really?
                        jobdict["arcstate"] = "cancelled"

                    if "arcstate" in jobdict:
                        jobdict["tarcstate"] = tstamp

                    # difference of two datetime objects yields timedelta object
                    # with seconds attribute
                    fromCreated = (datetime.now() - job["created"]).seconds // 60

                    # fix reported wall time and cpu time
                    activityDict = job["info_document"].get("ComputingActivity", {})
                    wallTime = int(activityDict.get("UsedTotalWallTime", 0))
                    slots = int(activityDict.get("RequestedSlots", 0))
                    if wallTime and slots:
                        wallTime = wallTime // slots
                        if wallTime > fromCreated:
                            self.log.warning(f"Job {job['JobID']}: Fixing reported walltime {wallTime} to {fromCreated}")
                            jobdict["UsedTotalWallTime"] = fromCreated
                    cpuTime = int(activityDict.get("UsedTotalCPUTime", 0))
                    if cpuTime and cpuTime > 10 ** 7:
                        self.log.warning(f"Job {job['JobID']}: Discarding reported CPUtime {cpuTime}")
                        jobdict["UsedTotalCPUTime"] = -1

                    self.db.updateArcJobLazy(job["id"], jobdict)

            except ssl.SSLError as e:
                self.log.error(f"Could not create SSL context for proxy {proxypath}: {e}")
            except (http.client.HTTPException, ConnectionError) as e:
                self.log.error(f"Could not connect to cluster {url.netloc}: {e}")
            except ACTError as e:
                self.log.error(f"Error fetching info for ARC jobs: {e}")
            finally:
                if conn:
                    conn.close()

#            self.uc.CredentialString(str(self.db.getProxy(proxyid)))
#
#            job_supervisor = arc.JobSupervisor(self.uc, [j[2] for j in jobs])
#            job_supervisor.Update()
#            jobsupdated = job_supervisor.GetAllJobs()
#            jobsnotupdated = job_supervisor.GetIDsNotProcessed()
#
#            for (originaljobinfo, updatedjob) in zip(jobs, jobsupdated):
#                (id, appjobid, originaljob, created) = originaljobinfo
#                if updatedjob.JobID in jobsnotupdated:
#                    self.log.error("%s: Failed to find information on %s" % (appjobid, updatedjob.JobID))
#                    continue
#                if updatedjob.JobID != originaljob.JobID:
#                    # something went wrong with list order
#                    self.log.warning("%s: Bad job id (%s), expected %s" % (appjobid, updatedjob.JobID, originaljob.JobID))
#                    continue
#                # compare strings here to get around limitations of JobState API
#                # map INLRMS:S and O to HOLD (not necessary when ARC 4.1 is used)
#                if updatedjob.State.GetGeneralState() == 'Queuing' and (updatedjob.State.GetSpecificState() == 'INLRMS:S' or updatedjob.State.GetSpecificState() == 'INLRMS:O'):
#                    updatedjob.State = arc.JobState('Hold')
#                if originaljob.State.GetGeneralState() == updatedjob.State.GetGeneralState() \
#                     and self.cluster not in ['gsiftp://gar-ex-etpgrid1.garching.physik.uni-muenchen.de:2811/preempt', 'gsiftp://arc1-it4i.farm.particle.cz/qfree', 'gsiftp://arc2-it4i.farm.particle.cz/qfree']:
#                    # just update timestamp
#                    # Update numbers every time for superMUC since walltime is missing for finished jobs
#                    self.db.updateArcJob(id, {'tarcstate': self.db.getTimeStamp()})
#                    continue
#
#                self.log.info("%s: Job %s: %s -> %s (%s)" % (appjobid, originaljob.JobID, originaljob.State.GetGeneralState(),
#                               updatedjob.State.GetGeneralState(), updatedjob.State.GetSpecificState()))
#
#                # state changed, update whole Job object
#                arcstate = 'submitted'
#                if updatedjob.State == arc.JobState.FINISHED:
#                    if updatedjob.ExitCode == -1:
#                        # Missing exit code, but assume success
#                        self.log.warning("%s: Job %s FINISHED but has missing exit code, setting to zero" % (appjobid, updatedjob.JobID))
#                        updatedjob.ExitCode = 0
#                    arcstate = 'finished'
#                    self.log.debug('%s: reported walltime %d, cputime %d' % (appjobid, updatedjob.UsedTotalWallTime.GetPeriod(), updatedjob.UsedTotalCPUTime.GetPeriod()))
#                elif updatedjob.State == arc.JobState.FAILED:
#                    # EMI-ES reports cancelled jobs as failed so check substate (this is fixed in ARC 6.8)
#                    if 'cancel' in updatedjob.State.GetSpecificState():
#                        arcstate = 'cancelled'
#                    else:
#                        arcstate = self.processJobErrors(id, appjobid, updatedjob)
#                elif updatedjob.State == arc.JobState.KILLED:
#                    arcstate = 'cancelled'
#                elif updatedjob.State == arc.JobState.RUNNING:
#                    arcstate = 'running'
#                elif updatedjob.State == arc.JobState.FINISHING:
#                    arcstate = 'finishing'
#                elif updatedjob.State == arc.JobState.HOLD:
#                    arcstate = 'holding'
#                elif updatedjob.State == arc.JobState.DELETED or \
#                     updatedjob.State == arc.JobState.OTHER:
#                    # unexpected
#                    arcstate = 'failed'
#
#                # Walltime reported by ARC 6 is multiplied by cores
#                if arc.ARC_VERSION_MAJOR >= 6 and updatedjob.RequestedSlots > 0:
#                    updatedjob.UsedTotalWallTime = arc.Period(updatedjob.UsedTotalWallTime.GetPeriod() // updatedjob.RequestedSlots)
#                # Fix crazy wallclock and CPU times
#                if updatedjob.UsedTotalWallTime > arc.Time() - arc.Time(int(created.strftime("%s"))):
#                    fixedwalltime = arc.Time() - arc.Time(int(created.strftime("%s")))
#                    self.log.warning("%s: Fixing reported walltime %d to %d" % (appjobid, updatedjob.UsedTotalWallTime.GetPeriod(), fixedwalltime.GetPeriod()))
#                    updatedjob.UsedTotalWallTime =
#                if updatedjob.UsedTotalCPUTime > arc.Period(10**7):
#                    self.log.warning("%s: Discarding reported CPUtime %d" % (appjobid, updatedjob.UsedTotalCPUTime.GetPeriod()))
#                    updatedjob.UsedTotalCPUTime = arc.Period(-1)
#                self.db.updateArcJob(id, {'arcstate': arcstate, 'tarcstate': self.db.getTimeStamp(), 'tstate': self.db.getTimeStamp()}, updatedjob)

        self.log.info('Done')

#    def checkLostJobs(self):
#        '''
#        Move jobs with a long time since status update to lost.
#        '''
#
#        # 2 days limit. TODO: configurable?
#        tstampCond = self.db.timeStampLessThan("tarcstate", 172800)
#        jobs = self.db.getArcJobsInfo(
#            "(arcstate='submitted' or arcstate='running' or arcstate="
#            "'cancelling' or arcstate='finished') and cluster="
#            f"'{self.cluster}' and {tstampCond}",
#            ['id', 'appjobid', 'JobID', 'arcstate']
#        )
#
#        for job in jobs:
#            if job["arcstate"] == "cancelling":
#                self.log.warning(f"Job {job['JobID']} lost from information system, marking as cancelled")
#                self.db.updateArcJob(job["id"], {"arcstate": "cancelled", "tarcstate": self.db.getTimeStamp()})
#            else:
#                self.log.warning("Job {job['JobID']} lost from information system, marking as lost")
#                self.db.updateArcJob(job["id"], {"arcstate": "lost", "tarcstate": self.db.getTimeStamp()})
#
#    def checkStuckJobs(self):
#        '''
#        check jobs with tstate too long ago and set them tocancel
#        maxtimestate can be set in arc config file for any state in arc.JobState,
#        e.g. maxtimerunning, maxtimequeuing
#        Also mark as cancelled jobs stuck in cancelling for more than one hour
#        '''
#        COLUMNS = ["id", "JobID", "arcstate", "state", "tarcstate", "tstate"]
#
#        config = Config()
#
#        timeoutDict = config["timeouts"]["ARC_state"]
#        for state, timeout in timeoutDict.items():
#
#            tstampCond = self.db.timeStampLessThan("tstate", timeout)
#            jobs = self.db.getArcJobsInfo(f"cluster='{self.cluster}' and state='{state}' {tstampCond}", COLUMNS)
#
#            for job in jobs:
#
#                # TODO: why this block? There is no indication that jobs can remain stuck in "toclean"
#                #if job["arcstate"] == "toclean":
#                #    self.log.info(f"Job {job['JobID']} stuck in toclean for too long, deleting")
#                #    self.db.deleteArcJob(job["id"])
#                #    continue
#
#                self.log.warning(f"Job {job['JobID']} too long in state {state}")
#                tstamp = self.db.getTimeStamp()
#                if job["JobID"]:
#                    self.db.updateArcJobLazy(job["id"], {"arcstate": "tocancel", "tarcstate": tstamp, 'tstate': tstamp})
#                else:
#                    self.db.updateArcJobLazy(job["id"], {"arcstate": "cancelled", "tarcstate": tstamp, 'tstate': tstamp})
#
#            self.db.Commit()
#
#        # stuck in cancelling to cancelled
#        # TODO: tstamp used to be "tstate", was this a bug or a trick?
#        tstampCond = self.db.timeStampLessThan("tarcstate", config["timeouts"]["aCT_state"]["cancelling"])
#        jobs = self.db.getArcJobsInfo(
#            f"arcstate='cancelling' and cluster='{self.cluster}' and "
#            f"{tstampCond}",
#            ['id', 'JobID']
#        )
#        for job in jobs:
#            self.log.info(f"Job {job['JobID']} stuck in cancelling for more than 1 hour, marking cancelled")
#            tstamp = self.db.getTimeStamp()
#            self.db.updateArcJob(job["id"], {"arcstate": "cancelled", "tarcstate": tstamp, 'tstate': tstamp})

    def checkACTStateTimeouts(self):
        COLUMNS = ["id", "JobID"]

        config = Config()

        timeoutDict = config["timeouts"]["aCT_state"]

        # mark jobs stuck in cancelling as cancelled
        tstampCond = self.db.timeStampLessThan("tarcstate", timeoutDict["cancelling"])
        jobs = self.db.getArcJobsInfo(f"cluster='{self.cluster}' and arcstate='cancelling' and {tstampCond}", COLUMNS)
        for job in jobs:
            self.log.warning(f"Job {job['JobID']} too long in \"cancelling\"")
            tstamp = self.db.getTimeStamp()
            self.db.updateArcJob(job["id"], {"arcstate": "cancelled", "tarcstate": tstamp, 'tstate': tstamp})
        self.db.Commit()

    def checkARCStateTimeouts(self):
        COLUMNS = ["id", "JobID", "arcstate", "State"]

        config = Config()

        timeoutDict = config["timeouts"]["ARC_state"]
        for state, timeout in timeoutDict.items():

            tstampCond = self.db.timeStampLessThan("tstate", timeout)
            jobs = self.db.getArcJobsInfo(f"cluster='{self.cluster}' and State='{state}' and {tstampCond}", COLUMNS)

            for job in jobs:

                # do not cancel already cancelled jobs
                if job["arcstate"] in ("cancelled", "cancelling"):
                    continue

                # do not cancel jobs in states that don't make sense
                if job["State"] in ("FINISHED", "FAILED", "KILLING", "KILLED", "WIPED"):
                    continue

                # cancel jobs
                self.log.warning(f"Job {job['JobID']} too long in state {state}")
                tstamp = self.db.getTimeStamp()
                if job["JobID"]:
                    self.db.updateArcJobLazy(job["id"], {"arcstate": "tocancel", "tarcstate": tstamp, 'tstate': tstamp})
                else:
                    self.db.updateArcJobLazy(job["id"], {"arcstate": "cancelled", "tarcstate": tstamp, 'tstate': tstamp})

        self.db.Commit()

    def process(self):
        # check job status
        self.checkJobs()

        ## check for lost jobs
        #self.checkLostJobs()
        ## check for stuck jobs too long in one state and kill them
        #self.checkStuckJobs()

        self.checkARCStateTimeouts()
        self.checkACTStateTimeouts()


if __name__ == '__main__':
    st = aCTStatus()
    st.run()
    st.finish()
