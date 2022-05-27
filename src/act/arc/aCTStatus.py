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
# - finishing (finished?)
#
# The ARC states that should not be checked for timeout (or rather, not be
# cancelled on timeout):
# - FINISHED
# - FAILED
# - KILLING
# - KILLED
# - WIPED


import os
import time
from datetime import datetime, timedelta
from http.client import HTTPException
from ssl import SSLError
from urllib.parse import urlparse

from act.arc.rest import ARCError, ARCHTTPError, ARCRest
from act.common.aCTProcess import aCTProcess
from act.common.aCTJob import ACTJob

ARC_STATE_MAPPING = {
    "ACCEPTING": "Accepted",
    "ACCEPTED": "Accepted",
    "PREPARING": "Preparing",
    "PREPARED": "Preparing",
    "SUBMITTING": "Submitting",
    "QUEUING": "Queuing",
    "RUNNING": "Running",
    "HELD": "Hold",
    "EXITINGLRMS": "Running",
    "OTHER": "Running",
    "EXECUTED": "Running",
    "FINISHING": "Finishing",
    "FINISHED": "Finished",
    "FAILED": "Failed",
    "KILLING": "Running",
    "KILLED": "Killed",
    "WIPED": "Deleted",
}


class aCTStatus(aCTProcess):
    '''
    Class for checking the status of submitted ARC jobs and updating their
    status in the DB.
    '''

    def __init__(self):

        aCTProcess.__init__(self)

        # store the last checkJobs time to avoid overloading of GIIS
        self.checktime = time.time()

    def checkJobs(self):
        '''
        Query all running jobs
        '''
        COLUMNS = ["id", "appjobid", "proxyid", "IDFromEndpoint", "created",
                   "State", "attemptsleft", "tstate"]

        # parse cluster URL
        try:
            url = urlparse(self.cluster)
        except ValueError as exc:
            self.log.error(f"Error parsing cluster URL {url}: {exc}")
            return

        # minimum time between checks
        if time.time() < self.checktime + self.conf.jobs.checkmintime:
            self.log.debug("mininterval not reached")
            return
        self.checktime = time.time()

        # check jobs which were last checked more than checkinterval ago
        # TODO: HARDCODED
        tstampCond = self.db.timeStampLessThan("tarcstate", self.conf.jobs.checkinterval)
        jobstocheck = self.db.getArcJobsInfo(
            "arcstate in ('submitted', 'running', 'finishing', 'cancelling', "
            f"'holding') and jobid not like '' and cluster='{self.cluster}' "
            f"and {tstampCond} limit 100000",
            COLUMNS
        )
        if not jobstocheck:
            return

        self.log.info(f"{len(jobstocheck)} jobs to check")

        # aggregate jobs by proxyid
        jobsdict = {}
        for row in jobstocheck:
            if not row["proxyid"] in jobsdict:
                jobsdict[row["proxyid"]] = []
            jobsdict[row["proxyid"]].append(row)

        for proxyid, dbjobs in jobsdict.items():
            jobs = []
            tocheck = []
            oldStates = {}
            for dbjob in dbjobs:
                job = ACTJob()
                job.loadARCDBJob(dbjob)
                jobs.append(job)
                tocheck.append(job.arcjob)
                oldStates[job.arcid] = job.arcjob.state

            proxypath = os.path.join(self.db.proxydir, f"proxiesid{proxyid}")

            arcrest = None
            try:
                arcrest = ARCRest(url.hostname, port=url.port, proxypath=proxypath)
                joblist = {job["id"] for job in arcrest.getJobsList()}  # set type for performance
                arcrest.getJobsInfo(tocheck)
            except (HTTPException, ConnectionError, SSLError, ARCError, ARCHTTPError, TimeoutError) as e:
                self.log.error(f"Error fetching job info from ARC: {e}")
                continue
            finally:
                if arcrest:
                    arcrest.close()

            tstamp = self.db.getTimeStamp()

            # process jobs and update DB
            for job in jobs:

                # cancel jobs that are stuck in tstate and not in job list anymore
                # TODO: HARDCODED
                if job.arcjob.tstate + timedelta(days=7) < datetime.utcnow():
                    if job.arcjob.id not in joblist:
                        self.log.error(f"Job {job.appid} {job.arcid} not in ARC anymore, cancelling")
                        self.db.updateArcJobLazy(job.arcid, {"arcstate": "tocancel", "tarcstate": tstamp})
                        continue

                if job.arcjob.errors:
                    for error in job.arcjob.errors:
                        if isinstance(error, ARCHTTPError):
                            if error.status == 404:
                                self.log.error(f"Job {job.appid} {job.arcid} not found, cancelling")
                                self.db.updateArcJobLazy(job.arcid, {"arcstate": "tocancel", "tarcstate": tstamp})
                                continue
                        self.log.error(f"Error checking {job.appid} {job.arcid}: {error}")
                    continue

                if not job.arcjob.state:
                    continue

                mappedState = ARC_STATE_MAPPING[job.arcjob.state]
                if oldStates[job.arcid] == mappedState:
                    self.db.updateArcJobLazy(job.arcid, {"tarcstate": tstamp})
                    continue

                self.log.info(f"ARC status change for job {job.appid}: {oldStates[job.arcid]} -> {mappedState}")

                jobdict = {"State": mappedState, "tstate": tstamp}

                if job.arcjob.state in ("ACCEPTING", "ACCEPTED", "PREPARING", "PREPARED", "SUBMITTING", "QUEUING"):
                    jobdict["arcstate"] = "submitted"

                elif job.arcjob.state in ("RUNNING", "EXITINGLRMS", "EXECUTED"):
                    jobdict["arcstate"] = "running"

                elif job.arcjob.state == "HELD":
                    jobdict["arcstate"] = "holding"

                elif job.arcjob.state == "FINISHING":
                    jobdict["arcstate"] = "finishing"

                elif job.arcjob.state == "FINISHED":
                    if job.ExitCode is None:
                        # missing exit code, but assume success
                        self.log.warning(f"Job {job.appid} is finished but has missing exit code, setting to zero")
                        jobdict["ExitCode"] = 0
                    else:
                        jobdict["ExitCode"] = job.ExitCode
                    jobdict["arcstate"] = "finished"

                elif job.arcjob.state == "FAILED":
                    jobdict["arcstate"] = "failed"
                    patchDict = self.processJobErrors(job)
                    jobdict.update(patchDict)

                elif job.arcjob.state == "KILLED":
                    jobdict["arcstate"] = "cancelled"

                elif job.arcjob.state == "WIPED":
                    jobdict["arcstate"] = "cancelled"

                if "arcstate" in jobdict:
                    jobdict["tarcstate"] = tstamp

                # difference of two datetime objects yields timedelta object
                # with seconds attribute
                fromCreated = (datetime.utcnow() - job.tcreated).seconds // 60

                if job.UsedTotalWallTime and job.RequestedSlots != -1:
                    wallTime = job.UsedTotalWallTime // job.RequestedSlots
                    if wallTime > fromCreated:
                        self.log.warning(f"Job {job.appid}: Fixing reported walltime {wallTime} to {fromCreated}")
                        jobdict["UsedTotalWallTime"] = fromCreated
                    else:
                        jobdict["UsedTotalWallTime"] = wallTime
                else:
                    self.log.warning(f"Job {job.appid}: No reported walltime, using DB timestamps: {fromCreated}")
                    jobdict["UsedTotalWallTime"] = fromCreated

                if job.UsedTotalCPUTime:
                    # TODO: HARDCODED
                    if job.UsedTotalCPUTime > 10 ** 7:
                        self.log.warning(f"Job {job.appid}: Discarding reported CPUtime {job.UsedTotalCPUTime}")
                        jobdict["UsedTotalCPUTime"] = -1
                    else:
                        jobdict["UsedTotalCPUTime"] = job.UsedTotalCPUTime

                if job.Type:
                    jobdict["Type"] = job.Type
                if job.LocalIDFromManager:
                    jobdict["LocalIDFromManager"] = job.LocalIDFromManager
                if job.WaitingPosition:
                    jobdict["WaitingPosition"] = job.WaitingPosition
                if job.Owner:
                    jobdict["Owner"] = job.Owner
                if job.LocalOwner:
                    jobdict["LocalOwner"] = job.LocalOwner
                if job.RequestedTotalCPUTime:
                    jobdict["RequestedTotalCPUTime"] = job.RequestedTotalCPUTime
                if job.RequestedSlots:
                    jobdict["RequestedSlots"] = job.RequestedSlots
                if job.StdIn:
                    jobdict["StdIn"] = job.StdIn
                if job.StdOut:
                    jobdict["StdOut"] = job.StdOut
                if job.StdErr:
                    jobdict["StdErr"] = job.StdErr
                if job.LogDir:
                    jobdict["LogDir"] = job.LogDir
                if job.ExecutionNode:
                    jobdict["ExecutionNode"] = ",".join(job.ExecutionNode)
                if job.Queue:
                    jobdict["Queue"] = job.Queue
                if job.UsedMainMemory:
                    jobdict["UsedMainMemory"] = job.UsedMainMemory
                if job.SubmissionTime:
                    jobdict["SubmissionTime"] = job.SubmissionTime
                if job.EndTime:
                    jobdict["EndTime"] = job.EndTime
                if job.WorkingAreaEraseTime:
                    jobdict["WorkingAreaEraseTime"] = job.WorkingAreaEraseTime
                if job.ProxyExpirationTime:
                    jobdict["ProxyExpirationTime"] = job.ProxyExpirationTime

                # AF BUG
                try:
                    self.db.updateArcJobLazy(job.arcid, jobdict)
                except:
                    self.log.error(f"Bad dict for job {job.appid} {job.arcid}: {jobdict}")

            self.db.Commit()

        self.log.info('Done')

    # Returns a dictionary reflecting job changes that should update the final
    # jobdict for DB
    def processJobErrors(self, job):
        tstamp = self.db.getTimeStamp()
        patchDict = {"arcstate": "failed", "tarcstate": tstamp}

        resub = []
        if job.arcjob.errors:
            errors = ";".join(job.arcjob.errors)
            resub = [err for err in self.conf.errors.toresubmit.arcerrors if err in errors]
            self.log.info(f"Job {job.appid} {job.arcid} failed with error: {errors}")
            patchDict["Error"] = errors
        else:
            self.log.info(f"Job {job.appid} {job.arcid} failed, no error given")

        restartState = ""
        for state in job.arcjob.RestartState:
            if state.startswith("arcrest:"):
                restartState = state[len("arcrest:"):]

        if restartState in ("PREPARING", "FINISHING"):
            if "Error reading user generated output file list" not in resub:
                self.log.info(f"Will rerun {job.appid} {job.arcid}")
                patchDict.update({"State": "Undefined", "tstate": tstamp, "arcstate": "torerun"})
                return patchDict

        if resub:
            if job.attemptsLeft <= 0:
                self.log.info(f"Job {job.appid} {job.arcid} out of retries")
            else:
                job.attetmptsLeft -= 1
                self.log.info(f"Job {job.appid} {job.arcid} will be resubmitted, {job.attemptsLeft} attempts left")
                patchDict.update({"State": "Undefined", "tstate": tstamp, "arcstate": "toresubmit", "attemptsleft": job.attemptsLeft})

        return patchDict


    def checkACTStateTimeouts(self):
        COLUMNS = ["id", "appjobid"]

        for state, timeout in self.conf.timeouts.aCT_state:
            tstampCond = self.db.timeStampLessThan("tarcstate", timeout)
            jobs = self.db.getArcJobsInfo(f"cluster='{self.cluster}' and arcstate='{state}' and {tstampCond}", COLUMNS)
            for job in jobs:
                self.log.warning(f"Job {job['appjobid']} {job['id']} too long in \"{state}\"")
                tstamp = self.db.getTimeStamp()

                if state == "cancelling":
                    self.db.updateArcJob(job["id"], {"arcstate": "cancelled", "tarcstate": tstamp})

                if state == "finished":
                    self.db.updateArcJob(job["id"], {"arcstate": "tocancel", "tarcstate": tstamp})

        self.db.Commit()

    def checkARCStateTimeouts(self):
        COLUMNS = ["id", "appjobid", "arcstate", "State", "IDFromEndpoint"]

        for state, timeout in self.conf.timeouts.ARC_state:
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
                self.log.warning(f"Job {job['appjobid']} {job['id']} too long in state {state}")
                tstamp = self.db.getTimeStamp()
                if job["IDFromEndpoint"]:
                    self.db.updateArcJobLazy(job["id"], {"arcstate": "tocancel", "tarcstate": tstamp})
                else:
                    self.db.updateArcJobLazy(job["id"], {"arcstate": "cancelled", "tarcstate": tstamp})

        self.db.Commit()

    def process(self):
        self.checkJobs()
        self.checkARCStateTimeouts()
        self.checkACTStateTimeouts()


if __name__ == '__main__':
    st = aCTStatus()
    st.run()
    st.finish()
