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
#
#
#
# Notes:
#
# [1] There was a bug in ARC where some rare jobs didn't exist anymore but ARC
#     would still return info on them because some files were left and job
#     existence was not checked. Therefore, aCT had to check for every job if
#     it is still on the list.
#     TODO: check if bug has already been fixed and if not, put the reference
#     to the bug here.


import json
import os
import time
from datetime import datetime, timedelta
from http.client import HTTPException
from ssl import SSLError

from act.arc.aCTARCProcess import aCTARCProcess
from act.common.aCTJob import ACTJob
from pyarcrest.arc import ARCRest
from pyarcrest.errors import ARCError, ARCHTTPError

ARC_STATE_MAPPING = {
    "ACCEPTING": "Accepted",
    "Accepted": "Accepted",
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


class aCTStatus(aCTARCProcess):
    '''
    Class for checking the status of submitted ARC jobs and updating their
    status in the DB.
    '''

    def setup(self):
        super().setup()
        # store the last checkJobs time to avoid overloading of GIIS
        self.checktime = time.time()

    # TODO: refactor to some library aCT job operation
    def checkJobs(self):
        """
        Update the info of all running jobs.

        Signal handling strategy:
        - termination is checked before updating every jobs
        """
        COLUMNS = ["id", "appjobid", "proxyid", "IDFromEndpoint", "created",
                   "State", "attemptsleft", "tstate"]

        # check jobs which were last checked more than checkinterval ago
        # TODO: HARDCODED
        tstampCond = self.db.timeStampLessThan("tarcstate", self.conf.jobs.checkinterval)
        jobstocheck = self.db.getArcJobsInfo(
            "arcstate in ('submitted', 'running', 'finishing', "
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

            try:
                arcrest = ARCRest.getClient(self.cluster, proxypath=proxypath, logger=self.log, version="1.0")
            except Exception as exc:
                self.log.error(f"Cannot create REST client for proxyid {proxyid}: {exc}")
                continue

            # For now, if the list cannot be fetched, the empty list is
            # returned, which will cause jobs stuck for a long time to be
            # deleted.
            try:
                joblist = {job["id"] for job in arcrest.getJobsList()}  # set type for performance
            except Exception as exc:
                self.log.warning(f"Cannot fetch a list of jobs for proxyid {proxyid}: {exc}")
                joblist = []

            try:
                arcrest.getJobsInfo(tocheck)
            except json.JSONDecodeError as exc:
                self.log.error(f"Error parsing returned JSON document: {exc.doc}")
                continue
            except (HTTPException, ConnectionError, SSLError, ARCError, ARCHTTPError, TimeoutError, OSError, ValueError) as e:
                self.log.error(f"Error fetching job info from ARC: {e}")
                continue
            finally:
                arcrest.close()

            tstamp = self.db.getTimeStamp()

            for job in jobs:

                if self.mustExit:
                    self.log.info(f"Exiting early due to requested shutdown")
                    self.stopWithException()

                arcjob = job.arcjob
                jobdict = {}

                # cancel jobs that are stuck in tstate and not in job list anymore [1]
                # TODO: HARDCODED
                if job.tstate + timedelta(days=7) < datetime.utcnow():
                    if arcjob.id not in joblist:
                        self.log.error(f"Job {job.appid} {job.arcid} not in ARC anymore, cancelling")
                        jobdict.update({"arcstate": "tocancel", "tarcstate": tstamp})
                        self.db.updateArcJob(job.arcid, jobdict)
                        continue

                # cancel 404 jobs and log errors
                if arcjob.errors:
                    cancelled = False
                    for error in arcjob.errors:
                        if isinstance(error, ARCHTTPError):
                            if error.status == 404:
                                self.log.error(f"Job {job.appid} {job.arcid} not found, cancelling")
                                jobdict.update({"arcstate": "tocancel", "tarcstate": tstamp})
                                self.db.updateArcJob(job.arcid, jobdict)
                                cancelled = True
                                continue
                        self.log.error(f"Error for job {job.appid} {job.arcid}: {error}")
                    if cancelled:
                        continue

                # process state change
                try:
                    mappedState = ARC_STATE_MAPPING[arcjob.state]
                except KeyError:
                    self.log.debug(f"STATE MAPPING KEY ERROR: state: {arcjob.state}")
                    self.db.updateArcJob(job.arcid, jobdict)
                    continue

                # update and continue early when no state change
                if oldStates[job.arcid] == mappedState:
                    jobdict["tarcstate"] = tstamp
                    self.db.updateArcJob(job.arcid, jobdict)
                    continue

                self.log.info(f"ARC status change for job {job.appid}: {oldStates[job.arcid]} -> {mappedState}")

                jobdict.update({"State": mappedState, "tstate": tstamp})

                if arcjob.state in ("ACCEPTING", "ACCEPTED", "PREPARING", "PREPARED", "SUBMITTING", "QUEUING"):
                    jobdict["arcstate"] = "submitted"

                elif arcjob.state in ("RUNNING", "EXITINGLRMS", "EXECUTED"):
                    jobdict["arcstate"] = "running"

                elif arcjob.state == "HELD":
                    jobdict["arcstate"] = "holding"

                elif arcjob.state == "FINISHING":
                    jobdict["arcstate"] = "finishing"

                elif arcjob.state == "FINISHED":
                    if arcjob.ExitCode is None:
                        # missing exit code, but assume success
                        self.log.warning(f"Job {job.appid} is finished but has missing exit code, setting to zero")
                        jobdict["ExitCode"] = 0
                    else:
                        jobdict["ExitCode"] = arcjob.ExitCode
                    jobdict["arcstate"] = "finished"

                elif arcjob.state == "FAILED":
                    patchDict = self.processJobErrors(job)
                    jobdict.update(patchDict)

                elif arcjob.state == "KILLED":
                    jobdict["arcstate"] = "cancelled"

                elif arcjob.state == "WIPED":
                    jobdict["arcstate"] = "cancelled"

                jobdict["tarcstate"] = tstamp

                # Add available job info to dict

                # difference of two datetime objects yields timedelta object
                # with seconds attribute
                fromCreated = int((datetime.utcnow() - job.tcreated).total_seconds()) // 60

                if arcjob.UsedTotalWallTime and arcjob.RequestedSlots is not None:
                    wallTime = arcjob.UsedTotalWallTime // arcjob.RequestedSlots
                    if wallTime > fromCreated:
                        self.log.warning(f"Job {job.appid}: Fixing reported walltime {wallTime} to {fromCreated}")
                        jobdict["UsedTotalWallTime"] = fromCreated
                    else:
                        jobdict["UsedTotalWallTime"] = wallTime
                else:
                    self.log.warning(f"Job {job.appid}: No reported walltime, using DB timestamps: {fromCreated}")
                    jobdict["UsedTotalWallTime"] = fromCreated

                if arcjob.UsedTotalCPUTime:
                    # TODO: HARDCODED
                    if arcjob.UsedTotalCPUTime > 10 ** 7:
                        self.log.warning(f"Job {job.appid}: Discarding reported CPUtime {arcjob.UsedTotalCPUTime}")
                        jobdict["UsedTotalCPUTime"] = -1
                    else:
                        jobdict["UsedTotalCPUTime"] = arcjob.UsedTotalCPUTime

                if arcjob.Type:
                    jobdict["Type"] = arcjob.Type
                if arcjob.LocalIDFromManager:
                    jobdict["LocalIDFromManager"] = arcjob.LocalIDFromManager
                if arcjob.WaitingPosition:
                    jobdict["WaitingPosition"] = arcjob.WaitingPosition
                if arcjob.Owner:
                    jobdict["Owner"] = arcjob.Owner
                if arcjob.LocalOwner:
                    jobdict["LocalOwner"] = arcjob.LocalOwner
                if arcjob.RequestedTotalCPUTime:
                    jobdict["RequestedTotalCPUTime"] = arcjob.RequestedTotalCPUTime
                if arcjob.RequestedSlots:
                    jobdict["RequestedSlots"] = arcjob.RequestedSlots
                if arcjob.StdIn:
                    jobdict["StdIn"] = arcjob.StdIn
                if arcjob.StdOut:
                    jobdict["StdOut"] = arcjob.StdOut
                if arcjob.StdErr:
                    jobdict["StdErr"] = arcjob.StdErr
                if arcjob.LogDir:
                    jobdict["LogDir"] = arcjob.LogDir
                if arcjob.ExecutionNode:
                    jobdict["ExecutionNode"] = ",".join(arcjob.ExecutionNode)
                if arcjob.Queue:
                    jobdict["Queue"] = arcjob.Queue
                if arcjob.UsedMainMemory:
                    jobdict["UsedMainMemory"] = arcjob.UsedMainMemory
                if arcjob.SubmissionTime:
                    jobdict["SubmissionTime"] = arcjob.SubmissionTime
                if arcjob.EndTime:
                    jobdict["EndTime"] = arcjob.EndTime
                if arcjob.WorkingAreaEraseTime:
                    jobdict["WorkingAreaEraseTime"] = arcjob.WorkingAreaEraseTime
                if arcjob.ProxyExpirationTime:
                    jobdict["ProxyExpirationTime"] = arcjob.ProxyExpirationTime
                if arcjob.Error:
                    jobdict["Error"] = ";".join(arcjob.Error)

                # AF BUG
                try:
                    self.db.updateArcJob(job.arcid, jobdict)
                except:
                    self.log.error(f"Bad dict for job {job.appid} {job.arcid}: {jobdict}")

        self.log.info('Done')

    # Returns a dictionary reflecting job changes that should update the
    # jobdict for DB
    def processJobErrors(self, job):
        """Handle failed jobs."""
        tstamp = self.db.getTimeStamp()
        patchDict = {"arcstate": "failed", "tarcstate": tstamp}

        # a list of job errors for job should be resubmission
        resub = []
        if job.arcjob.Error:
            errors = ";".join(job.arcjob.Error)
            resub = [err for err in self.conf.errors.toresubmit.arcerrors if err in errors]
            self.log.info(f"Job {job.appid} {job.arcid} failed with error: {errors}")
        else:
            self.log.info(f"Job {job.appid} {job.arcid} failed, no error given")

        # restart if data staging problem but not output file list problem
        restartState = ""
        for state in job.arcjob.RestartState:
            if state.startswith("arcrest:"):
                restartState = state[len("arcrest:"):]
        if restartState in ("PREPARING", "FINISHING"):
            if "Error reading user generated output file list" not in job.arcjob.Error:
                self.log.info(f"Will rerun {job.appid} {job.arcid}")
                patchDict.update({"State": "Undefined", "tstate": tstamp, "arcstate": "torerun"})

        # resubmit if certain errors
        elif resub:
            if job.attemptsleft <= 0:
                self.log.info(f"Job {job.appid} {job.arcid} out of retries")
            else:
                job.attetmptsLeft -= 1
                self.log.info(f"Job {job.appid} {job.arcid} will be resubmitted, {job.attemptsleft} attempts left")
                patchDict.update({"State": "Undefined", "tstate": tstamp, "arcstate": "toresubmit", "attemptsleft": job.attemptsleft})

        return patchDict

    # TODO: refactor to some library aCT job operation
    def checkLostJobs(self):
        """
        Move jobs with a long time since status update to lost

        Signal handling strategy:
        - termination is checked before updating every job
        """
        # TODO: HARDCODED
        jobs = self.db.getArcJobsInfo(
            "(arcstate='submitted' or arcstate='running' or "
            "arcstate='cancelling' or arcstate='finished') and "
            f"cluster='{self.cluster}' and "
            f"{self.db.timeStampLessThan('tarcstate', 172800)}",
            ['id', 'appjobid', 'JobID', 'arcstate']
        )
        tstamp = self.db.getTimeStamp()
        for job in jobs:
            if self.mustExit:
                self.log.info(f"Exiting early due to requested shutdown")
                self.stopWithException()
            if job['arcstate'] == 'cancelling':
                self.log.warning(f"Job {job['appjobid']} {job['id']} too long in cancelling, marking as cancelled")
                self.db.updateArcJob(job['id'], {'arcstate': 'cancelled', 'tarcstate': tstamp})
            else:
                self.log.warning(f"Job {job['appjobid']} {job['id']} too long in {job['arcstate']}, marking as lost")
                self.db.updateArcJob(job['id'], {'arcstate': 'lost', 'tarcstate': tstamp})

    # TODO: refactor to some library aCT job operation
    def checkStuckJobs(self):
        """
        Check jobs with tstate too long ago and set them tocancel
        maxtimestate can be set in arc config file for any state in arc.JobState,
        e.g. maxtimerunning, maxtimequeuing
        Also mark as cancelled jobs stuck in cancelling for more than one hour

        Signal handling strategy:
        - termination is checked before handling every job
        """
        tstamp = self.db.getTimeStamp()

        # Loop over possible states
        # Note: MySQL is case-insensitive. Need to watch out with other DBs

        # Some states are repeated in mapping so set is used.
        for jobstate in set(ARC_STATE_MAPPING.values()):
            maxtime = self.conf.jobs.get(f"maxtime{jobstate.lower()}", None)
            if not maxtime:
                continue

            # be careful not to cancel jobs that are stuck in cleaning
            select = f"state='{jobstate}' and {self.db.timeStampLessThan('tstate', maxtime)}"
            jobs = self.db.getArcJobsInfo(select, columns=['id', 'JobID', 'appjobid', 'arcstate'])

            for job in jobs:
                if self.mustExit:
                    self.log.info(f"Exiting early due to requested shutdown")
                    self.stopWithException()
                if job["arcstate"] == "toclean":
                    # delete jobs stuck in toclean
                    self.log.info(f"Job {job['appjobid']} {job['id']} stuck in toclean for too long, deleting")
                    self.db.deleteArcJob(job["id"])
                else:
                    # cancel other stuck jobs
                    self.log.warning(f"Job {job['appjobid']} {job['id']} too long in state {jobstate}, cancelling")
                    if job["JobID"]:
                        # if jobid is defined, cancel
                        self.db.updateArcJob(job["id"], {"arcstate": "tocancel", "tarcstate": tstamp, "tstate": tstamp})
                    else:
                        # otherwise mark cancelled
                        self.db.updateArcJob(job["id"], {"arcstate": "cancelled", "tarcstate": tstamp, "tstate": tstamp})

    # TODO: refactor to some library aCT job operation
    # TODO: is the comprehensive update of jobs from info required? (return
    #       value, walltimes, etc. can be done on library refactor)
    def checkCancellingJobs(self):
        """
        Update jobs that are in cancelling state.

        Signal handling strategy:
        - termination is checked before updating every job
        """
        COLUMNS = ["id", "appjobid", "proxyid", "IDFromEndpoint", "tarcstate", "created"]

        # check jobs which were last checked more than checkinterval ago
        # TODO: HARDCODED
        tstampCond = self.db.timeStampLessThan("tarcstate", self.conf.jobs.checkinterval)
        jobstocheck = self.db.getArcJobsInfo(
            f"arcstate = 'cancelling' and jobid not like '' and cluster="
            f"'{self.cluster}' and {tstampCond} limit 100000",
            COLUMNS
        )
        if not jobstocheck:
            return

        self.log.info(f"Checking {len(jobstocheck)} jobs in cancelling state")

        # aggregate jobs by proxyid
        jobsdict = {}
        for row in jobstocheck:
            if not row["proxyid"] in jobsdict:
                jobsdict[row["proxyid"]] = []
            jobsdict[row["proxyid"]].append(row)

        for proxyid, dbjobs in jobsdict.items():
            # construct job objects from DB rows
            totimeout = []
            for dbjob in dbjobs:
                job = ACTJob()
                job.loadARCDBJob(dbjob)
                totimeout.append(job)

            tstamp = self.db.getTimeStamp()

            # jobs too long in cancelling are considered to be cancelled
            tocheck = []
            toGetInfo = []
            for job in totimeout:
                if not job.tarcstate:
                    job.tarcstate = job.tcreated
                if job.tarcstate + timedelta(seconds=3600) < datetime.utcnow():
                    self.log.error(f"Job {job.appid} {job.arcid} stuck in cancelling, setting to cancelled")
                    self.db.updateArcJob(job.arcid, {"arcstate": "cancelled", "tarcstate": tstamp})
                else:
                    tocheck.append(job)
                    toGetInfo.append(job.arcjob)

            # get job info from ARC
            proxypath = os.path.join(self.db.proxydir, f"proxiesid{proxyid}")
            arcrest = None
            try:
                arcrest = ARCRest.getClient(self.cluster, proxypath=proxypath, logger=self.log, version="1.0")
                joblist = {job["id"] for job in arcrest.getJobsList()}  # set type for performance
                arcrest.getJobsInfo(toGetInfo)
            except json.JSONDecodeError as exc:
                self.log.error(f"Error parsing returned JSON document: {exc.doc}")
                continue
            except (HTTPException, ConnectionError, SSLError, ARCError, ARCHTTPError, TimeoutError, OSError, ValueError) as e:
                self.log.error(f"Error fetching job info from ARC: {e}")
                continue
            finally:
                if arcrest:
                    arcrest.close()

            # update DB
            for job in tocheck:

                if self.mustExit:
                    self.log.info(f"Exiting early due to requested shutdown")
                    self.stopWithException()

                arcjob = job.arcjob

                # jobs that are not on the list anymore are considered to
                # be cancelled [1]
                if arcjob.id not in joblist:
                    self.log.error(f"Job {job.appid} {job.arcid} not in ARC job list anymore, setting to cancelled")
                    self.db.updateArcJob(job.arcid, {"arcstate": "cancelled", "tarcstate": tstamp})
                    continue

                # set 404 jobs to cancelled
                if arcjob.errors:
                    cancelled = False
                    for error in arcjob.errors:
                        if isinstance(error, ARCHTTPError):
                            if error.status == 404:
                                self.log.error(f"Job {job.appid} {job.arcid} not found in ARC, setting to cancelled")
                                self.db.updateArcJob(job.arcid, {"arcstate": "cancelled", "tarcstate": tstamp})
                                cancelled = True
                                continue
                        self.log.error(f"Error for job {job.appid} {job.arcid}: {error}")
                    if cancelled:
                        continue

                # set to cancelled if in terminal state
                try:
                    mappedState = ARC_STATE_MAPPING[arcjob.state]
                except KeyError:
                    self.log.debug(f"STATE MAPPING KEY ERROR: state: {arcjob.state}")
                    continue
                if mappedState in ("Finished", "Failed", "Killed", "Deleted"):
                    self.log.debug(f"Job {job.appid} {job.arcid} is cancelled by ARC")
                    self.db.updateArcJob(job.arcid, {"arcstate": "cancelled", "tarcstate": tstamp, "State": mappedState, "tstate": tstamp})

        self.log.info('Done')

    def process(self):
        # minimum time between checks
        if time.time() < self.checktime + self.conf.jobs.checkmintime:
            self.log.debug("mininterval not reached")
            return
        self.checktime = time.time()

        self.checkJobs()
        self.checkCancellingJobs()
        self.checkLostJobs()
        self.checkStuckJobs()
