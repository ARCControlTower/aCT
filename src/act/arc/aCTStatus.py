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
            arcids = [dbjob["IDFromEndpoint"] for dbjob in dbjobs]

            proxypath = os.path.join(self.db.proxydir, f"proxiesid{proxyid}")
            try:
                arcrest = ARCRest.getClient(self.cluster, proxypath=proxypath, logger=self.log)
            except Exception as exc:
                self.log.error(f"Cannot create REST client for proxyid {proxyid}: {exc}")
                continue

            # For now, if the job list cannot be fetched, the empty set is
            # returned, which will cause jobs stuck for a long time to be
            # deleted. Set type is used for performance as we can be working
            # with a lot of jobs (limit in SQL statement).
            try:
                joblist = set(arcrest.getJobsList())
            except Exception as exc:
                self.log.warning(f"Cannot fetch a list of jobs for proxyid {proxyid}: {exc}")
                joblist = set()

            # fetch jobs' info from ARC
            try:
                results = arcrest.getJobsInfo(arcids)
            except json.JSONDecodeError as exc:
                self.log.error(f"Error parsing returned JSON document: {exc.doc}")
                continue
            except (HTTPException, ConnectionError, SSLError, ARCError, ARCHTTPError, TimeoutError, OSError, ValueError) as e:
                self.log.error(f"Error fetching job info from ARC: {e}")
                continue
            finally:
                arcrest.close()

            tstamp = self.db.getTimeStamp()

            for job, result in zip(dbjobs, results):

                if self.mustExit:
                    self.log.info("Exiting early due to requested shutdown")
                    self.stopWithException()

                jobdict = {"tarcstate": tstamp}

                # cancel jobs that are stuck in tstate and not in job list anymore [1]
                # TODO: HARDCODED
                if job["tstate"] + timedelta(days=7) < datetime.utcnow():
                    if job["IDFromEndpoint"] not in joblist:
                        self.log.error(f"Job {job['appjobid']} not in ARC anymore, cancelling")
                        jobdict.update({"arcstate": "tocancel"})
                        self.db.updateArcJob(job["id"], jobdict)
                        continue

                # cancel 404 jobs and log errors
                if isinstance(result, ARCError):
                    if isinstance(result, ARCHTTPError):
                        if result.status == 404:
                            self.log.error(f"Job {job['appjobid']} not found, cancelling")
                            jobdict.update({"arcstate": "tocancel"})
                            self.db.updateArcJob(job["id"], jobdict)
                            continue
                    self.log.error(f"Error fetching info for job {job['appjobid']}: {result}")
                    continue

                jobInfo = result

                # process state change
                state = jobInfo.get("State", None)
                if state:
                    try:
                        mappedState = ARC_STATE_MAPPING[state]
                    except KeyError:
                        self.log.debug(f"No state mapping for state {state}")
                    else:
                        if job["State"] != mappedState:

                            self.log.info(f"ARC status change for job {job['appjobid']}: {job['State']} -> {mappedState}")
                            jobdict.update({"State": mappedState, "tstate": tstamp})

                            if state in ("ACCEPTING", "ACCEPTED", "PREPARING", "PREPARED", "SUBMITTING", "QUEUING"):
                                jobdict["arcstate"] = "submitted"

                            elif state in ("RUNNING", "EXITINGLRMS", "EXECUTED"):
                                jobdict["arcstate"] = "running"

                            elif state == "HELD":
                                jobdict["arcstate"] = "holding"

                            elif state == "FINISHING":
                                jobdict["arcstate"] = "finishing"

                            elif state == "FINISHED":
                                if jobInfo["ExitCode"] is None:
                                    # missing exit code, but assume success
                                    self.log.warning(f"Job {job['appjobid']} is finished but has missing exit code, setting to zero")
                                    jobdict["ExitCode"] = 0
                                else:
                                    jobdict["ExitCode"] = jobInfo["ExitCode"]
                                jobdict["arcstate"] = "finished"

                            elif state == "FAILED":
                                patchDict = self.processJobErrors(job, jobInfo)
                                jobdict.update(patchDict)

                            elif state == "KILLED":
                                jobdict["arcstate"] = "cancelled"

                            elif state == "WIPED":
                                jobdict["arcstate"] = "cancelled"

                # Add available job info to dict

                # difference of two datetime objects yields timedelta object
                # with seconds attribute
                fromCreated = int((datetime.utcnow() - job["created"]).total_seconds()) // 60

                # calculate proper wall time and fix wrongly reported one
                walltime = jobInfo.get("UsedTotalWallTime", None)
                slots = jobInfo.get("RequestedSlots", None)
                if walltime and slots:
                    time = walltime // slots
                    if time > fromCreated:
                        self.log.warning(f"Job {job['appjobid']}: Fixing reported walltime {time} to {fromCreated}")
                        jobdict["UsedTotalWallTime"] = fromCreated
                    else:
                        jobdict["UsedTotalWallTime"] = time
                else:
                    self.log.warning(f"Job {job['appjobid']}: No reported walltime, using DB timestamps: {fromCreated}")
                    jobdict["UsedTotalWallTime"] = fromCreated

                # fix wrongly reported cpu time
                cputime = jobInfo.get("UsedTotalCPUTime", None)
                if cputime:
                    # TODO: HARDCODED
                    if cputime > 10 ** 7:
                        self.log.warning(f"Discarding reported CPU time {cputime} for job {job['appjobid']}")
                        jobdict["UsedTotalCPUTime"] = -1
                    else:
                        jobdict["UsedTotalCPUTime"] = cputime

                # format data properly for DB
                if "ExecutionNode" in jobInfo:
                    jobdict["ExecutionNode"] = ",".join(jobInfo["ExecutionNode"])
                if "Error" in jobInfo:
                    jobdict["Error"] = ";".join(jobInfo["Error"])

                # copy values that can be copied directly
                COPY_KEYS = [
                    "Type", "LocalIDFromManager", "WaitingPosition", "Owner",
                    "LocalOwner", "RequestedTotalCPUTime", "RequestedSlots",
                    "StdIn", "StdOut", "StdErr", "LogDir", "Queue",
                    "UsedMainMemory", "SubmissionTime", "EndTime",
                    "WorkingAreaEraseTime", "ProxyExpirationTime"
                ]
                for key in COPY_KEYS:
                    if key in jobInfo:
                        jobdict[key] = jobInfo[key]

                try:
                    self.db.updateArcJob(job["id"], jobdict)
                except:
                    self.log.error(f"Bad dict for job {job['appjobid']}: {jobdict}")

        self.log.info("Done")

    # Returns a dictionary reflecting job changes that should update the
    # jobdict for DB
    def processJobErrors(self, job, jobInfo):
        """Handle failed jobs."""
        patchDict = {"arcstate": "failed"}

        # a list of job errors for job should be resubmission
        resub = []
        if "Error" in jobInfo:
            errors = ";".join(jobInfo["Error"])
            resub = [err for err in self.conf.errors.toresubmit.arcerrors if err in errors]
            self.log.info(f"Job {job['appjobid']} failed with error: {errors}")
        else:
            self.log.info(f"Job {job['appjobid']} failed, no error given")

        # restart if data staging problem but not output file list problem
        restartState = ""
        for state in jobInfo.get("RestartState", []):
            if state.startswith("arcrest:"):
                restartState = state[len("arcrest:"):]
        if restartState in ("PREPARING", "FINISHING"):
            if "Error reading user generated output file list" not in jobInfo.get("Error", []):
                self.log.info(f"Will rerun {job['appjobid']}")
                patchDict.update({"State": "Undefined", "arcstate": "torerun"})

        # resubmit if certain errors
        elif resub:
            if job["attemptsleft"] <= 0:
                self.log.info(f"Job {job['appjobid']} out of retries")
            else:
                attempts = job["attemptsleft"] - 1
                self.log.info(f"Job {job['appjobid']} will be resubmitted, {attempts} attempts left")
                patchDict.update({"State": "Undefined", "arcstate": "toresubmit", "attemptsleft": attempts})

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
            ["id", "appjobid", "JobID", "arcstate"]
        )
        tstamp = self.db.getTimeStamp()
        for job in jobs:
            if self.mustExit:
                self.log.info("Exiting early due to requested shutdown")
                self.stopWithException()
            if job["arcstate"] == "cancelling":
                self.log.warning(f"Job {job['appjobid']} too long in cancelling, marking as cancelled")
                self.db.updateArcJob(job["id"], {"arcstate": "cancelled", "tarcstate": tstamp})
            else:
                self.log.warning(f"Job {job['appjobid']} too long in {job['arcstate']}, marking as lost")
                self.db.updateArcJob(job["id"], {"arcstate": "lost", "tarcstate": tstamp})

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
            jobs = self.db.getArcJobsInfo(select, columns=["id", "JobID", "appjobid", "arcstate"])

            for job in jobs:
                if self.mustExit:
                    self.log.info("Exiting early due to requested shutdown")
                    self.stopWithException()
                if job["arcstate"] == "toclean":
                    # delete jobs stuck in toclean
                    self.log.info(f"Job {job['appjobid']} stuck in toclean for too long, deleting")
                    self.db.deleteArcJob(job["id"])
                else:
                    # cancel other stuck jobs
                    self.log.warning(f"Job {job['appjobid']} too long in state {jobstate}, cancelling")
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
            tstamp = self.db.getTimeStamp()

            # jobs too long in cancelling are considered to be cancelled
            tocheck = []
            for job in dbjobs:
                if not job["tarcstate"]:
                    job["tarcstate"] = job["tcreated"]
                if job["tarcstate"] + timedelta(seconds=3600) < datetime.utcnow():
                    self.log.error(f"Job {job['appjobid']} stuck in cancelling, setting to cancelled")
                    self.db.updateArcJob(job["id"], {"arcstate": "cancelled", "tarcstate": tstamp})
                else:
                    tocheck.append(job)

            # get job info from ARC
            proxypath = os.path.join(self.db.proxydir, f"proxiesid{proxyid}")
            arcrest = None
            try:
                arcrest = ARCRest.getClient(self.cluster, proxypath=proxypath, logger=self.log)
                joblist = set(arcrest.getJobsList())  # set type for performance
                results = arcrest.getJobsInfo([job["IDFromEndpoint"] for job in tocheck])
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
            for job, result in zip(tocheck, results):

                if self.mustExit:
                    self.log.info("Exiting early due to requested shutdown")
                    self.stopWithException()

                # jobs that are not on the list anymore are considered to
                # be cancelled [1]
                if job["id"] not in joblist:
                    self.log.error(f"Job {job['appjobid']} not in ARC job list anymore, setting to cancelled")
                    self.db.updateArcJob(job["id"], {"arcstate": "cancelled", "tarcstate": tstamp})
                    continue

                # cancel 404 jobs and log errors
                if isinstance(result, ARCError):
                    if isinstance(result, ARCHTTPError):
                        if result.status == 404:
                            self.log.error(f"Job {job['appjobid']} not found, cancelling")
                            self.db.updateArcJob(job["id"], {"arcstate": "tocancel", "tarcstate": tstamp})
                            continue
                    self.log.error(f"Error fetching info for job {job['appjobid']}: {result}")
                    continue

                # set to cancelled if in terminal state
                state = result.get("State", None)
                if state:
                    try:
                        mappedState = ARC_STATE_MAPPING[state]
                    except KeyError:
                        self.log.debug(f"STATE MAPPING ERROR: state: {state}")
                    else:
                        if mappedState in ("Finished", "Failed", "Killed", "Deleted"):
                            self.log.debug(f"Job {job['appjobid']} is cancelled by ARC")
                            self.db.updateArcJob(job["id"], {"arcstate": "cancelled", "tarcstate": tstamp, "State": mappedState, "tstate": tstamp})

        self.log.info("Done")

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
