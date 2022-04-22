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


import os
import time
from datetime import datetime
from http.client import HTTPException
from ssl import SSLError
from urllib.parse import urlparse

from act.arc.rest import RESTClient
from act.common.aCTProcess import aCTProcess
from act.common.config import Config
from act.common.exceptions import ACTError, ARCHTTPError

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
                   "State", "attemptsleft"]

        # parse cluster URL
        try:
            url = urlparse(self.cluster)
        except ValueError as exc:
            self.log.error(f"Error parsing cluster URL {url}: {exc}")
            return

        # minimum time between checks
        if time.time() < self.checktime + int(self.conf.get(['jobs', 'checkmintime'])):
            self.log.debug("mininterval not reached")
            return
        self.checktime = time.time()

        # check jobs which were last checked more than checkinterval ago
        # TODO: hardcoded
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
                    "created": job["created"],
                    "errors": [],
                }
                tocheck.append(jobdict)

            proxypath = os.path.join(self.db.proxydir, f"proxiesid{proxyid}")

            try:
                restClient = RESTClient(url.hostname, port=url.port, proxypath=proxypath)
                tocheck = restClient.getJobsInfo(tocheck)
            except (HTTPException, ConnectionError, SSLError, ACTError, ARCHTTPError) as e:
                self.log.error(f"Error fetching job info from ARC: {e}")
                continue
            finally:
                restClient.close()

            # process jobs and update DB
            for job in tocheck:
                if job["errors"]:
                    for error in job["errors"]:
                        self.log.error(f"Error checking {job['appjobid']} {job['id']}: {error}")
                    continue

                # get activity dictionary from document
                activityDict = job["info_document"].get("ComputingActivity", {})

                # get state from a list of activity states in different systems
                restState = ""
                for state in activityDict.get("State", []):
                    if state.startswith("arcrest:"):
                        restState = state[len("arcrest:"):]
                if not restState:
                    continue

                mappedState = ARC_STATE_MAPPING[restState]
                if job["State"] == mappedState:
                    continue

                self.log.info(f"ARC status change for job {job['appjobid']}: {job['State']} -> {mappedState}")

                tstamp = self.db.getTimeStamp()
                jobdict = {"State": mappedState, "tstate": tstamp}

                if restState in ("ACCEPTING", "ACCEPTED", "PREPARING",
                                    "PREPARED", "SUBMITTING", "QUEUING"):
                    jobdict["arcstate"] = "submitted"

                elif restState in ("RUNNING", "EXITINGLRMS", "EXECUTED"):
                    jobdict["arcstate"] = "running"

                elif restState == "HELD":
                    jobdict["arcstate"] = "holding"

                elif restState == "FINISHING":
                    jobdict["arcstate"] = "finishing"

                elif restState == "FINISHED":
                    exitCode = activityDict.get("ExitCode", -1)
                    if exitCode == -1:
                        # missing exit code, but assume success
                        self.log.warning(f"Job {job['appjobid']} is finished but has missing exit code, setting to zero")
                        jobdict["ExitCode"] = 0
                    else:
                        jobdict["ExitCode"] = exitCode
                    jobdict["arcstate"] = "finished"

                elif restState == "FAILED":
                    jobdict["arcstate"] = "failed"
                    patchDict = self.processJobErrors(job, activityDict)
                    jobdict.update(patchDict)

                elif restState == "KILLED":
                    jobdict["arcstate"] = "cancelled"

                elif restState == "WIPED":
                    jobdict["arcstate"] = "cancelled"

                if "arcstate" in jobdict:
                    jobdict["tarcstate"] = tstamp

                # difference of two datetime objects yields timedelta object
                # with seconds attribute
                fromCreated = (datetime.utcnow() - job["created"]).seconds // 60

                if "UsedTotalWallTime" in activityDict and "RequestedSlots" in activityDict:
                    wallTime = int(activityDict["UsedTotalWallTime"])
                    slots = int(activityDict["RequestedSlots"])
                    wallTime = wallTime // slots
                    if wallTime > fromCreated:
                        self.log.warning(f"Job {job['appjobid']}: Fixing reported walltime {wallTime} to {fromCreated}")
                        jobdict["UsedTotalWallTime"] = fromCreated
                    else:
                        jobdict["UsedTotalWallTime"] = wallTime
                else:
                    self.log.warning(f"Job {job['appjobid']}: No reported walltime, using DB timestamps: {fromCreated}")
                    jobdict["UsedTotalWallTime"] = fromCreated

                if "UsedTotalCPUTime" in activityDict:
                    cpuTime = int(activityDict["UsedTotalCPUTime"])
                    if cpuTime and cpuTime > 10 ** 7:
                        self.log.warning(f"Job {job['appjobid']}: Discarding reported CPUtime {cpuTime}")
                        jobdict["UsedTotalCPUTime"] = -1
                    else:
                        jobdict["UsedTotalCPUTime"] = cpuTime

                if "Type" in activityDict:
                    jobdict["Type"] = activityDict["Type"]
                if "LocalIDFromManager" in activityDict:
                    jobdict["LocalIDFromManager"] = activityDict["LocalIDFromManager"]
                if "WaitingPosition" in activityDict:
                    jobdict["WaitingPosition"] = int(activityDict["WaitingPosition"])
                if "Owner" in activityDict:
                    jobdict["Owner"] = activityDict["Owner"]
                if "LocalOwner" in activityDict:
                    jobdict["LocalOwner"] = activityDict["LocalOwner"]
                if "RequestedTotalCPUTime" in activityDict:
                    jobdict["RequestedTotalCPUTime"] = int(activityDict["RequestedTotalCPUTime"])
                if "RequestedSlots" in activityDict:
                    jobdict["RequestedSlots"] = int(activityDict["RequestedSlots"])
                if "StdIn" in activityDict:
                    jobdict["StdIn"] = activityDict["StdIn"]
                if "StdOut" in activityDict:
                    jobdict["StdOut"] = activityDict["StdOut"]
                if "StdErr" in activityDict:
                    jobdict["StdErr"] = activityDict["StdErr"]
                if "LogDir" in activityDict:
                    jobdict["LogDir"] = activityDict["LogDir"]
                if "ExecutionNode" in activityDict:
                    jobdict["ExecutionNode"] = activityDict["ExecutionNode"]
                if "Queue" in activityDict:
                    jobdict["Queue"] = activityDict["Queue"]
                if "Error" in activityDict:
                    jobdict["Error"] = activityDict["Error"]
                if "UsedMainMemory" in activityDict:
                    jobdict["UsedMainMemory"] = int(activityDict["UsedMainMemory"])
                if "SubmissionTime" in activityDict:
                    #jobdict["SubmissionTime"] = datetime.fromisoformat(activityDict["SubmissionTime"])
                    jobdict["SubmissionTime"] = datetime.strptime(activityDict["SubmissionTime"], "%Y-%m-%dT%H:%M:%SZ")
                if "EndTime" in activityDict:
                    #jobdict["EndTime"] = datetime.fromisoformat(activityDict["EndTime"])
                    jobdict["EndTime"] = datetime.strptime(activityDict["EndTime"], "%Y-%m-%dT%H:%M:%SZ")
                if "WorkingAreaEraseTime" in activityDict:
                    #jobdict["WorkingAreaEraseTime"] = datetime.fromisoformat(activityDict["WorkingAreaEraseTime"])
                    jobdict["WorkingAreaEraseTime"] = datetime.strptime(activityDict["WorkingAreaEraseTime"], "%Y-%m-%dT%H:%M:%SZ")
                if "ProxyExpirationTime" in activityDict:
                    #jobdict["ProxyExpirationTime"] = datetime.fromisoformat(activityDict["ProxyExpirationTime"])
                    jobdict["ProxyExpirationTime"] = datetime.strptime(activityDict["ProxyExpirationTime"], "%Y-%m-%dT%H:%M:%SZ")

                self.db.updateArcJobLazy(job["id"], jobdict)

            self.db.Commit()

        self.log.info('Done')

    # Returns a dictionary reflecting job changes that should update the final
    # jobdict for DB
    def processJobErrors(self, job, activityDict):
        if "Error" in activityDict:
            resub = [err for err in self.conf.getList(['errors', 'toresubmit', 'arcerrors', 'item']) if err in activityDict["Error"]]
            self.log.info(f"Job {job['appjobid']} {job['id']} failed with error: {activityDict['Error']}")
        else:
            self.log.info(f"Job {job['appjobid']} {job['id']} failed, no error given")

        tstamp = self.db.getTimeStamp()

        restartState = ""
        for state in activityDict.get("RestartState", []):
            if state.startswith("arcrest:"):
                restartState = state[len("arcrest:"):]
        if restartState in ("PREPARING", "FINISHING"):
            # TODO: original code does not restart if error is specifically:
            # "Error reading user generated output file list"
            self.log.info(f"Will rerun {job['appjobid']} {job['id']}")
            return {"State": "Undefined", "tstate": tstamp, "arcstate": "torerun", "tarcstate": tstamp}

        attemptsleft = int(job["attemptsleft"])
        if resub:
            if attemptsleft <= 0:
                self.log.info(f"Job {job['appjobid']} {job['id']} out of retries")
                return {"arcstate": "failed", "tarcstate": tstamp}
            attemptsleft -= 1
            self.log.info(f"Job {job['appjobid']} {job['id']} will be resubmitted, {attemptsleft} attempts left")
            return {"State": "Undefined", "tstate": tstamp, "arcstate": "toresubmit", "tarcstate": tstamp, "attemptsleft": attemptsleft}


    def checkACTStateTimeouts(self):
        COLUMNS = ["id", "appjobid"]

        config = Config()

        timeoutDict = config["timeouts"]["aCT_state"]

        # mark jobs stuck in cancelling as cancelled
        tstampCond = self.db.timeStampLessThan("tarcstate", timeoutDict["cancelling"])
        jobs = self.db.getArcJobsInfo(f"cluster='{self.cluster}' and arcstate='cancelling' and {tstampCond}", COLUMNS)
        for job in jobs:
            self.log.warning(f"Job {job['appjobid']} {job['id']} too long in \"cancelling\", setting to \"cancelled\"")
            tstamp = self.db.getTimeStamp()
            self.db.updateArcJob(job["id"], {"arcstate": "cancelled", "tarcstate": tstamp, 'tstate': tstamp})
        self.db.Commit()

    def checkARCStateTimeouts(self):
        COLUMNS = ["id", "appjobid", "arcstate", "State", "IDFromEndpoint"]

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
                self.log.warning(f"Job {job['appjobid']} too long in state {state}")
                tstamp = self.db.getTimeStamp()
                if job["IDFromEndpoint"]:
                    self.db.updateArcJobLazy(job["id"], {"arcstate": "tocancel", "tarcstate": tstamp, 'tstate': tstamp})
                else:
                    self.db.updateArcJobLazy(job["id"], {"arcstate": "cancelled", "tarcstate": tstamp, 'tstate': tstamp})

        self.db.Commit()

    def process(self):
        self.checkJobs()
        self.checkARCStateTimeouts()
        self.checkACTStateTimeouts()


if __name__ == '__main__':
    st = aCTStatus()
    st.run()
    st.finish()
