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


import http.client
import os
import ssl
import time
from datetime import datetime
from urllib.parse import urlparse

from act.arc.rest import getJobsInfo, getJobsStatus
from act.common.aCTProcess import aCTProcess
from act.common.config import Config
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

    def checkJobs(self):
        '''
        Query all running jobs
        '''
        COLUMNS = ["id", "appjobid", "proxyid", "IDFromEndpoint", "created",
                   "State"]

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
                        self.log.error(f"Failed to get info for job {job['appjobid']}: {job['msg']}")
                        continue

                    if job["State"] == job["state"]:
                        continue

                    self.log.info(f"ARC status change for job {job['appjobid']}: {job['State']} -> {job['state']}")

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
                        exitCode = job["info_document"].get("ComputingActivity", {}).get("ExitCode", -1)
                        if exitCode == -1:
                            # missing exit code, but assume success
                            self.log.warning(f"Job {job['appjobid']} FINISHED but has missing exit code, setting to zero")
                            jobdict["ExitCode"] = 0
                        else:
                            jobdict["ExitCode"] = exitCode
                        jobdict["arcstate"] = "finished"

                    elif job["state"] == "FAILED":
                        jobdict["arcstate"] = "failed"
                        self.log.info(f"Job {job['appjobid']} failed")

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
                            self.log.warning(f"Job {job['appjobid']}: Fixing reported walltime {wallTime} to {fromCreated}")
                            jobdict["UsedTotalWallTime"] = fromCreated
                    cpuTime = int(activityDict.get("UsedTotalCPUTime", 0))
                    if cpuTime and cpuTime > 10 ** 7:
                        self.log.warning(f"Job {job['appjobid']}: Discarding reported CPUtime {cpuTime}")
                        jobdict["UsedTotalCPUTime"] = -1

                    self.db.updateArcJobLazy(job["id"], jobdict)

                self.db.Commit()

            except ssl.SSLError as e:
                self.log.error(f"Could not create SSL context for proxy {proxypath}: {e}")
            except (http.client.HTTPException, ConnectionError) as e:
                self.log.error(f"Could not connect to cluster {url.netloc}: {e}")
            except ACTError as e:
                self.log.error(f"Error fetching info for ARC jobs: {e}")
            finally:
                if conn:
                    conn.close()

        self.log.info('Done')

    def checkACTStateTimeouts(self):
        COLUMNS = ["id", "appjobid"]

        config = Config()

        timeoutDict = config["timeouts"]["aCT_state"]

        # mark jobs stuck in cancelling as cancelled
        tstampCond = self.db.timeStampLessThan("tarcstate", timeoutDict["cancelling"])
        jobs = self.db.getArcJobsInfo(f"cluster='{self.cluster}' and arcstate='cancelling' and {tstampCond}", COLUMNS)
        for job in jobs:
            self.log.warning(f"Job {job['appjobid']} too long in \"cancelling\"")
            tstamp = self.db.getTimeStamp()
            self.db.updateArcJob(job["id"], {"arcstate": "cancelled", "tarcstate": tstamp, 'tstate': tstamp})
        self.db.Commit()

    def checkARCStateTimeouts(self):
        COLUMNS = ["id", "appjobid", "arcstate", "State", "JobID"]

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
                if job["JobID"]:
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
