import datetime

from act.atlas.aCTATLASProcess import aCTATLASProcess


class aCTATLASStatusCondor(aCTATLASProcess):
    """Checks the status of condor jobs and reports back to pandajobs."""

    def __init__(self):
        super().__init__(ceflavour=["HTCONDOR-CE", "CREAM-CE"])

    def checkJobstoKill(self):
        """
        Get starting jobs for offline sites and kill them.
        Check for jobs with pandastatus tobekilled and cancel them in Condor:
        - pandastatus NULL: job was killed by panda so nothing to report
        - pandastatus something else: job was manually killed, so create pickle
          and report failed back to panda

        Signal handling strategy:
        - exit is checked before updating every job
        """
        offlinesites = [site for site, a in self.sites.items() if a["status"] == "offline"]
        if offlinesites:
            siteStr = ",".join([f"'{site}'" for site in offlinesites])
            jobs = self.dbpanda.getJobs(f"(actpandastatus='starting' or actpandastatus='sent') and sitename in ({siteStr})",
                                        ["pandaid", "condorjobid", "siteName", "id"])
            for job in jobs:

                self.stopOnFlag()

                continue  # TODO: does this make sense?
                self.log.info(f"Cancelling starting job for appjob({job['pandaid']}) for offline site {job['siteName']}")
                select = f"id={job['id']}"
                self.dbpanda.updateJobs(select, {"actpandastatus": "failed", "pandastatus": "failed",
                                                    "error": "Starting job was killed because queue went offline"})
                if job["condorjobid"]:
                    self.dbcondor.updateCondorJob(job["condorjobid"], {"condorstate": "tocancel"})

        # Get jobs killed by panda
        jobs = self.dbpanda.getJobs(f"actpandastatus='tobekilled' and sitename in {self.sitesselect}",
                                    ["pandaid", "condorjobid", "pandastatus", "id"])
        if not jobs:
            return

        for job in jobs:

            self.stopOnFlag()

            self.log.info(f"Cancelling Condor job for appjob({job['pandaid']}")
            select = f"id={job['id']}"

            # Check if condorjobid is set before cancelling the job
            if not job["condorjobid"]:
                self.dbpanda.updateJobs(select, {"actpandastatus": "cancelled"})
                continue

            # Put timings in the DB
            condorselect = f"condorjobid={job['condorjobid']} and condorjobs.id=pandajobs.condorjobid and siteName in {self.sitesselect}"
            condorjobs = self.dbcondor.getCondorJobsInfo(condorselect, tables="condorjobs,pandajobs")
            desc = {}
            if condorjobs:
                desc["endTime"] = condorjobs[0]["CompletionDate"] if condorjobs[0]["CompletionDate"] else datetime.datetime.utcnow()
                desc["startTime"] = condorjobs[0]["JobCurrentStartDate"] if condorjobs[0]["JobCurrentStartDate"] else datetime.datetime.utcnow()

            # Check if job was manually killed
            if job["pandastatus"] is not None:
                self.log.info(f"appjob({job['pandaid']}): Manually killed, marking cancelled")
                desc["pandastatus"] = None
                desc["error"] = "Job was killed in aCT"
            desc["actpandastatus"] = "cancelled"
            self.dbpanda.updateJobs(select, desc)

            # Finally cancel the condor job
            self.dbcondor.updateCondorJob(job["condorjobid"], {"condorstate": "tocancel"})

    def getStartTime(self, endtime, walltime):
        """
        Get starttime from endtime-walltime where endtime is datetime.datetime and walltime is in seconds
        If endtime is none then use current time
        """
        if not endtime:
            return datetime.datetime.utcnow() - datetime.timedelta(0, walltime)
        return endtime-datetime.timedelta(0, walltime)


    def updateStartingJobs(self):
        """
        Check for sent jobs that have been submitted to Condor and update
        actpandastatus to starting, also for jobs that were held.

        Signal handling strategy:
        - exit is checked before every job update
        """
        select = "condorjobs.id=pandajobs.condorjobid and (condorjobs.condorstate='submitted' or condorjobs.condorstate='holding')"
        select += f" and pandajobs.actpandastatus='sent' and siteName in {self.sitesselect}"
        select += " limit 100000"
        columns = ["condorjobs.id", "condorjobs.cluster", "condorjobs.appjobid"]
        jobstoupdate = self.dbcondor.getCondorJobsInfo(select, columns=columns, tables="condorjobs,pandajobs")

        if len(jobstoupdate) == 0:
            return
        else:
            idstr = ",".join([j["appjobid"] for j in jobstoupdate])
            self.log.info(f"Found {len(jobstoupdate)} submitted jobs ({idstr})")

        for job in jobstoupdate:
            self.stopOnFlag()
            select = "condorjobid='"+str(job["id"])+"'"
            desc = {}
            desc["pandastatus"] = "starting"
            desc["actpandastatus"] = "starting"
            desc["computingElement"] = job["cluster"].split(":")[0]
            self.dbpanda.updateJobs(select, desc)

    def updateRunningJobs(self):
        """
        Check for new running jobs.

        pandajobs are updated with:
        - pandastatus
        - node
        - computingElement
        - startTime

        Signal handling strategy:
        - exit is checked before every job update
        """
        # TODO: HARDCODED limit
        # do an inner join to pick up all jobs that should be set to running
        select = "condorjobs.id=pandajobs.condorjobid and condorjobs.condorstate='running' and pandajobs.actpandastatus='starting'"
        select += f" and siteName in {self.sitesselect} limit 100000"
        columns = ["condorjobs.id", "condorjobs.JobCurrentStartDate",
                "condorjobs.cluster", "pandajobs.pandaid", "pandajobs.siteName", "condorjobs.appjobid"]
        jobstoupdate = self.dbcondor.getCondorJobsInfo(select, columns=columns, tables="condorjobs,pandajobs")

        if len(jobstoupdate) == 0:
            return
        else:
            idstr = ",".join([j["appjobid"] for j in jobstoupdate])
            self.log.info(f"Found {len(jobstoupdate)} running jobs ({idstr})")

        for cj in jobstoupdate:
            self.stopOnFlag()
            select = "condorjobid='"+str(cj["id"])+"'"
            desc = {}
            desc["pandastatus"] = "running"
            desc["actpandastatus"] = "running"
            desc["computingElement"] = cj["cluster"].split(":")[0]
            desc["startTime"] = cj["JobCurrentStartDate"]
            # When true pilot job has started running, turn of aCT heartbeats
            if self.sites[cj["siteName"]]["truepilot"]:
                self.log.info(f"appjob({cj['pandaid']}): Job is running so stop sending heartbeats")
                desc["sendhb"] = 0
            self.dbpanda.updateJobs(select, desc)

    def updateFinishedJobs(self):
        """
        Check for new finished jobs.

        pandajobs are updated with:
        - pandastatus
        - startTime
        - endTime

        Signal handling strategy:
        - exit is checked before every job update
        """
        # TODO: HARDCODED limit
        # don't get jobs already having actpandastatus states treated by
        # validator to avoid race conditions
        select = "condorjobs.id=pandajobs.condorjobid and condorjobs.condorstate='done'"
        select += " and pandajobs.actpandastatus != 'tovalidate'"
        select += " and pandajobs.actpandastatus != 'toresubmit'"
        select += " and pandajobs.actpandastatus != 'toclean'"
        select += " and pandajobs.actpandastatus != 'finished'"
        select += f" and pandajobs.sitename in {self.sitesselect} limit 100000"
        columns = ["condorjobs.id", "condorjobs.JobCurrentStartDate", "condorjobs.CompletionDate",
                "condorjobs.appjobid", "pandajobs.sendhb", "pandajobs.siteName"]
        jobstoupdate = self.dbcondor.getCondorJobsInfo(select, tables="condorjobs,pandajobs", columns=columns)

        if len(jobstoupdate) == 0:
            return
        else:
            idstr = ",".join([j["appjobid"] for j in jobstoupdate])
            self.log.info(f"Found {len(jobstoupdate)} finished jobs ({idstr})")

        for cj in jobstoupdate:
            self.stopOnFlag()
            select = "condorjobid='"+str(cj["id"])+"'"
            desc = {}
            desc["pandastatus"] = "transferring"
            desc["actpandastatus"] = "tovalidate"
            desc["startTime"] = cj["JobCurrentStartDate"]
            desc["endTime"] = cj["CompletionDate"]
            # True pilot job may have gone straight to finished, turn off aCT heartbeats if necessary
            if self.sites[cj["siteName"]]["truepilot"] and cj["sendhb"] == 1:
                self.log.info(f"appjob({cj['appjobid']}): Job finished so stop sending heartbeats")
                desc["sendhb"] = 0
            self.dbpanda.updateJobs(select, desc)

    def updateFailedJobs(self):
        """
        Handle jobs in different unsuccessful states.

        Set jobs in condorstate failed to tofetch. Query condorjobs in
        condorstate donefailed, cancelled and lost and fill status in
        pandajobs.

        Signal handling strategy:
        - exit is checked before every job update
        """
        # TODO: HARDCODED limit
        # Look for failed final states
        select = "(condorstate='donefailed' or condorstate='cancelled' or condorstate='lost')"
        select += " and actpandastatus!='toclean' and actpandastatus!='toresubmit'"
        select += f" and pandajobs.condorjobid = condorjobs.id and pandajobs.sitename in {self.sitesselect} limit 100000"
        columns = ["condorstate", "appjobid", "condorjobid", "JobCurrentStartDate", "CompletionDate", "actpandastatus"]

        jobstoupdate = self.dbcondor.getCondorJobsInfo(select, columns=columns, tables="condorjobs,pandajobs")

        if len(jobstoupdate) == 0:
            return

        failedjobs = [job for job in jobstoupdate if job["condorstate"]=="donefailed"]
        if len(failedjobs) != 0:
            idstr = ",".join([j["appjobid"] for j in failedjobs])
            self.log.info(f"Found {len(failedjobs)} failed jobs ({idstr})")
        lostjobs = [job for job in jobstoupdate if job["condorstate"]=="lost"]
        if len(lostjobs) != 0:
            idstr = ",".join([j["appjobid"] for j in lostjobs])
            self.log.info(f"Found {len(lostjobs)} lost jobs ({idstr})")
        # Cancelled jobs already in terminal state will be cleaned up in cleanupLeftovers()
        cancelledjobs = [job for job in jobstoupdate if job["condorstate"]=="cancelled" and job["actpandastatus"] not in ("cancelled", "donecancelled", "failed", "donefailed")]
        if len(cancelledjobs) != 0:
            idstr = ",".join([j["appjobid"] for j in cancelledjobs])
            self.log.info(f"Found {len(cancelledjobs)} cancelled jobs ({idstr})")

        # Get outputs to download for failed jobs
        select = "condorstate='failed'"
        columns = ["id"]
        condorjobs = self.dbcondor.getCondorJobsInfo(select, columns)

        desc = {"condorstate": "tofetch", "tcondorstate": self.dbcondor.getTimeStamp()}
        for cj in condorjobs:
            self.stopOnFlag()
            select = f"id={cj['id']}"
            self.dbcondor.updateCondorJobs(desc, select)

        for cj in failedjobs:
            self.stopOnFlag()
            self.log.info(f"appjob({cj['appjobid']}): Job failed so stop sending heartbeats")
            select = "condorjobid='"+str(cj["condorjobid"])+"'"
            desc = {}
            desc["pandastatus"] = "transferring"
            desc["actpandastatus"] = "toclean" # to clean up any output
            desc["endTime"] = cj["CompletionDate"]
            desc["startTime"] = cj["JobCurrentStartDate"]
            # True pilot job may have gone straight to failed, turn off aCT heartbeats
            desc["sendhb"] = 0
            self.dbpanda.updateJobs(select, desc)

        for cj in lostjobs:
            self.stopOnFlag()
            # For truepilot, just set to clean and transferring to clean up condor job
            self.log.info(f"appjob({cj['appjobid']}): Job is lost, cleaning up condor job")
            select = "condorjobid='"+str(cj["condorjobid"])+"'"
            desc = {}
            desc["sendhb"] = 0
            desc["pandastatus"] = "transferring"
            desc["actpandastatus"] = "toclean"
            self.dbpanda.updateJobs(select,desc)

        for cj in cancelledjobs:
            self.stopOnFlag()
            # Only applies to manually cancelled jobs, simply clean them
            self.log.info(f"appjob({cj['appjobid']}): Job was cancelled, cleaning up condor job")
            select = f"condorjobid={cj['condorjobid']}"
            desc = {}
            desc["sendhb"] = 0
            desc["pandastatus"] = "transferring"
            desc["actpandastatus"] = "toclean"
            self.dbpanda.updateJobs(select, desc)

    def cleanupLeftovers(self):
        """
        Clean jobs left behind in condorjobs table.

        The following jobs are left behind:
        - condorstate=tocancel or cancelling when cluster is empty
        - condorstate=done or cancelled or lost or donefailed when id not in pandajobs
        - condorstate=cancelled and actpandastatus=cancelled/donecancelled/failed/donefailed

        Signal handling strategy:
        - exit is checked before every job update
        """
        # Even though the transaction probably gets rolled back
        # automatically, it is nice to handle it explicitly. Also, this
        # simplifies the method with one nested block.
        select = "(condorstate='tocancel' or condorstate='cancelling') and (cluster='' or cluster is NULL)"
        jobs = self.dbcondor.getCondorJobsInfo(select, ["id", "appjobid"])
        for job in jobs:
            self.stopOnFlag()
            self.log.info(f"appjob({job['appjobid']}): Deleting from condorjobs unsubmitted job condorjob({job['id']})")
            self.dbcondor.deleteCondorJob(job["id"])

        select = "(condorstate='done' or condorstate='lost' or condorstate='cancelled' or condorstate='donefailed') \
                and condorjobs.id not in (select condorjobid from pandajobs where condorjobid is not NULL)"
        jobs = self.dbcondor.getCondorJobsInfo(select, ["id", "appjobid", "condorstate"])
        cleandesc = {"condorstate":"toclean", "tcondorstate": self.dbcondor.getTimeStamp()}
        for job in jobs:
            self.stopOnFlag()
            # done jobs should not be there, log a warning
            if job["condorstate"] == "done":
                self.log.warning(f"appjob({job['appjobid']}): Removing orphaned done job condorjob({job['id']})")
            else:
                self.log.info(f"appjob({job['appjobid']}): Cleaning left behind {job['condorstate']} job condorjob({job['id']})")
            self.dbcondor.updateCondorJob(job["id"], cleandesc)

        select = "condorstate='cancelled' and (actpandastatus in ('cancelled', 'donecancelled', 'failed', 'donefailed')) " \
                 f"and pandajobs.condorjobid = condorjobs.id and pandajobs.sitename in {self.sitesselect}"
        cleandesc = {"condorstate":"toclean", "tcondorstate": self.dbcondor.getTimeStamp()}
        jobs = self.dbcondor.getCondorJobsInfo(select, ["condorjobs.id", "condorjobs.appjobid"], tables="condorjobs, pandajobs")
        for job in jobs:
            self.stopOnFlag()
            self.log.info(f"appjob({job['appjobid']}): Cleaning cancelled job condorjob({job['id']})")
            self.dbcondor.updateCondorJob(job["id"], cleandesc)

    def process(self):
        """Main loop"""
        self.log.info("Running")
        self.setSites()
        # Check for jobs that panda told us to kill and cancel them in Condor
        self.checkJobstoKill()
        # Check status of condorjobs
        # Query jobs that were submitted since last time
        self.updateStartingJobs()
        # Query jobs which changed to running condorstate
        self.updateRunningJobs()
        # Query jobs in condorstate done and update pandajobs
        # Set to toclean
        self.updateFinishedJobs()
        # Query jobs in condorstate failed, set to tofetch
        # Query jobs in condorstate done, donefailed, cancelled and lost, set to toclean.
        self.updateFailedJobs()
        # Clean up jobs left behind in condorjobs table
        self.cleanupLeftovers()
