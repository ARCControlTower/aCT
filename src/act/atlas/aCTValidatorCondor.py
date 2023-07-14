import os
import shutil

from act.atlas.aCTATLASProcess import aCTATLASProcess


class aCTValidatorCondor(aCTATLASProcess):
    """
    Condor is truepilot-only so validator just moves jobs to final state, sets
    condorjobs to clean, and cleans up any leftover temp files
    """

    def __init__(self):
        super().__init__(ceflavour=['HTCONDOR-CE', 'CREAM-CE'])

    def cleanFinishedJob(self, pandaid):
        """Remove temporary files needed for this job."""
        pandainputdir = os.path.join(self.tmpdir, 'inputfiles', str(pandaid))
        shutil.rmtree(pandainputdir, ignore_errors=True)

    def validateFinishedJobs(self):
        """
        Check for jobs with actpandastatus tovalidate and pandastatus transferring
        and move to actpandastatus to finished

        Signal handling strategy:
        - exit is checked before every job update
        """
        # get all jobs with pandastatus running and actpandastatus tovalidate
        select = "(pandastatus='transferring' and actpandastatus='tovalidate') and siteName in %s limit 100000" % self.sitesselect
        columns = ["condorjobid", "pandaid"]
        jobstoupdate = self.dbpanda.getJobs(select, columns=columns)

        if len(jobstoupdate) == 0:
            # nothing to do
            return

        # Skip validation for the true pilot jobs, just copy logs, set to done and clean condor job
        cleandesc = {"condorstate": "toclean", "tcondorstate": self.dbcondor.getTimeStamp()}
        finishdesc = {"pandastatus": None, "actpandastatus": "finished"}
        for job in jobstoupdate:
            self.stopOnFlag()
            self.log.info(f"appjob({job['pandaid']}): Skip validation")
            select = "condorjobid='"+str(job["condorjobid"])+"'"
            self.dbpanda.updateJobs(select, finishdesc)
            # set condorjobs state toclean
            self.dbcondor.updateCondorJob(job['condorjobid'], cleandesc)
            self.cleanFinishedJob(job['pandaid'])

    def cleanFailedJobs(self):
        """
        Check for jobs with actpandastatus toclean and pandastatus transferring.
        Move actpandastatus to failed.

        Signal handling strategy:
        - exit is checked before every job update
        """
        # get all jobs with pandastatus transferring and actpandastatus toclean
        select = "(pandastatus='transferring' and actpandastatus='toclean') and siteName in %s limit 100000" % self.sitesselect
        columns = ["condorjobid", "pandaid"]
        jobstoupdate = self.dbpanda.getJobs(select, columns=columns)

        if len(jobstoupdate) == 0:
            # nothing to do
            return

        # For truepilot jobs, don't try to clean outputs (too dangerous), just clean condor job
        cleandesc = {"condorstate":"toclean", "tcondorstate": self.dbcondor.getTimeStamp()}
        faildesc = {"pandastatus": None, "actpandastatus": "failed"}
        for job in jobstoupdate[:]:
            self.stopOnFlag()
            self.log.info(f"appjob({job['pandaid']}): Skip cleanup of output files")
            select = "condorjobid='"+str(job["condorjobid"])+"'"
            self.dbpanda.updateJobs(select, faildesc)
            # set condorjobs state toclean
            self.dbcondor.updateCondorJob(job['condorjobid'], cleandesc)
            self.cleanFinishedJob(job['pandaid'])

    def cleanResubmittingJobs(self):
        """
        Check for jobs with actpandastatus toresubmit and pandastatus starting.
        Move actpandastatus to starting and set condorjobid to NULL.
        For Condor true pilot, resubmission should never be automatic, so this
        workflow only happens when the DB is manually changed.

        Signal handling strategy:
        - exit is checked before every job update
        """
        # First check for resubmitting jobs with no arcjob id defined
        select = "(actpandastatus='toresubmit' and condorjobid=NULL) and siteName in %s limit 100000" % self.sitesselect
        columns = ["pandaid", "id"]

        jobstoupdate = self.dbpanda.getJobs(select, columns=columns)

        desc = {"actpandastatus": "starting", "condorjobid": None}
        for job in jobstoupdate:
            self.stopOnFlag()
            self.log.info(f"appjob({job['pandaid']}): resubmitting")
            select = "id="+str(job['id'])
            self.dbpanda.updateJobs(select, desc)

        # Get all other jobs with actpandastatus toresubmit
        select = "actpandastatus='toresubmit' and condorjobs.id=pandajobs.condorjobid and siteName in %s limit 100" % self.sitesselect
        columns = ["pandajobs.condorjobid", "pandajobs.pandaid", "condorjobs.ClusterId", "condorjobs.condorstate"]
        jobstoupdate = self.dbcondor.getCondorJobsInfo(select, columns=columns, tables='condorjobs, pandajobs')

        if len(jobstoupdate) == 0:
            # nothing to do
            return

        canceldesc = {'condorstate': 'tocancel', 'tcondorstate': self.dbcondor.getTimeStamp()}
        resubdesc = {"actpandastatus": "starting", "condorjobid": None}
        for job in jobstoupdate:
            self.stopOnFlag()
            # Only try to cancel jobs which are not finished
            if job['condorstate'] not in ('donefailed', 'done', 'lost', 'cancelled'):
                self.log.info(f"appjob({job['pandaid']}): manually asked to resubmit, cancelling condor job condorid({job['ClusterId']})")
                self.dbcondor.updateCondorJob(job['condorjobid'], canceldesc)

            self.log.info(f"appjob({job['pandaid']}): resubmitting")
            select = "pandaid="+str(job['pandaid'])
            self.dbpanda.updateJobs(select, resubdesc)

    def process(self):
        self.setSites()
        self.validateFinishedJobs()
        self.cleanFailedJobs()
        self.cleanResubmittingJobs()
