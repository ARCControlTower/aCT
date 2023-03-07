import os
import shutil

from act.atlas.aCTATLASProcess import aCTATLASProcess


class aCTValidatorCondor(aCTATLASProcess):
    """
    Condor is truepilot-only so validator just moves jobs to final state, sets
    condorjobs to clean, and cleans up any leftover temp files
    """

    def __init__(self, name):
        super().__init__(name, ceflavour=['HTCONDOR-CE', 'CREAM-CE'])

    def cleanFinishedJob(self, pandaid):
        """Remove temporary files needed for this job."""
        pandainputdir = os.path.join(self.tmpdir, 'inputfiles', str(pandaid))
        shutil.rmtree(pandainputdir, ignore_errors=True)

    def validateFinishedJobs(self):
        """
        Check for jobs with actpandastatus tovalidate and pandastatus transferring
        and move to actpandastatus to finished

        Signal handling strategy:
        - all updates are executed in one transaction which gets rolled back on
          signal
        - if cleanFinishedJob() happens before the transaction is commited, it
          is not a problem
        """
        with self.transaction([self.dbpanda, self.dbcondor]):
            # get all jobs with pandastatus running and actpandastatus tovalidate
            select = "(pandastatus='transferring' and actpandastatus='tovalidate') and siteName in %s limit 100000" % self.sitesselect
            columns = ["condorjobid", "pandaid"]
            jobstoupdate = self.dbpanda.getJobs(select, columns=columns)

            if len(jobstoupdate) == 0:
                # nothing to do
                return

            # Skip validation for the true pilot jobs, just copy logs, set to done and clean condor job
            for job in jobstoupdate:
                self.log.info('%s: Skip validation' % job['pandaid'])
                select = "condorjobid='"+str(job["condorjobid"])+"'"
                desc = {"pandastatus": None, "actpandastatus": "finished"}
                self.dbpanda.updateJobsLazy(select, desc)
                # set condorjobs state toclean
                desc = {"condorstate":"toclean", "tcondorstate": self.dbcondor.getTimeStamp()}
                self.dbcondor.updateCondorJobLazy(job['condorjobid'], desc)
                self.cleanFinishedJob(job['pandaid'])

    def cleanFailedJobs(self):
        """
        Check for jobs with actpandastatus toclean and pandastatus transferring.
        Move actpandastatus to failed.

        Signal handling strategy:
        - all updates are executed in one transaction which gets rolled back on
          signal
        - if cleanFinishedJob() happens before the transaction is commited, it
          is not a problem
        """
        with self.transaction([self.dbpanda, self.dbcondor]):

            # get all jobs with pandastatus transferring and actpandastatus toclean
            select = "(pandastatus='transferring' and actpandastatus='toclean') and siteName in %s limit 100000" % self.sitesselect
            columns = ["condorjobid", "pandaid"]
            jobstoupdate = self.dbpanda.getJobs(select, columns=columns)

            if len(jobstoupdate) == 0:
                # nothing to do
                return

            # For truepilot jobs, don't try to clean outputs (too dangerous), just clean condor job
            for job in jobstoupdate[:]:
                self.log.info("%s: Skip cleanup of output files" % job['pandaid'])
                select = "condorjobid='"+str(job["condorjobid"])+"'"
                desc = {"pandastatus": None, "actpandastatus": "failed"}
                self.dbpanda.updateJobsLazy(select, desc)
                # set condorjobs state toclean
                desc = {"condorstate":"toclean", "tcondorstate": self.dbcondor.getTimeStamp()}
                self.dbcondor.updateCondorJobLazy(job['condorjobid'], desc)
                self.cleanFinishedJob(job['pandaid'])

    def cleanResubmittingJobs(self):
        """
        Check for jobs with actpandastatus toresubmit and pandastatus starting.
        Move actpandastatus to starting and set condorjobid to NULL.
        For Condor true pilot, resubmission should never be automatic, so this
        workflow only happens when the DB is manually changed.

        Signal handling strategy:
        - all updates are executed in one transaction which gets rolled back on
          signal
        """
        with self.transaction([self.dbpanda, self.dbcondor]):

            # First check for resubmitting jobs with no arcjob id defined
            select = "(actpandastatus='toresubmit' and condorjobid=NULL) and siteName in %s limit 100000" % self.sitesselect
            columns = ["pandaid", "id"]

            jobstoupdate = self.dbpanda.getJobs(select, columns=columns)

            for job in jobstoupdate:
                self.log.info('%s: resubmitting' % job['pandaid'])
                select = "id="+str(job['id'])
                desc = {"actpandastatus": "starting", "condorjobid": None}
                self.dbpanda.updateJobsLazy(select, desc)

            # Get all other jobs with actpandastatus toresubmit
            select = "actpandastatus='toresubmit' and condorjobs.id=pandajobs.condorjobid and siteName in %s limit 100" % self.sitesselect
            columns = ["pandajobs.condorjobid", "pandajobs.pandaid", "condorjobs.ClusterId", "condorjobs.condorstate"]
            jobstoupdate = self.dbcondor.getCondorJobsInfo(select, columns=columns, tables='condorjobs, pandajobs')

            if len(jobstoupdate) == 0:
                # nothing to do
                return

            for job in jobstoupdate:
                # Only try to cancel jobs which are not finished
                if job['condorstate'] not in ('donefailed', 'done', 'lost', 'cancelled'):
                    self.log.info('%s: manually asked to resubmit, cancelling condor job %s' %
                                  (job['pandaid'], job['ClusterId']))
                    desc = {'condorstate': 'tocancel', 'tcondorstate': self.dbcondor.getTimeStamp()}
                    self.dbcondor.updateCondorJobLazy(job['condorjobid'], desc)

                self.log.info('%s: resubmitting' % job['pandaid'])
                select = "pandaid="+str(job['pandaid'])
                desc = {"actpandastatus": "starting", "condorjobid": None}
                self.dbpanda.updateJobsLazy(select, desc)

    def process(self):
        self.setSites()
        self.validateFinishedJobs()
        self.cleanFailedJobs()
        self.cleanResubmittingJobs()
