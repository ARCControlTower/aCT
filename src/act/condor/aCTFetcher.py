# aCTFetcher.py
#
# Condor automatically collects the stdout for the job so this Fetcher simply
# moves the job to the next state.

from act.common.aCTProcess import ExitProcessException
from act.condor.aCTCondorProcess import aCTCondorProcess


class aCTFetcher(aCTCondorProcess):

    def fetchJobs(self, condorstate, nextcondorstate):
        """
        Move finished and failed jobs to the next state.

        Signal handling strategy:
        - All operations are done in one transaction which is rolled back
          on signal.
        """
        # exit handling try block
        try:

            # Get list of jobs in the right state
            select = f"condorstate='{condorstate}' and cluster='{self.cluster}' limit 100"
            columns = ['id', 'ClusterId', 'appjobid']
            jobstofetch = self.db.getCondorJobsInfo(select, columns)

            if not jobstofetch:
                return

            self.log.info(f"Fetching {len(jobstofetch)} jobs")

            for job in jobstofetch:
                self.log.info(f"{job['appjobid']}: Finished with job {job['ClusterId']}")
                self.db.updateCondorJobLazy(
                    job['id'],
                    {"condorstate": nextcondorstate, "tcondorstate": self.db.getTimeStamp()}
                )

        except Exception as exc:
            if isinstance(exc, ExitProcessException):
                self.log.info("Rolling back DB transaction on process exit")
            else:
                self.log.error(f"Rolling back DB transaction on error: {exc}")
            self.db.db.conn.rollback()
            raise
        else:
            self.db.Commit()

    def process(self):
        # failed jobs
        self.fetchJobs('tofetch', 'donefailed')
        # finished jobs
        self.fetchJobs('finished', 'done')
