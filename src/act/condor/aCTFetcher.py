# aCTFetcher.py
#
# Condor automatically collects the stdout for the job so this Fetcher simply
# moves the job to the next state.

from act.condor.aCTCondorProcess import aCTCondorProcess


class aCTFetcher(aCTCondorProcess):

    def fetchJobs(self, condorstate, nextcondorstate):
        """
        Move finished and failed jobs to the next state.

        Signal handling strategy:
        - exit is checked before updating every job
        """
        # Get list of jobs in the right state
        select = f"condorstate='{condorstate}' and cluster='{self.cluster}' limit 100"
        columns = ['id', 'ClusterId', 'appjobid']
        jobstofetch = self.db.getCondorJobsInfo(select, columns)

        if not jobstofetch:
            return

        self.log.info(f"Fetching {len(jobstofetch)} jobs")

        for job in jobstofetch:
            self.stopOnFlag()
            self.log.info(f"appjob({job['appjobid']}): Finished with condorjob({job['ClusterId']})")
            self.db.updateCondorJob(
                job['id'],
                {"condorstate": nextcondorstate, "tcondorstate": self.db.getTimeStamp()}
            )

    def process(self):
        # failed jobs
        self.fetchJobs('tofetch', 'donefailed')
        # finished jobs
        self.fetchJobs('finished', 'done')
