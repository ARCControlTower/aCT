# aCTCleaner.py
#
# Cleans jobs from Condor table
#

from act.condor.aCTCondorProcess import aCTCondorProcess


class aCTCleaner(aCTCondorProcess):

    def processToClean(self):

        select = f"condorstate='toclean' and cluster='{self.cluster}' limit 100"
        columns = ['id', 'ClusterId', 'appjobid']
        jobstoclean = self.db.getCondorJobsInfo(select, columns)

        if not jobstoclean:
            return

        self.log.info(f"Cleaning {len(jobstoclean)} jobs")

        for job in jobstoclean:
            if self.mustExit:
                self.log.info(f"Exiting early due to requested shutdown")
                self.stopWithException()
            self.log.info(f"{job['appjobid']}: Cleaning job {job['ClusterId']}")
            self.db.deleteCondorJob(job['id'])

    def process(self):
        # clean jobs
        self.processToClean()
