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
            self.stopOnFlag()
            self.log.info(f"{job['appjobid']}: Cleaning job {job['ClusterId']}")
            self.db.deleteCondorJob(job['id'])

    def process(self):
        # clean jobs
        self.processToClean()
