# aCTCleaner.py
#
# Cleans jobs from Condor table
#

from act.common.aCTProcess import aCTProcess


class aCTCleaner(aCTProcess):

    def processToClean(self):

        select = f"condorstate='toclean' and cluster='{self.cluster}' limit 100"
        columns = ['id', 'ClusterId', 'appjobid']
        jobstoclean = self.dbcondor.getCondorJobsInfo(select, columns)

        if not jobstoclean:
            return

        self.log.info(f"Cleaning {len(jobstoclean)} jobs")

        for job in jobstoclean:
            self.log.info(f"{job['appjobid']}: Cleaning job {job['ClusterId']}")
            self.dbcondor.deleteCondorJob(job['id'])

    def process(self):

        # clean jobs
        self.processToClean()


if __name__ == '__main__':
    st = aCTCleaner()
    st.run()
    st.finish()
