# aCTFetcher.py
#
# Condor automatically collects the stdout for the job so this Fetcher simply
# moves the job to the next state.

from act.common.aCTProcess import aCTProcess

class aCTFetcher(aCTProcess):
    '''
    Moves finished/failed jobs to the next state
    '''

    def fetchJobs(self, condorstate, nextcondorstate):

        # Get list of jobs in the right state
        select = "condorstate='%s' and cluster='%s' limit 100" % (condorstate, self.cluster)
        columns = ['id', 'ClusterId', 'appjobid']
        jobstofetch = self.dbcondor.getCondorJobsInfo(select, columns)

        if not jobstofetch:
            return

        self.log.info("Fetching %d jobs" % len(jobstofetch))

        for job in jobstofetch:
            self.log.info("%s: Finished with job %s" % (job['appjobid'], job['ClusterId']))
            self.dbcondor.updateCondorJob(job['id'], {"condorstate": nextcondorstate,
                                                      "tcondorstate": self.db.getTimeStamp()})


    def process(self):

        # failed jobs
        self.fetchJobs('tofetch', 'donefailed')
        # finished jobs
        self.fetchJobs('finished', 'done')


if __name__ == '__main__':
    st = aCTFetcher()
    st.run()
    st.finish()
