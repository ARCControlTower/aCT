
import arc
from act.arc.aCTDBArc import aCTDBArc
from act.client.clientdb import ClientDB
from act.common.aCTConfig import aCTConfigARC
from act.common.aCTProcess import aCTProcess


class aCTClient2Arc(aCTProcess):
    """Object that runs until interrupted and periodically submits new jobs."""

    # overriding to prevent cluster argument
    def __init__(self):
        super().__init__()

    def loadConf(self):
        self.conf = aCTConfigARC()

    def setup(self):
        super().setup()
        self.clidb = ClientDB(self.log)
        self.arcdb = aCTDBArc(self.log)

    def process(self):
        """
        Insert new jobs to ARC table.

        Signal handling strategy:
        - termination is checked before handling every proxy
        """
        proxies = self.clidb.getProxies()
        for proxyid in proxies:
            self.stopOnFlag()
            self.insertNewJobs(proxyid, 1000)

    def insertNewJobs(self, proxyid, num):
        """Insert new jobs to ARC table for proxy."""
        # client2arc races with jobmgr.killJobs for jobs that have no arcjobid
        res = self.arcdb.db.getMutexLock('nulljobs')
        if not res:
            raise Exception("Could not acquire lock to insert jobs")

        # Get jobs that haven't been inserted to ARC table yet
        # (they don't have reference to ARC table, arcjobid is null).
        jobs = self.clidb.getJobsInfo(
            ['id', 'jobdesc', 'clusterlist'],
            where='proxyid = %s AND arcjobid IS NULL AND jobdesc IS NOT NULL',
            where_params=[proxyid],
            order_by='%s',
            order_by_params=['id'],
            limit=num
        )
        jobdescs = arc.JobDescriptionList()
        for job in jobs:
            # create downloads list
            arc.JobDescription.Parse(job['jobdesc'], jobdescs)

            # TODO: this should be done according to the xRSL output files
            # all files from session dir
            downloads = ['/']

            # all diagnose files if log dir is specified
            logdir = jobdescs[-1].Application.LogDir
            if logdir:
                if logdir.endswith('/'):
                    downloads.append(f'diagnose={logdir}')
                else:
                    downloads.append(f'diagnose={logdir}/')

            # insert job to ARC table
            try:
                row = self.arcdb.insertArcJobDescription(
                    job['jobdesc'],
                    proxyid,
                    0,
                    job['clusterlist'],
                    job['id'],
                    ';'.join(downloads)
                )
            except:
                self.log.exception(f'Error inserting appjob({job["id"]}) to arc table')
            else:
                # create a reference to job in client table
                self.clidb.updateJob(job['id'], {
                    'arcjobid': row['LAST_INSERT_ID()'],
                    'modified': self.clidb.getTimeStamp()
                })
                self.log.info(f'Successfully inserted appjob({job["id"]}) {row["LAST_INSERT_ID()"]} to ARC engine')

        res = self.arcdb.db.releaseMutexLock('nulljobs')
        if not res:
            raise Exception("Could not release lock after inserting jobs")

    def finish(self):
        self.clidb.close()
        self.arcdb.close()
        super().finish()
