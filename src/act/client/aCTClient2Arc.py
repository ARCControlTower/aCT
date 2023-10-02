
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
            # aCTClient2Arc races with jobmgr.killJobs so lock is required
            with self.arcdb.namedLock('nulljobs', timeout=10) as lock:
                if lock:
                    self.insertNewJobs(proxyid, 1000)
                else:
                    self.log.warning('Could not acquire lock to insert jobs')

    def insertNewJobs(self, proxyid, num):
        """Insert new jobs to ARC table for proxy."""
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
            except Exception as exc:
                self.log.error(f'Error inserting appjob({job["id"]}) to arc table: {exc}')
            else:
                # create a reference to job in client table
                self.clidb.updateJob(job['id'], {
                    'arcjobid': row['LAST_INSERT_ID()'],
                    'modified': self.clidb.getTimeStamp()
                })
                self.log.info(f'Successfully inserted appjob({job["id"]}) {row["LAST_INSERT_ID()"]} to ARC engine')

    def finish(self):
        self.clidb.close()
        self.arcdb.close()
        super().finish()
