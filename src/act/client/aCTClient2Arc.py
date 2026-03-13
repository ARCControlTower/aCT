
import arc
from act.arc.aCTDBArcNEW import aCTDBArc
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
        with self.clidb.Session() as session:
            proxies = self.clidb.getProxies(session)
        with self.clidb.Session.begin() as session:
            for proxyid in proxies:
                self.stopOnFlag()
                self.insertNewJobs(proxyid, session, 1000)

    def insertNewJobs(self, proxyid, session, num):
        """Insert new jobs to ARC table for proxy."""
        # Get jobs that haven't been inserted to ARC table yet
        # (they don't have reference to ARC table, arcjobid is null).
        jobs = self.clidb.getJobsInfo(proxyid, session, num)
        jobdescs = arc.JobDescriptionList()
        for job in jobs:
            # create downloads list
            arc.JobDescription.Parse(job.jobdesc, jobdescs)

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
                arcjobid = self.arcdb.insertArcJobDescription(
                    session,
                    job.jobdesc,
                    proxyid,
                    0,
                    job.clusterlist,
                    job.id,
                    ';'.join(downloads)
                )
            except Exception as exc:
                self.log.error(f'Error inserting appjob({job.id}) to arc table: {exc}')
            else:
                # create a reference to job in client table
                try:
                    self.clidb.updateJob(proxyid, session, job.id,  {'arcjobid':arcjobid})
                    self.log.info(f'Successfully inserted appjob({job.id}) {arcjobid} to ARC engine')
                except Exception as exc:
                    self.log.error(f'Error connecting clientjob({job.id}) with arcjob({arcjobid}): {exc}')

    def finish(self):
        self.clidb.close()
        self.arcdb.close()
        super().finish()
