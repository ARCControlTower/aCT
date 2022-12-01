# aCTStatus.py
#
# Process to check the status of running Condor jobs
#
import time

import classad
import htcondor
from act.common.aCTProcess import ExitProcessException
from act.condor.aCTCondorProcess import aCTCondorProcess

CONDOR_STATE_MAP = {
    0: 'Undefined', # used before real state is known
    1: 'Idle',
    2: 'Running',
    3: 'Removed',
    4: 'Completed',
    5: 'Held',
    6: 'Transferring Output',
    7: 'Suspended'
}


class aCTStatus(aCTCondorProcess):
    '''
    Class for checking the status of submitted Condor jobs and updating their
    status in the DB.
    '''

    def setup(self):
        super().setup()
        self.schedd = htcondor.Schedd()
        # store the last checkJobs time to avoid overloading of GIIS
        self.checktime = time.time()

    def checkJobs(self):
        """
        Update status of all running jobs.

        Signal handling strategy:
        - All operations are done in a single transaction that gets rolled back
          on signal.
        """
        # exit handling try block
        try:

            # minimum time between checks
            if time.time() < self.checktime + self.conf.jobs.checkmintime:
                self.log.debug("mininterval not reached")
                return
            self.checktime = time.time()

            # check jobs which were last checked more than checkinterval ago
            query = "condorstate in ('submitted', 'running', 'cancelling', 'holding') and " \
                    f"ClusterId not like '' and cluster='{self.cluster}' and " \
                    f"{self.db.timeStampLessThan('tcondorstate', self.conf.jobs.checkinterval)}" \
                    " limit 100000"
            jobstocheck = self.db.getCondorJobsInfo(query, columns=['id', 'appjobid', 'JobStatus', 'ClusterId'])

            if not jobstocheck:
                return
            self.log.info(f"{len(jobstocheck)} jobs to check")

            # Query condor for all jobs with this cluster
            # Add here attributes we eventually want in the DB
            attrs = ['JobStatus', 'ExitCode', 'GlobalJobId', 'GridJobId', 'JobCurrentStartDate', 'CompletionDate']
            # Here with attributes we want to query but not store
            qattrs = attrs + ['ClusterId', 'GridResourceUnavailableTime']
            t1 = time.time()
            try:
                status = self.schedd.xquery(requirements=f'ACTCluster=?="{self.cluster}"',
                                            projection=qattrs)
            except IOError as e:
                self.log.error(f'Failed querying schedd: {e}')
                return
            condorstatuses = {}
            while True:
                try:
                    stat = next(status)
                    condorstatuses[stat['ClusterId']] = stat
                except StopIteration:
                    break
                except RuntimeError as e: # Usually a timeout connecting to remote host, try again
                    self.log.error(f'Problem querying schedd: {e}')
                    break
            t2 = time.time()
            self.log.debug(f'took {t2 - t1} to query schedd (returning {len(condorstatuses)} results)')

            # Loop over jobs
            for job in jobstocheck:

                appjobid = job['appjobid']
                oldstatus = job['JobStatus']
                clusterid = job['ClusterId']

                try:
                    updatedjob = condorstatuses[clusterid]
                    jobstatus = updatedjob['JobStatus']
                except KeyError:
                    # If not in the queue, look in history for finished jobs
                    self.log.debug(f'{appjobid}: Job not in condor queue, checking history')
                    history = self.schedd.history(f'ClusterID=?={clusterid}', qattrs, 1)
                    try:
                        hist = next(history)
                    except StopIteration:
                        self.log.warning(f'{appjobid}: Job id {clusterid} not found!')
                        continue
                    except RuntimeError as e: # Usually a timeout connecting to remote host
                        self.log.error(f'{appjobid}: Problem getting history: {e}')
                        continue
                    updatedjob = hist
                    jobstatus = updatedjob['JobStatus']

                self.log.debug(f'{appjobid}: Job {clusterid}: Status {jobstatus} ({CONDOR_STATE_MAP[jobstatus]})')

                if jobstatus == 1 and 'GridResourceUnavailableTime' in updatedjob:
                    # Job could not be submitted due to remote CE being down
                    self.log.warning(f'{appjobid}: Could not submit to remote CE, will retry')
                    jobdesc = {}
                    jobdesc['condorstate'] = 'toresubmit'
                    jobdesc['JobStatus'] = 0
                    jobdesc['tcondorstate'] = self.db.getTimeStamp()
                    jobdesc['tstate'] = self.db.getTimeStamp()
                    self.db.updateCondorJobLazy(job['id'], jobdesc)
                    continue

                if oldstatus == jobstatus:
                    # just update timestamp
                    self.db.updateCondorJobLazy(job['id'], {'tcondorstate': self.db.getTimeStamp()})
                    continue

                self.log.info(f"{appjobid}: Job {clusterid}: {CONDOR_STATE_MAP[oldstatus]} -> {CONDOR_STATE_MAP[jobstatus]}")

                # state changed, update condorstate
                condorstate = 'submitted'
                if jobstatus in (2, 6) : # running, transferring output
                    condorstate = 'running'
                elif jobstatus == 3: # removed
                    condorstate = 'cancelled'
                elif jobstatus == 4: # finished
                    # If job is killed by signal ExitCode can be missing from the classad
                    if 'ExitCode' in updatedjob and updatedjob['ExitCode'] == 0:
                        condorstate = 'finished'
                    else:
                        condorstate = 'failed'
                elif jobstatus in (5, 7):
                    condorstate = 'holding'

                # Filter out fields added by condor that we are not interested in
                jobdesc = dict([(k, v) for (k, v) in updatedjob.items() if k in attrs and v != classad.Value.Undefined])
                # Undefined is 2 in condor which means JobStatus running is ignored
                jobdesc['JobStatus'] = updatedjob['JobStatus']
                jobdesc['condorstate'] = condorstate
                jobdesc['tcondorstate'] = self.db.getTimeStamp()
                jobdesc['tstate'] = self.db.getTimeStamp()
                jobdesc['CompletionDate'] = self.db.getTimeStamp(jobdesc.get('CompletionDate', 0))
                jobdesc['JobCurrentStartDate'] = self.db.getTimeStamp(jobdesc.get('JobCurrentStartDate', 0))
                self.log.debug(str(jobdesc))
                self.db.updateCondorJobLazy(job['id'], jobdesc)

        except Exception as exc:
            if isinstance(exc, ExitProcessException):
                self.log.info("Rolling back DB transaction on process exit")
            else:
                self.log.error(f"Rolling back DB transaction on error: {exc}")
            self.db.db.conn.rollback()
            raise
        else:
            self.db.Commit()

        self.log.info('Done')

    def checkLostJobs(self):
        """
        Move jobs with a long time since status update to lost.

        Signal handling strategy:
        - All operations are done in a single transaction that gets rolled back
          on signal.
        """
        # exit handling try block
        try:

            # 2 days limit. TODO: configurable?
            jobs = self.db.getCondorJobsInfo(
                "condorstate in ('submitted', 'running', 'cancelling', 'finished') and " \
                f"cluster='{self.cluster}' and {self.db.timeStampLessThan('tcondorstate', 172800)}",
                ['id', 'appjobid', 'ClusterId', 'condorstate']
            )

            for job in jobs:
                if job['condorstate'] == 'cancelling':
                    self.log.warning(f"{job['appjobid']}: Job {job['ClusterId']} lost from information system, marking as cancelled")
                    self.db.updateCondorJobLazy(
                        job['id'],
                        {'condorstate': 'cancelled', 'tcondorstate': self.db.getTimeStamp()}
                    )
                else:
                    self.log.warning(f"{job['appjobid']}: Job {job['ClusterId']} lost from information system, marking as lost")
                    self.db.updateCondorJobLazy(
                        job['id'],
                        {'condorstate': 'lost', 'tcondorstate': self.db.getTimeStamp()}
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

    def checkStuckJobs(self):
        """
        check jobs with tstate too long ago and set them tocancel
        maxtimestate can be set in arc config file for any condor JobStatus,
        e.g. maxtimeidle, maxtimerunning

        Signal handling strategy:
        - Calls of deleteCondorJob() perform Commit() so it is possible that
          multiple transactions will be done. Ideally, the delete would be
          lazy as well. The result is not wrong, just a little bit
          inefficient.
        - All operation are done in one transaction which is rolled back
          on signal.
        """
        # exit handling try block
        try:

            # Loop over possible states
            # Note: MySQL is case-insensitive. Need to watch out with other DBs
            for jobstateid, jobstate in CONDOR_STATE_MAP.items():
                maxtime = self.conf.jobs.get(f"maxtime{jobstate.lower()}")
                if not maxtime:
                    continue

                # be careful not to cancel jobs that are stuck in cleaning
                select = f"JobStatus='{jobstateid}' and {self.db.timeStampLessThan('tstate', maxtime)}"
                jobs = self.db.getCondorJobsInfo(select, columns=['id', 'ClusterId', 'appjobid', 'condorstate'])

                for job in jobs:
                    if job['condorstate'] == 'toclean' or job['condorstate'] == 'cancelling':
                        # mark as cancelled jobs stuck in toclean/cancelling
                        self.log.info(f"{job['appjobid']}: Job stuck in toclean/cancelling for too long, marking cancelled")
                        self.db.updateCondorJobLazy(
                            job['id'],
                            {'condorstate': 'cancelled', 'tcondorstate': self.db.getTimeStamp(), 'tstate': self.db.getTimeStamp()}
                        )
                        continue

                    self.log.warning(f"{job['appjobid']}: Job {job['ClusterId']} too long in state {jobstate}, cancelling")
                    if job['ClusterId']:
                        # If jobid is defined, cancel
                        self.db.updateCondorJobLazy(
                            job['id'],
                            {'condorstate': 'tocancel', 'tcondorstate': self.db.getTimeStamp(), 'tstate': self.db.getTimeStamp()}
                        )
                    else:
                        # Otherwise delete it
                        self.db.deleteCondorJob(job['id'])

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
        # check job status
        self.checkJobs()
        # check for lost jobs
        self.checkLostJobs()
        # check for stuck jobs too long in one state and kill them
        self.checkStuckJobs()
