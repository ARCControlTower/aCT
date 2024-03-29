# aCTStatus.py
#
# Process to check the status of running Condor jobs
#
import time
import htcondor
import classad

from act.common.aCTProcess import aCTProcess

class aCTStatus(aCTProcess):
    '''
    Class for checking the status of submitted Condor jobs and updating their
    status in the DB.
    '''

    def __init__(self):

        aCTProcess.__init__(self)

        self.schedd = htcondor.Schedd()

        self.condorjobstatemap = {0: 'Undefined', # used before real state is known
                                  1: 'Idle',
                                  2: 'Running',
                                  3: 'Removed',
                                  4: 'Completed',
                                  5: 'Held',
                                  6: 'Transferring Output',
                                  7: 'Suspended'}

        # store the last checkJobs time to avoid overloading of GIIS
        self.checktime=time.time()


    def checkJobs(self):
        '''
        Query all running jobs
        '''

        # minimum time between checks
        if time.time() < self.checktime + self.conf.jobs.checkmintime:
            self.log.debug("mininterval not reached")
            return
        self.checktime = time.time()

        # check jobs which were last checked more than checkinterval ago
        query = "condorstate in ('submitted', 'running', 'cancelling', 'holding') and " \
                "ClusterId not like '' and cluster='"+self.cluster+"' and "+ \
                self.dbcondor.timeStampLessThan("tcondorstate", self.conf.jobs.checkinterval) + \
                " limit 100000"
        jobstocheck = self.dbcondor.getCondorJobsInfo(query, columns=['id', 'appjobid', 'JobStatus', 'ClusterId'])

        if not jobstocheck:
            return
        self.log.info("%d jobs to check" % len(jobstocheck))

        # Query condor for all jobs with this cluster
        # Add here attributes we eventually want in the DB
        attrs = ['JobStatus', 'ExitCode', 'GlobalJobId', 'GridJobId', 'JobCurrentStartDate', 'CompletionDate']
        # Here with attributes we want to query but not store
        qattrs = attrs + ['ClusterId', 'GridResourceUnavailableTime']
        t1 = time.time()
        try:
            status = self.schedd.xquery(requirements='ACTCluster=?="%s"' % self.cluster,
                                        projection=qattrs)
        except IOError as e:
            self.log.error('Failed querying schedd: %s' % str(e))
            return
        condorstatuses = {}
        while True:
            try:
                stat = next(status)
                condorstatuses[stat['ClusterId']] = stat
            except StopIteration:
                break
            except RuntimeError as e: # Usually a timeout connecting to remote host, try again
                self.log.error('Problem querying schedd: %s' % str(e))
                break
        t2 = time.time()
        self.log.debug('took %f to query schedd (returning %d results)' % ((t2-t1), len(condorstatuses)))
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
                self.log.debug('%s: Job not in condor queue, checking history' % appjobid)
                history = self.schedd.history('ClusterID=?=%d' % clusterid, qattrs, 1)
                try:
                    hist = next(history)
                except StopIteration:
                    self.log.warning('%s: Job id %d not found!' % (appjobid, clusterid))
                    continue
                except RuntimeError as e: # Usually a timeout connecting to remote host
                    self.log.error('%s: Problem getting history: %s' % (appjobid, str(e)))
                    continue
                updatedjob = hist
                jobstatus = updatedjob['JobStatus']

            self.log.debug('%s: Job %d: Status %d (%s)' % (appjobid, clusterid, jobstatus, self.condorjobstatemap[jobstatus]))

            if jobstatus == 1 and 'GridResourceUnavailableTime' in updatedjob:
                # Job could not be submitted due to remote CE being down
                self.log.warning('%s: Could not submit to remote CE, will retry' % appjobid)
                jobdesc = {}
                jobdesc['condorstate'] = 'toresubmit'
                jobdesc['JobStatus'] = 0
                jobdesc['tcondorstate'] = self.dbcondor.getTimeStamp()
                jobdesc['tstate'] = self.dbcondor.getTimeStamp()
                self.dbcondor.updateCondorJob(job['id'], jobdesc)
                continue

            if oldstatus == jobstatus:
                # just update timestamp
                self.dbcondor.updateCondorJob(job['id'], {'tcondorstate': self.dbcondor.getTimeStamp()})
                continue

            self.log.info("%s: Job %d: %s -> %s" % (appjobid, clusterid,
                                                    self.condorjobstatemap[oldstatus],
                                                    self.condorjobstatemap[jobstatus]))

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
            jobdesc = dict([(k,v) for (k,v) in updatedjob.items() if k in attrs and v != classad.Value.Undefined])
            # Undefined is 2 in condor which means JobStatus running is ignored
            jobdesc['JobStatus'] = updatedjob['JobStatus']
            jobdesc['condorstate'] = condorstate
            jobdesc['tcondorstate'] = self.dbcondor.getTimeStamp()
            jobdesc['tstate'] = self.dbcondor.getTimeStamp()
            jobdesc['CompletionDate'] = self.dbcondor.getTimeStamp(jobdesc.get('CompletionDate', 0))
            jobdesc['JobCurrentStartDate'] = self.dbcondor.getTimeStamp(jobdesc.get('JobCurrentStartDate', 0))
            self.log.debug(str(jobdesc))
            self.dbcondor.updateCondorJob(job['id'], jobdesc)

        self.log.info('Done')

    def checkLostJobs(self):
        '''
        Move jobs with a long time since status update to lost
        '''

        # 2 days limit. TODO: configurable?
        jobs=self.dbcondor.getCondorJobsInfo("condorstate in ('submitted', 'running', 'cancelling', 'finished') and " \
                                       "cluster='"+self.cluster+"' and "+self.dbcondor.timeStampLessThan("tcondorstate", 172800),
                                       ['id', 'appjobid', 'ClusterId', 'condorstate'])

        for job in jobs:
            if job['condorstate'] == 'cancelling':
                self.log.warning("%s: Job %s lost from information system, marking as cancelled" % (job['appjobid'], job['ClusterId']))
                self.dbcondor.updateCondorJob(job['id'], {'condorstate': 'cancelled', 'tcondorstate': self.dbcondor.getTimeStamp()})
            else:
                self.log.warning("%s: Job %s lost from information system, marking as lost" % (job['appjobid'], job['ClusterId']))
                self.dbcondor.updateCondorJob(job['id'], {'condorstate': 'lost', 'tcondorstate': self.dbcondor.getTimeStamp()})


    def checkStuckJobs(self):
        '''
        check jobs with tstate too long ago and set them tocancel
        maxtimestate can be set in arc config file for any condor JobStatus,
        e.g. maxtimeidle, maxtimerunning
        '''

        # Loop over possible states
        # Note: MySQL is case-insensitive. Need to watch out with other DBs
        for jobstateid, jobstate in self.condorjobstatemap.items():
            maxtime = self.conf.jobs.get(f"maxtime{jobstate.lower()}")
            if not maxtime:
                continue

            # be careful not to cancel jobs that are stuck in cleaning
            select = "JobStatus='%s' and %s" % (jobstateid, self.dbcondor.timeStampLessThan("tstate", maxtime))
            jobs = self.dbcondor.getCondorJobsInfo(select, columns=['id', 'ClusterId', 'appjobid', 'condorstate'])

            for job in jobs:
                if job['condorstate'] == 'toclean' or job['condorstate'] == 'cancelling':
                    # mark as cancelled jobs stuck in toclean/cancelling
                    self.log.info("%s: Job stuck in toclean/cancelling for too long, marking cancelled" % (job['appjobid']))
                    self.dbcondor.updateCondorJob(job['id'], {'condorstate': 'cancelled',
                                                              'tcondorstate': self.dbcondor.getTimeStamp(),
                                                              'tstate': self.dbcondor.getTimeStamp()})
                    continue

                self.log.warning("%s: Job %s too long in state %s, cancelling" % (job['appjobid'], job['ClusterId'], jobstate))
                if job['ClusterId']:
                    # If jobid is defined, cancel
                    self.dbcondor.updateCondorJob(job['id'], {'condorstate': 'tocancel',
                                                              'tcondorstate': self.dbcondor.getTimeStamp(),
                                                              'tstate': self.dbcondor.getTimeStamp()})
                else:
                    # Otherwise delete it
                    self.dbcondor.deleteCondorJob(job['id'])


    def process(self):
        # check job status
        self.checkJobs()
        # check for lost jobs
        self.checkLostJobs()
        # check for stuck jobs too long in one state and kill them
        self.checkStuckJobs()

if __name__ == '__main__':
    st = aCTStatus()
    st.run()
    st.finish()
