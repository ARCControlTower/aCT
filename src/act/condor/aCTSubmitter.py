import ast
import re
import time
import htcondor

from threading import Thread
from act.common.aCTProcess import aCTProcess

class SubmitThr(Thread):
    def __init__ (self, func, id, appjobid, jobdesc, logger, schedd):
        Thread.__init__(self)
        self.func = func
        self.id = id
        self.appjobid = appjobid
        self.jobdesc = jobdesc
        self.log = logger
        self.schedd = schedd
        self.jobid = None
    def run(self):
        self.jobid = self.func(self.jobdesc, self.log, self.appjobid, self.schedd)


def Submit(jobdesc, log, appjobid, schedd):

    global queuelist

    if len(queuelist) == 0  :
        log.error("%s: no cluster free for submission" % appjobid)
        return None

    # This method only works with condor version >= 8.5.8 but is needed to
    # get $() variable expansion working
    sub = htcondor.Submit(dict(jobdesc))
    with schedd.transaction() as txn:
        jobid = sub.queue(txn)
    return jobid


class aCTSubmitter(aCTProcess):
    
    def __init__(self):
        aCTProcess.__init__(self)
        self.schedd = htcondor.Schedd()

    def RunThreadsSplit(self, plist, nthreads=1):
        it = 0
        while it < len(plist):
            tl = []
            for i in range(0, nthreads):
                try:
                    t = plist[it]
                    tl.append(t)
                    t.start()
                except:
                    pass
                it += 1
            errfl = False
            for t in tl:
                t.join(60.0)
                if t.isAlive() :
                    # abort due to timeout and try again
                    self.log.error("%s: submission timeout: exit and try again" % t.appjobid)
                    errfl = True
                    continue
                # updatedb
                if t.jobid is None:
                    self.log.error("%s: no job defined for %d" % (t.appjobid, t.id))
                    errfl = True
                    continue
                jd = {}
                jd['condorstate'] = 'submitted'
                # initial offset to 1 minute to force first status check
                jd['tcondorstate'] = self.dbcondor.getTimeStamp(time.time() - int(self.conf.get(['jobs', 'checkinterval'])) + 120)
                jd['cluster'] = self.cluster
                jd['ClusterId'] = t.jobid
                self.log.info("%s: Job submitted with ClusterId %d" % (t.appjobid, t.jobid))
                self.dbcondor.updateCondorJobLazy(t.id, jd)
            if errfl:
                break


    def submit(self):
        """
        Main function to submit jobs.
        """

        if not self.cluster:
            self.log.error('Cluster must be defined for condor jobs')
            return 0
        
        global queuelist

        # check for stopsubmission flag
        if self.conf.get(['downtime','stopsubmission']) == "true":
            self.log.info('Submission suspended due to downtime')
            return 0

        # Apply fair-share
        fairshares = self.dbcondor.getCondorJobsInfo("condorstate='tosubmit' and clusterlist like '%"+self.cluster+"%'", ['fairshare'])
            
        if not fairshares:
            self.log.info('Nothing to submit')
            return 0
        
        fairshares = set([p['fairshare'] for p in fairshares])
        count = 0

        for fairshare in fairshares:

            try:
                # catch any exceptions here to avoid leaving lock
                # Lock row for update in case multiple clusters are specified
                jobs = self.dbcondor.getCondorJobsInfo("condorstate='tosubmit' and ( clusterlist like '% {0}%' or clusterlist like '%{0},%' ) and fairshare='{1}' limit 10".format(self.cluster, fairshare),
                                            columns=["id", "jobdesc", "appjobid", "priority", "proxyid", "clusterlist"], lock=True)
                if jobs:
                    self.log.debug("started lock for writing %d jobs" % len(jobs))

                # mark submitting in db
                jobs_taken = []
                for j in jobs:
                    jd = {'cluster': self.cluster, 'condorstate': 'submitting', 'tcondorstate': self.dbcondor.getTimeStamp()}
                    self.dbcondor.updateCondorJobLazy(j['id'], jd)
                    jobs_taken.append(j)
                jobs = jobs_taken

            finally:
                try:
                    self.dbcondor.Commit(lock=True)
                    self.log.debug("ended lock")
                except:
                    self.log.warning("Failed to release DB lock")

            if len(jobs) == 0:
                #self.log.debug("No jobs to submit")
                continue
            self.log.info("Submitting %d jobs for fairshare %s" % (len(jobs), fairshare))

            # max waiting priority
            try:
                maxpriowaiting = max(jobs,key = lambda x : x['priority'])['priority']
            except:
                maxpriowaiting = 0
            self.log.info("Maximum priority of waiting jobs: %d" % maxpriowaiting)

            # Filter only sites for this process
            queuelist = []

            # Check queued jobs and limits
            qjobs=self.dbcondor.getCondorJobsInfo("cluster='" +str(self.cluster)+ "' and ( condorstate='submitted' or condorstate='holding' ) and fairshare='%s'" % fairshare, ['id', 'priority'])
            rjobs=self.dbcondor.getCondorJobsInfo("cluster='" +str(self.cluster)+ "' and condorstate='running' and fairshare='%s'" % fairshare, ['id'])
                    
            # max queued priority
            try:
                maxprioqueued = max(qjobs,key = lambda x : x['priority'])['priority']
            except:
                maxprioqueued = 0
            self.log.info("Max priority queued: %d" % maxprioqueued)

            # Set number of submitted jobs to running * 0.15 + 400/num of shares
            # Note: assumes only a few shares are used
            jlimit = len(rjobs)*0.15 + 400/len(fairshares)

            if len(qjobs) < jlimit or ( ( maxpriowaiting > maxprioqueued ) and ( maxpriowaiting > 10 ) ) :
                if maxpriowaiting > maxprioqueued :
                    self.log.info("Overriding limit, maxpriowaiting: %d > maxprioqueued: %d" % (maxpriowaiting, maxprioqueued))
                queuelist.append(self.cluster)
                self.log.debug("Adding target %s" % (self.cluster))
            else:
                self.log.info("%s already at limit of submitted jobs for fairshare %s" % (self.cluster, fairshare))

            # check if any queues are available, if not leave and try again next time
            if not queuelist:
                self.log.info("No free queues available")
                self.dbcondor.Commit()
                continue
    
            self.log.info("start submitting")
    
            # Just run one thread for each job in sequence.
            for j in jobs:
                self.log.debug("%s: preparing submission" % j['appjobid'])
                jobdescstr = self.dbcondor.getCondorJobDescription(str(j['jobdesc']))
                try:
                    # Not so nice using eval but condor doesn't accept unicode
                    # strings returned from json.loads()
                    jobdesc = ast.literal_eval(jobdescstr)
                except:
                    self.log.error("%s: Failed to prepare job description" % j['appjobid'])
                    continue

                # Extract the GridResource
                # CREAM has the queue at the end of the GridResource, for condor it's a separate attribute
                gridresource = re.search(r',*([^,]* %s[^,]*),*' % self.cluster, j['clusterlist'])
                gridresource = str(gridresource.group(1))
                queue = gridresource.split()[-1]
                if gridresource.startswith('condor'):
                    gridresource = re.sub(r' %s$' % queue, '', gridresource)
                jobdesc['GridResource'] = gridresource
                # Set the remote queue
                jobdesc['+queue'] = '"%s"' % queue
                # Set tag for aCTStatus to query
                jobdesc['+ACTCluster'] = '"%s"' % self.cluster
                self.log.debug('%s: Set GridResource to %s, queue %s' % (j['appjobid'], gridresource, queue))
                self.log.debug(jobdesc)
                t = SubmitThr(Submit, j['id'], j['appjobid'], jobdesc, self.log, self.schedd)
                self.RunThreadsSplit([t], 1)
                count += 1
    
            self.log.info("threads finished")
            # commit transaction to release row locks
            self.dbcondor.Commit()

        self.log.info("end submitting")

        return count


    def checkFailedSubmissions(self):

        jobs = self.dbcondor.getCondorJobsInfo("condorstate='submitting' and cluster='"+self.cluster+"'",
                                               ["id"])

        for job in jobs:
            # set to toresubmit and the application should figure out what to do
            self.dbcondor.updateCondorJob(job['id'], {"condorstate": "toresubmit",
                                                      "tcondorstate": self.dbcondor.getTimeStamp()})

    def processToCancel(self):
        
        jobstocancel = self.dbcondor.getCondorJobsInfo("condorstate='tocancel' and cluster='"+self.cluster+"'",
                                                       ['id', 'appjobid', 'ClusterId'])
        if not jobstocancel:
            return
        
        for job in jobstocancel:
	    self.log.info("%s: Cancelling condor job" % job['appjobid'])
    
            if not job['ClusterId']:
                # Job not submitted
                self.log.info("%s: Marking unsubmitted job cancelled" % job['appjobid'])
                self.dbcondor.updateCondorJob(job['id'], {"condorstate": "cancelled",
                                                          "tcondorstate": self.dbcondor.getTimeStamp()})
                continue

            try:
                remove = self.schedd.act(htcondor.JobAction.Remove, ['%d.0' % job['ClusterId']])
            except RuntimeError as e:
                self.log.error("%s: Failed to cancel in condor: %s" % (job['appjobid'], str(e)))
                continue
            self.log.debug("%s: Cancellation returned %s" % (job['appjobid'], remove))
            self.dbcondor.updateCondorJob(job['id'], {"condorstate": "cancelling",
                                                      "tcondorstate": self.dbcondor.getTimeStamp()})
            # TODO deal with failed cancel
            continue


    def processToResubmit(self):
        
        jobstoresubmit = self.dbcondor.getCondorJobsInfo("condorstate='toresubmit' and cluster='"+self.cluster+"'",
                                                         ['id', 'appjobid', 'ClusterId'])

        for job in jobstoresubmit:

            # Clean up jobs which were submitted
            if job['ClusterId']:
                try:
                    self.schedd.act(htcondor.JobAction.Remove, [str(job['ClusterId'])])
                except RuntimeError as e:
                    self.log.error("%s: Failed to cancel in condor: %s" % (job['appjobid'], str(e)))
                # TODO handle failed clean

            self.dbcondor.updateCondorJob(job['id'], {"condorstate": "tosubmit",
                                                      "tcondorstate": self.dbcondor.getTimeStamp(),
                                                      "cluster": None,
                                                      "ClusterId": None})


    def process(self):

        # check jobs which failed to submit the previous loop
        self.checkFailedSubmissions()
        # process jobs which have to be cancelled
        self.processToCancel()
        # process jobs which have to be resubmitted
        self.processToResubmit()
        # submit new jobs
        while self.submit():
            continue


# Main
if __name__ == '__main__':
    asb=aCTSubmitter()
    asb.run()
    asb.finish()
    
