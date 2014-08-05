import re
import time
import arc
from urlparse import urlparse
from threading import Thread
from act.common.aCTProcess import aCTProcess

class SubmitThr(Thread):
    def __init__ (self,func,id,appjobid,jobdescs,uc,logger):
        Thread.__init__(self)
        self.func=func
        self.id=id
        self.appjobid=appjobid
        self.jobdescs = jobdescs
        self.uc = uc
        self.log=logger
        self.job = None
    def run(self):
        self.job=self.func(self.jobdescs,self.uc,self.log,self.appjobid)


def Submit(jobdescs,uc,log,appjobid):

    global queuelist

    if len(queuelist) == 0  :
        log.error("%s: no cluster free for submission" % appjobid)
        return None
    
    # Do brokering among the available queues
    jobdesc = jobdescs[0]
    broker = arc.Broker(uc, jobdesc, "Random")
    targetsorter = arc.ExecutionTargetSorter(broker)
    for target in queuelist:
        log.debug("%s: considering target %s:%s" % (appjobid, target.ComputingService.Name, target.ComputingShare.Name))

        # Adding an entity performs matchmaking and brokering
        targetsorter.addEntity(target)
    
    if len(targetsorter.getMatchingTargets()) == 0:
        log.error("%s: no clusters satisfied job description requirements" % appjobid)
        return None
        
    targetsorter.reset() # required to reset iterator, otherwise we get a seg fault
    selectedtarget = targetsorter.getCurrentTarget()
    # Job object will contain the submitted job
    job = arc.Job()
    submitter = arc.Submitter(uc)
    if submitter.Submit(selectedtarget, jobdesc, job) != arc.SubmissionStatus.NONE:
        log.error("%s: Submission failed" % appjobid)
        return None

    return job

class aCTSubmitter(aCTProcess):

    def RunThreadsSplit(self,plist,nthreads=1):
        it=0
        while it < len(plist):
            tl=[]
            for i in range(0,nthreads):
                try:
                    t=plist[it]
                    tl.append(t)
                    t.start()
                except:
                    pass
                it+=1
            errfl=False
            for t in tl:
                t.join(60.0)
                if t.isAlive() :
                    # abort due to timeout and try again
                    self.log.error("%s: submission timeout: exit and try again" % t.appjobid)
                    errfl=True
                    continue
                # updatedb
                if t.job is None:
                    #self.log.error("no jobname")
                    self.log.error("%s: no job defined for %d" % (t.appjobid, t.id))
                    errfl=True
                    continue
                jd={}
                jd['arcstate']='submitted'
                # initial offset to 1 minute to force first status check
                jd['tarcstate']=self.db.getTimeStamp(time.time()-int(self.conf.get(['jobs','checkinterval']))+120)
                # extract hostname of cluster (depends on JobID being a URL)
                self.log.info("%s: job id %s" % (t.appjobid, t.job.JobID))
                jd['cluster']=self.cluster
                self.db.updateArcJobLazy(t.id,jd,t.job)
            if errfl:
                break


    def submit(self):
        """
        Main function to submit jobs.
        """

        global queuelist

        # check for stopsubmission flag
        if self.conf.get(['downtime','stopsubmission']) == "true":
            self.log.info('Submission suspended due to downtime')
            return 0

        # Get cluster host and queue: cluster/queue
        clusterhost = self.cluster
        clusterqueue = ''
        if self.cluster.find('/') != -1:
            (clusterhost, clusterqueue) = self.cluster.split('/', 1)

        if self.cluster:
            # Lock row for update in case multiple clusters are specified
            jobs=self.db.getArcJobsInfo("arcstate='tosubmit' and clusterlist like '%"+self.cluster+"%' limit 10",
                                        columns=["id", "jobdesc", "proxyid", "appjobid"], lock=True)
            if jobs:
                self.log.debug("started lock for writing %d jobs"%len(jobs))
        else:
            jobs=self.db.getArcJobsInfo("arcstate='tosubmit' and clusterlist='' limit 10", ["id", "jobdesc", "proxyid", "appjobid"])

        # mark submitting in db
        jobs_taken=[]
        for j in jobs:
            jd={'cluster': self.cluster, 'arcstate': 'submitting', 'tarcstate': self.db.getTimeStamp()}
            try:
                self.db.updateArcJobLazy(j['id'],jd)
            except Exception,x:
                self.log.error('%s: %s' % (j['id'], x))
                continue
            jobs_taken.append(j)
        jobs=jobs_taken
 
        if self.cluster:
            self.db.Commit(lock=True)
            if jobs:
                self.log.debug("ended lock")
        else:
            self.db.Commit()

        if len(jobs) == 0:
            #self.log.debug("No jobs to submit")
            return 0
        self.log.info("Submitting %d jobs:" % len(jobs))

        # Query infosys - either local or index
        if self.cluster:
            # Endpoint and type will come from cluster table eventually
            aris = 'ldap://'+clusterhost+'/mds-vo-name=local,o=grid'
            infoendpoints = [arc.Endpoint(aris, arc.Endpoint.COMPUTINGINFO, 'org.nordugrid.ldapng')]
                          
        else:
            giises = self.conf.getList(['atlasgiis','item'])
            infoendpoints = []
            for g in giises:
                # Specify explicitly EGIIS
                infoendpoints.append(arc.Endpoint(str(g), arc.Endpoint.REGISTRY, "org.nordugrid.ldapegiis"))

        # retriever contains a list of CE endpoints
        # TODO: WS info service requires credentials
        retriever = arc.ComputingServiceRetriever(self.uc, infoendpoints)
        retriever.wait()
        # targets is the list of queues
        # parse target.ComputingService.ID for the CE hostname
        # target.ComputingShare.Name is the queue name
        targets = retriever.GetExecutionTargets()
        
        # Filter only sites for this process
        queuelist=[]
        for target in targets:
            if not target.ComputingService.ID:
                self.log.info("Target %s does not have ComputingService ID defined, skipping" % target.ComputingService.Name)
                continue
            # Check for matching host and queue
            targethost = re.sub(':arex$', '', re.sub('urn:ogf:ComputingService:', '', target.ComputingService.ID))
            targetqueue = target.ComputingShare.Name
            if clusterhost and targethost != clusterhost:
                self.log.debug('Rejecting target host %s as it does not match %s' % (targethost, clusterhost))
                continue
            if clusterqueue and targetqueue != clusterqueue:
                self.log.debug('Rejecting target queue %s as it does not match %s' % (targetqueue, clusterqueue))
                continue
            s = self.db.getSchedconfig(targethost)
            status = 'online'
            if s is not None:
                status=s['status']
            if targetqueue in self.conf.getList(['queuesreject','item']):
                self.log.debug('Rejecting target queue %s in queuesreject list' % targetqueue)
                continue
            elif targethost in self.conf.getList(['clustersreject','item']):
                self.log.debug('Rejecting target host %s in clustersreject list' % targethost)
                continue
            elif status == "XXXoffline":
                continue
            else:
                # tmp hack
                target.ComputingShare.LocalWaitingJobs = 0
                target.ComputingShare.PreLRMSWaitingJobs = 0
                target.ExecutionEnvironment.CPUClockSpeed = 2000
                qjobs=self.db.getArcJobsInfo("cluster='" +str(self.cluster)+ "' and  arcstate='submitted'", ['id'])
                rjobs=self.db.getArcJobsInfo("cluster='" +str(self.cluster)+ "' and  arcstate='running'", ['id'])

                # Set number of submitted jobs to running * 0.15 + 20
                jlimit = len(rjobs)*0.15 + 20
                target.ComputingShare.PreLRMSWaitingJobs=len(qjobs)
                if len(qjobs) < jlimit:
                    queuelist.append(target)
                    self.log.debug("Adding target %s:%s" % (targethost, targetqueue))
                else:
                    self.log.debug("%s/%s already at limit of submitted jobs" % (targethost, targetqueue))

        # check if any queues are available, if not leave and try again next time
        if not queuelist:
            self.log.info("No free queues available")
            self.db.Commit()
            return 0

        self.log.info("start submitting")

        # Just run one thread for each job in sequence. Strange things happen
        # when trying to create a new UserConfig object for each thread.
        count=0
        for j in jobs:
            self.log.debug("%s: preparing submission" % j['appjobid'])
            jobdescstr = str(self.db.getArcJobDescription(str(j['jobdesc'])))
            jobdescs = arc.JobDescriptionList()
            if not jobdescstr or not arc.JobDescription_Parse(jobdescstr, jobdescs):
                self.log.error("%s: Failed to prepare job description" % j['appjobid'])
                continue
            # Set UserConfig credential for each proxy
            self.uc.CredentialString(self.db.getProxy(j['proxyid']))
            t=SubmitThr(Submit,j['id'],j['appjobid'],jobdescs,self.uc,self.log)
            self.RunThreadsSplit([t],1)
            count=count+1

        self.log.info("threads finished")
        # commit transaction to release row locks
        self.db.Commit()

        self.log.info("end submitting")

        return count


    def checkFailedSubmissions(self):

        jobs=self.db.getArcJobsInfo("arcstate='submitting' and cluster='"+self.cluster+"'", ["id"])

        # TODO query GIIS for job name specified in description to see if job
        # was really submitted or not
        for j in jobs:
            # set to toresubmit and the application should figure out what to do
            self.db.updateArcJob(j['id'], {"arcstate": "toresubmit",
                                           "tarcstate": self.db.getTimeStamp()})

    def processToCancel(self):
        
        jobstocancel = self.db.getArcJobs("arcstate='tocancel' and cluster='"+self.cluster+"'")
        if not jobstocancel:
            return
        
        self.log.info("Cancelling %i jobs" % sum(len(v) for v in jobstocancel.values()))
        for proxyid, jobs in jobstocancel.items():
            self.uc.CredentialString(self.db.getProxy(proxyid))
                
            job_supervisor = arc.JobSupervisor(self.uc, [j[2] for j in jobs])
            job_supervisor.Update()
            job_supervisor.Cancel()
            
            notcancelled = job_supervisor.GetIDsNotProcessed()
    
            for (id, appjobid, job) in jobs:
                if job.JobID in notcancelled:
                    if job.State == arc.JobState.UNDEFINED:
                        # If longer than one hour since submission assume job never made it
                        if job.StartTime + arc.Period(3600) < arc.Time():
                            self.log.warning("%s: Assuming job %s is lost and marking as cancelled" % (appjobid, job.JobID))
                            self.db.updateArcJob(id, {"arcstate": "cancelled",
                                                      "tarcstate": self.db.getTimeStamp()})
                        else:
                            # Job has not yet reached info system
                            self.log.warning("%s: Job %s is not yet in info system so cannot be cancelled" % (appjobid, job.JobID))
                    else:
                        self.log.error("%s: Could not cancel job %s" % (appjobid, job.JobID))
                        # Just to mark as cancelled so it can be cleaned
                        self.db.updateArcJob(id, {"arcstate": "cancelled",
                                                  "tarcstate": self.db.getTimeStamp()})
                else:
                    self.db.updateArcJob(id, {"arcstate": "cancelling",
                                              "tarcstate": self.db.getTimeStamp()})

    def processToResubmit(self):
        
        jobstoresubmit = self.db.getArcJobs("arcstate='toresubmit' and cluster='"+self.cluster+"'")
 
        for proxyid, jobs in jobstoresubmit.items():
            self.uc.CredentialString(self.db.getProxy(proxyid))
            
            # Clean up jobs which were submitted
            jobstoclean = [job[2] for job in jobs if job[2].JobID]
            
            if jobstoclean:
                
                # Put all jobs to cancel, however the supervisor will only cancel
                # cancellable jobs and remove the rest so there has to be 2 calls
                # to Clean()
                job_supervisor = arc.JobSupervisor(self.uc, jobstoclean)
                job_supervisor.Update()
                self.log.info("Cancelling %i jobs" % len(jobstoclean))
                job_supervisor.Cancel()
                
                processed = job_supervisor.GetIDsProcessed()
                notprocessed = job_supervisor.GetIDsNotProcessed()
                # Clean the successfully cancelled jobs
                if processed:
                    job_supervisor.SelectByID(processed)
                    self.log.info("Cleaning %i jobs" % len(processed))
                    if not job_supervisor.Clean():
                        self.log.warning("Failed to clean some jobs")
                
                # New job supervisor with the uncancellable jobs
                if notprocessed:
                    notcancellable = [job for job in jobstoclean if job.JobID in notprocessed]
                    job_supervisor = arc.JobSupervisor(self.uc, notcancellable)
                    job_supervisor.Update()
                    
                    self.log.info("Cleaning %i jobs" % len(notcancellable))
                    if not job_supervisor.Clean():
                        self.log.warning("Failed to clean some jobs")
            
            # Empty job to reset DB info
            j = arc.Job()
            for (id, appjobid, job) in jobs:
                self.db.updateArcJob(id, {"arcstate": "tosubmit",
                                          "tarcstate": self.db.getTimeStamp(),
                                          "cluster": None}, j)

    def processToRerun(self):
        
        jobstorerun = self.db.getArcJobs("arcstate='torerun' and cluster='"+self.cluster+"'")
        if not jobstorerun:
            return

        # TODO: downtimes from AGIS
        if self.conf.get(['downtime', 'srmdown']) == 'True':
            self.log.info('SRM down, not rerunning')
            return

        self.log.info("Resuming %i jobs" % sum(len(v) for v in jobstorerun.values()))
        for proxyid, jobs in jobstorerun.items():
            self.uc.CredentialString(self.db.getProxy(proxyid))
    
            job_supervisor = arc.JobSupervisor(self.uc, [j[2] for j in jobs])
            job_supervisor.Update()
            # Renew proxy to be safe
            job_supervisor.Renew()
            job_supervisor = arc.JobSupervisor(self.uc, [j[2] for j in jobs])
            job_supervisor.Update()
            job_supervisor.Resume()
            
            notresumed = job_supervisor.GetIDsNotProcessed()
    
            for (id, appjobid, job) in jobs:
                if job.JobID in notresumed:
                    self.log.error("%s: Could not resume job %s" % (appjobid, job.JobID))
                    self.db.updateArcJob(id, {"arcstate": "failed",
                                              "tarcstate": self.db.getTimeStamp()})
                else:
                    # Force a wait before next status check, to allow the
                    # infosys to update and avoid the failed state being picked
                    # up again
                    self.db.updateArcJob(id, {"arcstate": "submitted",
                                              "tarcstate": self.db.getTimeStamp(time.time()+600)})


    def process(self):

        # check jobs which failed to submit the previous loop
        self.checkFailedSubmissions()
        # process jobs which have to be cancelled
        self.processToCancel()
        # process jobs which have to be resubmitted
        self.processToResubmit()
        # process jobs which have to be rerun
        self.processToRerun()
        # submit new jobs
        while self.submit():
            continue


# Main
if __name__ == '__main__':
    asb=aCTSubmitter()
    asb.run()
    asb.finish()
    
