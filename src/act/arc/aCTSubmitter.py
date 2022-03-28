import datetime
import http.client
import logging
import multiprocessing
import os
import re
import signal
import ssl
import time
from random import shuffle
from urllib.parse import urlparse

from act.arc.rest import (cleanJobs, getJobsDelegations, killJobs,
                          renewDelegation, restartJobs, submitJobs)
from act.common.aCTProcess import aCTProcess
from act.common.exceptions import ACTError, SubmitError

import arc


def KillPool(pool):
    # stop repopulating new child
    pool._state = multiprocessing.pool.TERMINATE
    pool._worker_handler._state = multiprocessing.pool.TERMINATE
    for p in pool._pool:
        try:
            os.kill(p.pid, signal.SIGKILL)
        except:
            pass
    # .is_alive() will reap dead process
    while any(p.is_alive() for p in pool._pool):
        pass
    pool.terminate()

class JobConv:

    def __init__(self):
        self.jobattrmap = {int: 'integer',
                      str: 'varchar(255)',
                      arc.JobState: 'varchar(255)',
                      arc.StringList: 'varchar(1024)',
                      arc.URL: 'varchar(255)',
                      arc.Period: 'int',
                      arc.Time: 'datetime',
                      arc.StringStringMap: 'varchar(1024)'}
        ignoremems=['STDIN',
                    'STDOUT',
                    'STDERR',
                    'STAGEINDIR',
                    'STAGEOUTDIR',
                    'SESSIONDIR',
                    'JOBLOG',
                    'JOBDESCRIPTION',
                    'JobDescriptionDocument']

        # Attributes of Job class mapped to DB column type
        self.jobattrs={}
        j=arc.Job()
        for i in dir(j):
            if re.match('^__',i):
                continue
            if i in ignoremems:
                continue
            if type(getattr(j, i)) in self.jobattrmap:
                self.jobattrs[i] = type(getattr(j, i))

    def db2job(self,dbinfo):
        '''
        Convert a dictionary of DB key value into arc Job object
        '''
        if dbinfo is None:
            return None
        j = arc.Job()
        for attr in self.jobattrs:
            if attr not in dbinfo or dbinfo[attr] is None:
                continue
            # Some object types need special treatment
            if self.jobattrs[attr] == arc.StringList:
                l = arc.StringList()
                for item in dbinfo[attr].split('|'):
                    l.append(item)
                setattr(j, attr, l)
                continue
            if self.jobattrs[attr] == arc.StringStringMap:
                m = arc.StringStringMap()
                d = eval(dbinfo[attr])
                if not isinstance(d, dict):
                    continue
                for (k,v) in d.items():
                    m[k] = v
                setattr(j, attr, m)
                continue

            setattr(j, attr, self.jobattrs[attr](str(dbinfo[attr])))
        return j

    def job2db(self,job):
        '''
        Convert an arc Job object to a dictionary of column name: value
        '''
        d = {}
        for attr in self.jobattrs:
            if self.jobattrs[attr] == int or self.jobattrs[attr] == str:
                d[attr] = str(getattr(job, attr))[:250]
            elif self.jobattrs[attr] == arc.JobState:
                d[attr] = getattr(job, attr).GetGeneralState()
            elif self.jobattrs[attr] == arc.StringList:
                d[attr] = '|'.join(getattr(job, attr))[:1000]
            elif self.jobattrs[attr] == arc.URL:
                d[attr] = getattr(job, attr).str().replace(r'\2f',r'/')
            elif self.jobattrs[attr] == arc.Period:
                d[attr] = str(getattr(job, attr).GetPeriod())
            elif self.jobattrs[attr] == arc.Time:
                if getattr(job, attr).GetTime() != -1:
                    # Use UTC time but strip trailing Z since mysql doesn't like it
                    t = str(getattr(job, attr).str(arc.UTCTime))
                    d[attr] = re.sub('Z$', '', t)
            elif self.jobattrs[attr] == arc.StringStringMap:
                ssm = getattr(job, attr)
                tmpdict = dict(zip(ssm.keys(), ssm.values()))
                d[attr] = str(tmpdict)[:1000]
            # Force everything to ASCII
                        # Force everything to ASCII
            if attr in d:
                d[attr] = ''.join([i for i in d[attr] if ord(i) < 128])
        return d

def Submit(id, appjobid, jobdescstr, ucproxy, cluster, fairshare, fairshares, nqjobs, nrjobs, maxpriowaiting, maxprioqueued, qfraction, qoffset, timeout):

    global queuelist
    global usercred

    clusterurl = arc.URL(cluster)
    clusterhost = clusterurl.Host()
    clusterqueue = clusterurl.Path()[1:] # strip off leading slash

    # get the submission logger
    ###AF logging.basicConfig(filename='aCTSubmitter-%s' % clusterhost, level=logging.DEBUG)
    #log = logging.getLogger()
    log = logging

    # retriever moved here
    if len(queuelist) == 0:
            # Query infosys - either local or index
            if cluster:
                if cluster.find('://') != -1:
                    aris = arc.URL(cluster)
                else:
                    aris = arc.URL('gsiftp://%s' % cluster)
                if aris.Protocol() == 'https':
                    aris.ChangePath('/arex')
                    # tmp filter for REST, to remove when EMI-ES no longer necessary
                    if fairshare in ['ARC-TEST','Vega','Vega_MCORE','Vega_largemem']:
                        infoendpoints = [arc.Endpoint(aris.str(), arc.Endpoint.COMPUTINGINFO, 'org.nordugrid.arcrest')]
                    else:
                        infoendpoints = [arc.Endpoint(aris.str(), arc.Endpoint.COMPUTINGINFO, 'org.ogf.glue.emies.resourceinfo')]
                elif aris.Protocol() == 'local':
                    infoendpoints = [arc.Endpoint(aris.str(), arc.Endpoint.COMPUTINGINFO, 'org.nordugrid.local')]
                else:
                    aris = 'ldap://'+aris.Host()+'/mds-vo-name=local,o=grid'
                    infoendpoints = [arc.Endpoint(aris, arc.Endpoint.COMPUTINGINFO, 'org.nordugrid.ldapng')]

            # retriever contains a list of CE endpoints
            uc=usercred
            uc.CredentialString(ucproxy)
            retriever = arc.ComputingServiceRetriever(uc, infoendpoints)
            retriever.wait()
            # targets is the list of queues
            # parse target.ComputingService.ID for the CE hostname
            # target.ComputingShare.Name is the queue name
            targets = retriever.GetExecutionTargets()


            # Filter only sites for this process
            for target in targets:
                if not target.ComputingService.ID:
                    log.info("Target %s does not have ComputingService ID defined, skipping" % target.ComputingService.Name)
                    continue
                # If EMI-ES infoendpoint, force EMI-ES submission
                if infoendpoints[0].InterfaceName == 'org.ogf.glue.emies.resourceinfo' and target.ComputingEndpoint.InterfaceName != 'org.ogf.glue.emies.activitycreation':
                    log.debug("Rejecting target interface %s because not EMI-ES" % target.ComputingEndpoint.InterfaceName)
                    continue
                # If REST infoendpoint, force REST submission
                if infoendpoints[0].InterfaceName == 'org.nordugrid.arcrest' and target.ComputingEndpoint.InterfaceName != 'org.nordugrid.arcrest':
                    log.debug("Rejecting target interface %s because not REST" % target.ComputingEndpoint.InterfaceName)
                    continue
                # Check for matching host and queue
                targethost = re.sub(':arex$', '', re.sub('urn:ogf:ComputingService:', '', target.ComputingService.ID))
                targetqueue = target.ComputingShare.Name
                if clusterhost and targethost != clusterhost:
                    log.debug('Rejecting target host %s as it does not match %s' % (targethost, clusterhost))
                    continue
                if clusterqueue and targetqueue != clusterqueue:
                    log.debug('Rejecting target queue %s as it does not match %s' % (targetqueue, clusterqueue))
                    continue
                #if targetqueue in self.conf.getList(['queuesreject','item']):
                #    #self.log.debug('Rejecting target queue %s in queuesreject list' % targetqueue)
                #    continue
                #elif targethost in self.conf.getList(['clustersreject','item']):
                #    log.debug('Rejecting target host %s in clustersreject list' % targethost)
                #    continue
                if False:
                    pass
                else:
                    # tmp hack
                    target.ComputingShare.LocalWaitingJobs = 0
                    target.ComputingShare.PreLRMSWaitingJobs = 0
                    target.ExecutionEnvironment.CPUClockSpeed = 2000

                    # Limit number of submitted jobs using configuration or default (0.15 + 100/num of shares)
                    # Note: assumes only a few shares are used
                    jlimit = nrjobs * qfraction + qoffset/len(fairshares)
                    log.debug("running %d, queued %d, queue limit %d" % (nrjobs, nqjobs, jlimit))
                    if str(cluster).find('arc-boinc-0') != -1:
                        jlimit = nrjobs*0.15 + 400
                    if str(cluster).find('vega') != -1:
                        jlimit = nrjobs*0.15 + 2000
                    if str(cluster).find('arc05.lcg') != -1:
                        jlimit = nrjobs*0.15 + 400
                    target.ComputingShare.PreLRMSWaitingJobs=nqjobs
                    if nqjobs < jlimit or ( ( maxpriowaiting > maxprioqueued ) and ( maxpriowaiting > 10 ) ) :
                        if maxpriowaiting > maxprioqueued :
                            log.info("Overriding limit, maxpriowaiting: %d > maxprioqueued: %d" % (maxpriowaiting, maxprioqueued))
                        queuelist.append(target)
                        log.debug("Adding target %s:%s" % (targethost, targetqueue))
                    else:
                        log.info("%s/%s already at limit of submitted jobs for fairshare %s" % (targethost, targetqueue, fairshare))

            # check if any queues are available, if not leave and try again next time
            #if not queuelist:
            #    self.log.info("No free queues available")
            #    self.db.Commit()
            #    continue



    if len(queuelist) == 0  :
        log.error("%s: no cluster free for submission" % appjobid)
        return None

    uc=usercred
    uc.CredentialString(ucproxy)

    jobdescs = arc.JobDescriptionList()
    if not jobdescstr or not arc.JobDescription_Parse(jobdescstr, jobdescs):
        log.error("%s: Failed to prepare job description" % appjobid)
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

    jconv = JobConv()
    return jconv.job2db(job)

class aCTSubmitter(aCTProcess):

    def submit(self):
        """
        Main function to submit jobs.
        """

        global queuelist

        # check for stopsubmission flag
        if self.conf.get(['downtime','stopsubmission']) == "true":
            self.log.info('Submission suspended due to downtime')
            return

        # check for any site-specific limits or status
        clusterstatus = self.conf.getCond(["sites", "site"], f"endpoint={self.cluster}", ["status"]) or 'online'
        if clusterstatus == 'offline':
            self.log.info('Site status is offline')
            return

        clustermaxjobs = int(self.conf.getCond(["sites", "site"], f"endpoint={self.cluster}", ["maxjobs"]) or 999999)
        nsubmitted = self.db.getNArcJobs(f"cluster='{self.cluster}'")
        if nsubmitted >= clustermaxjobs:
            self.log.info(f'{nsubmitted} submitted jobs is greater than or equal to max jobs {clustermaxjobs}')
            return

        # Apply fair-share
        if self.cluster:
            fairshares = self.db.getArcJobsInfo("arcstate='tosubmit' and clusterlist like '%"+self.cluster+"%'", ['fairshare', 'proxyid'])
        else:
            fairshares = self.db.getArcJobsInfo("arcstate='tosubmit' and clusterlist=''", ['fairshare', 'proxyid'])

        if not fairshares:
            self.log.info('Nothing to submit')
            return

        # split by proxy for GU queues
        fairshares = list(set([(p['fairshare'], p['proxyid']) for p in fairshares]))
        # For proxy bug - see below
        shuffle(fairshares)

        # apply maxjobs limit per submitter (check above should make sure greater than zero)
        nsubmitters = int(self.conf.getCond(["sites", "site"], f"endpoint={self.cluster}", ["submitters"]) or 1)
        limit = min(clustermaxjobs - nsubmitted, 100) // nsubmitters
        if limit == 0:
            self.log.info(f'{clustermaxjobs} maxjobs - {nsubmitted} submitted is smaller than {nsubmitters} submitters')
            return

        # Divide limit among fairshares, unless exiting after first loop due to
        # proxy bug, but make sure at least one job is submitted
        if len(self.db.getProxiesInfo('TRUE', ['id'])) == 1:
            limit = max(limit // len(fairshares), 1)

        for fairshare, proxyid in fairshares:

            # Exit loop if above limit
            if nsubmitted >= clustermaxjobs:
                self.log.info("CE is at limit of %s submitted jobs, exiting" % clustermaxjobs)
                break

            try:
                # catch any exceptions here to avoid leaving lock
                if self.cluster:
                    # Lock row for update in case multiple clusters are specified
                    #jobs=self.db.getArcJobsInfo("arcstate='tosubmit' and ( clusterlist like '%{0}' or clusterlist like '%{0},%' ) and fairshare='{1}' order by priority desc limit 10".format(self.cluster, fairshare),
                    jobs=self.db.getArcJobsInfo("arcstate='tosubmit' and ( clusterlist like '%{0}' or clusterlist like '%{0},%' ) and fairshare='{1}' and proxyid='{2}' limit {3}".format(self.cluster, fairshare, proxyid, limit),
                                                columns=["id", "jobdesc", "appjobid", "priority", "proxyid", "clusterlist"], lock=True)
                    if jobs:
                        self.log.debug("started lock for writing %d jobs"%len(jobs))
                else:
                    jobs=self.db.getArcJobsInfo("arcstate='tosubmit' and clusterlist='' and fairshare='{0} and proxyid={1}' limit {2}".format(fairshare, proxyid, limit),
                                                columns=["id", "jobdesc", "appjobid", "priority", "proxyid", "clusterlist"])
                # mark submitting in db
                jobs_taken=[]
                for j in jobs:
                    jd={'cluster': self.cluster, 'arcstate': 'submitting', 'tarcstate': self.db.getTimeStamp()}
                    self.db.updateArcJobLazy(j['id'],jd)
                    jobs_taken.append(j)
                jobs=jobs_taken

            finally:
                if self.cluster:
                    try:
                        self.db.Commit(lock=True)
                        self.log.debug("ended lock")
                    except:
                        self.log.warning("Failed to release DB lock")
                else:
                    self.db.Commit()

            if len(jobs) == 0:
                #self.log.debug("No jobs to submit")
                continue
            self.log.info("Submitting %d jobs for fairshare %s and proxyid %d" % (len(jobs), fairshare, proxyid))

            # Set UserConfig credential for querying infosys
            proxystring = str(self.db.getProxy(proxyid))
            self.uc.CredentialString(proxystring)
            global usercred
            usercred = self.uc

            # Filter only sites for this process
            queuelist=[]
            qjobs=self.db.getArcJobsInfo("cluster='" +str(self.cluster)+ "' and  arcstate='submitted' and fairshare='%s'" % fairshare, ['id','priority'])
            rjobs=self.db.getArcJobsInfo("cluster='" +str(self.cluster)+ "' and  arcstate='running' and fairshare='%s'" % fairshare, ['id'])

            # max waiting priority
            try:
                maxpriowaiting = max(jobs,key = lambda x : x['priority'])['priority']
            except:
                maxpriowaiting = 0
            self.log.info("Maximum priority of waiting jobs: %d" % maxpriowaiting)


            # max queued priority
            try:
                maxprioqueued = max(qjobs,key = lambda x : x['priority'])['priority']
            except:
                maxprioqueued = 0
            self.log.info("Max priority queued: %d" % maxprioqueued)

            qfraction = float(self.conf.get(['jobs', 'queuefraction'])) if self.conf.get(['jobs', 'queuefraction']) else 0.15
            qoffset = int(self.conf.get(['jobs', 'queueoffset'])) if self.conf.get(['jobs', 'queueoffset']) else 100

            self.log.info("start submitting")

            ## Just run one thread for each job in sequence. Strange things happen
            ## when trying to create a new UserConfig object for each thread.
            #tasks = []
            #for j in jobs:
            #    self.log.debug("%s: preparing submission" % j['appjobid'])
            #    jobdescstr = str(self.db.getArcJobDescription(str(j['jobdesc'])))
            #    jobdescs = arc.JobDescriptionList()
            #    if not jobdescstr or not arc.JobDescription_Parse(jobdescstr, jobdescs):
            #        self.log.error("%s: Failed to prepare job description" % j['appjobid'])
            #        continue
            #    tasks.append((j['id'], j['appjobid'], jobdescstr, proxystring, cluster, fairshare, fairshares, len(qjobs), len(rjobs), maxpriowaiting, maxprioqueued, qfraction, qoffset, int(self.conf.get(['atlasgiis','timeout'])) ))

            #npools=1
            #if any(s in self.cluster for s in self.conf.getList(['parallelsubmit','item'])):
            #    npools=int(self.conf.get(['parallelsubmit','npools']))
            #self.log.debug("Starting submitters: %s" % npools)

            #pool = multiprocessing.Pool(npools)
            ## Submit in workers
            #results = [pool.apply_async(Submit, (t)) for t in tasks]

            ## timeout per submission
            #timeout = 60
            #stopflag = False
            #for result,task in zip(results,tasks):
            #    try:
            #        jdb = result.get(timeout)
            #        jconv = JobConv()
            #        job = jconv.db2job(jdb)
            #    except multiprocessing.TimeoutError:
            #        self.log.error("%s: submission timeout: exit and try again" % task[1])
            #        # abort submission if Submit process is stuck
            #        #pool.terminate()
            #        KillPool(pool)
            #        pool.join()
            #        stopflag = True
            #        # reduce timeout to finish quickly
            #        timeout = 0.1
            #        continue
            #    if job is None:
            #        self.log.error("%s: no job defined for %d" % (task[1], task[0]))
            #        continue
            #    jd={}
            #    jd['arcstate']='submitted'
            #    # initial offset to 1 minute to force first status check
            #    jd['tarcstate']=self.db.getTimeStamp(time.time()-int(self.conf.get(['jobs','checkinterval']))+120)
            #    jd['tstate']=self.db.getTimeStamp()
            #    # extract hostname of cluster (depends on JobID being a URL)
            #    self.log.info("%s: job id %s" % (task[1], job.JobID))
            #    jd['cluster']=self.cluster
            #    self.db.updateArcJobLazy(task[0],jd,job)
            #    nsubmitted += 1
            #if not stopflag:
            #    pool.terminate()
            #    pool.join()
            #else:
            #    # stop submitting, gsiftp connection problem likely
            #    raise ExceptInterrupt(15)

            #self.log.info("threads finished")
            ## commit transaction to release row locks
            #self.db.Commit()

            ## still proxy bug - exit if there are multiple proxies
            ##if len(self.db.getProxiesInfo('TRUE', ['id'])) > 1:
            ##    raise ExceptInterrupt(15)

            ##################################################################
            #
            # New REST submission code
            #
            ##################################################################

            # get proxy path
            proxypath = os.path.join(self.db.proxydir, f"proxiesid{proxyid}")
            if not os.path.isfile(proxypath):
                self.log.error(f"Proxy path {proxypath} is not a file")
                self.setJobsArcstate(jobs, "tosubmit", commit=True)
                continue

            # read job descriptions from DB
            for job in jobs:
                job["descstr"] = str(self.db.getArcJobDescription(str(job["jobdesc"])))

            # parse cluster URL
            url = urlparse(self.cluster)
            queue = url.path.split("/")[-1]

            try:
                # create proxy authenticated connection
                context = ssl.SSLContext(ssl.PROTOCOL_TLS)
                context.load_cert_chain(proxypath, keyfile=proxypath)
                conn = http.client.HTTPSConnection(url.netloc, context=context)

                # submit jobs to ARC
                jobs = submitJobs(conn, queue, proxypath, jobs, logger=self.log)

            except ssl.SSLError as e:
                self.log.error(f"Could not create SSL context for proxy {proxypath}: {e}")
                self.setJobsArcstate(jobs, "tosubmit", commit=True)
                continue
            except (http.client.HTTPException, ConnectionError) as e:
                self.log.error(f"Could not connect to cluster {url.netloc}: {e}")
                self.setJobsArcstate(jobs, "tosubmit", commit=True)
                continue
            except SubmitError as e:
                self.log.error(str(e))
                self.setJobsArcstate(jobs, e.arcstate, commit=True)
                continue

            # log submission results and set job state
            for job in jobs:
                if "msg" in job:
                    job["arcstate"] = "tocancel"
                    self.log.debug(f"Submission failed for appjobid({job['appjobid']}), id({job['id']}): {job['msg']}")
                else:
                    job["arcstate"] = "submitted"
                    self.log.debug(f"Submission successfull for appjobid({job['appjobid']}), id({job['id']}) with ARC ID {job['arcid']}")

            # update job records in DB
            for job in jobs:
                jobdict = {}
                jobdict["arcstate"] = job["arcstate"]
                # initial offset to 1 minute to force first status check
                jobdict["tarcstate"] = self.db.getTimeStamp(time.time() - int(self.conf.get(['jobs', 'checkinterval'])) + 120)
                jobdict["tstate"] = self.db.getTimeStamp()
                jobdict["cluster"] = self.cluster

                if url.port is None:
                    port = 443
                else:
                    port = url.port
                interface = f"https://{url.hostname}:{port}/arex"

                if "delegation" in job:
                    jobdict["DelegationID"] = job["delegation"]
                if "arcid" in job:
                    jobdict["IDFromEndpoint"] = job["arcid"]
                    jobdict["JobID"] = f"{interface}/rest/1.0/jobs/{job['arcid']}"
                if "state" in job:
                    jobdict["State"] = job["state"]

                jobdict["JobManagementInterfaceName"] = "org.nordugrid.arcrest"
                jobdict["JobManagementURL"] = interface
                jobdict["JobStatusInterfaceName"] = "org.nordugrid.arcrest"
                jobdict["JobStatusURL"] = interface
                jobdict["ServiceInformationInterfaceName"] = "org.nordugrid.arcrest"
                jobdict["ServiceInformationURL"] = interface

                self.db.updateArcJobLazy(job["id"], jobdict)

            self.db.Commit()

        self.log.info("end submitting")

    def setJobsArcstate(self, jobs, arcstate, commit=False):
        for job in jobs:
            updateDict = {"arcstate": arcstate, "tarcstate": self.db.getTimeStamp()}
            self.db.updateArcJobLazy(job["id"], updateDict)
        if commit:
            self.db.Commit()

    # jobs that have been in submitting state for more than an hour
    # should be canceled
    def checkFailedSubmissions(self):

        jobs = self.db.getArcJobsInfo(f"arcstate='tosubmit' and cluster='{self.cluster}'", ["id", "appjobid", "jobdesc", "created"])

        for job in jobs:
            if job["created"] + datetime.timedelta(hours=1) < datetime.datetime.now(): # TODO: hardcoded
                self.db.updateArcJobLazy(job["id"], {"arcstate": "tocancel", "tarcstate": self.db.getTimeStamp()})

        self.db.Commit()

    def processToCancel(self):
        COLUMNS = ["id", "appjobid", "proxyid", "IDFromEndpoint"]

        # fetch jobs from DB for this cluster and also jobs that
        # don't have cluster assigned
        jobstocancel = self.db.getArcJobsInfo(
            "arcstate='tocancel' and (cluster='{0}' or clusterlist like '%{0}' or clusterlist like '%{0},%')".format(self.cluster),
            COLUMNS
        )
        dbtocancel = self.db.getArcJobsInfo(
            "arcstate='tocancel' and cluster=''",
            COLUMNS
        )
        if dbtocancel:
            jobstocancel.extend(dbtocancel)
        if not jobstocancel:
            return

        self.log.info(f"Cancelling {len(jobstocancel)} jobs")

        # aggregate jobs by proxyid
        jobsdict = {}
        for row in jobstocancel:
            if not row["proxyid"] in jobsdict:
                jobsdict[row["proxyid"]] = []
            jobsdict[row["proxyid"]].append(row)

        for proxyid, jobs in jobsdict.items():

            # create jobdicts for kill operation
            tokill = []  # all jobs to be killed
            toARCKill = []  # jobs to be killed in ARC
            for job in jobs:
                jobdict = {
                    "arcid": job["IDFromEndpoint"],
                    "id": job["id"],
                    "appjobid": job["appjobid"],
                    "arcstate": "cancelled"
                }
                tokill.append(jobdict)
                if job["IDFromEndpoint"]:
                    toARCKill.append(jobdict)

            proxypath = os.path.join(self.db.proxydir, f"proxiesid{proxyid}")
            conn = None
            try:
                # create proxy authenticated connection
                context = ssl.SSLContext(ssl.PROTOCOL_TLS)
                context.load_cert_chain(proxypath, keyfile=proxypath)
                url = urlparse(self.cluster)
                conn = http.client.HTTPSConnection(url.netloc, context=context)
                self.log.debug(f"Connected to cluster {url.netloc}")

                # clean jobs and log results
                toARCKill = killJobs(conn, toARCKill)
                for job in toARCKill:
                    if "msg" in job:
                        self.log.error(f"Error canceling appjobid({job['appjobid']}), id({job['id']}): {job['msg']}")
                    else:
                        job["arcstate"] = "cancelling"
                        self.log.debug(f"ARC will cancel appjobid({job['appjobid']}), id({job['id']})")

            except ssl.SSLError as e:
                self.log.error(f"Could not create SSL context for proxy {proxypath}: {e}")
            except (http.client.HTTPException, ConnectionError) as e:
                self.log.error(f"Could not connect to cluster {url.netloc}: {e}")
            except ACTError as e:
                self.log.error(f"Error canceling ARC jobs: {e}")
            finally:
                if conn:
                    conn.close()

            # update DB
            for job in tokill:
                jobdict = {"arcstate": job["arcstate"], "tarcstate": self.db.getTimeStamp()}
                if job["arcstate"] == "cancelling":
                    jobdict["tstate"] = self.db.getTimeStamp()
                self.db.updateArcJobLazy(job["id"], jobdict)
            self.db.Commit()

    # This does not handle jobs with empty clusterlist. What about that?
    #
    # This does not kill jobs!!!
    def processToResubmit(self):
        COLUMNS = ["id", "appjobid", "proxyid", "IDFromEndpoint", "cluster"]

        # fetch jobs from DB
        jobstoresubmit = self.db.getArcJobsInfo(
            f"arcstate='toresubmit' and cluster='{self.cluster}'",
            COLUMNS
        )
        if not jobstoresubmit:
            return

        #jobstoresubmit = self.db.getArcJobs("arcstate='toresubmit' and clusterlist=''")

        self.log.info(f"Resubmitting {len(jobstoresubmit)} jobs")

        # aggregate jobs by proxyid
        jobsdict = {}
        for row in jobstoresubmit:
            if not row["proxyid"] in jobsdict:
                jobsdict[row["proxyid"]] = []
            jobsdict[row["proxyid"]].append(row)

        for proxyid, jobs in jobsdict.items():

            # create job dicts for clean operation
            toclean = []
            toARCClean = []
            for job in jobs:
                jobdict = {
                    "arcid": job["IDFromEndpoint"],
                    "id": job["id"],
                    "appjobid": job["appjobid"]
                }
                toclean.append(jobdict)
                if "cluster" in job:
                    toARCClean.append(jobdict)

            proxypath = os.path.join(self.db.proxydir, f"proxiesid{proxyid}")
            conn = None
            try:
                # create proxy authenticated connection
                context = ssl.SSLContext(ssl.PROTOCOL_TLS)
                context.load_cert_chain(proxypath, keyfile=proxypath)
                url = urlparse(self.cluster)
                conn = http.client.HTTPSConnection(url.netloc, context=context)
                self.log.debug(f"Connected to cluster {url.netloc}")

                # clean jobs and log results
                toARCClean = cleanJobs(conn, toARCClean)
                for job in toARCClean:
                    if "msg" in job:
                        self.log.error(f"Error cleaning appjobid({job['appjobid']}), id({job['id']}): {job['msg']}")
                    else:
                        self.log.debug(f"Successfully cleaned appjobid({job['appjobid']}), id({job['id']})")

            except ssl.SSLError as e:
                self.log.error(f"Could not create SSL context for proxy {proxypath}: {e}")
            except http.client.HTTPException as e:
                self.log.error(f"Could not connect to cluster {url.netloc}: {e}")
            except ACTError as e:
                self.log.error(f"Error cleaning ARC jobs: {e}")
            finally:
                if conn:
                    conn.close()

            # update DB
            for job in toclean:
                tstamp = self.db.getTimeStamp()
                # "created" needs to be reset so that it doesn't get understood
                # as failing to submit since first insertion.
                jobdict = {"arcstate": "tosubmit", "tarcstate": tstamp, "created": tstamp}
                self.db.updateArcJobLazy(job["id"], jobdict)
            self.db.Commit()

    def processToRerun(self):
        COLUMNS = ["id", "appjobid", "proxyid", "IDFromEndpoint"]

        # fetch jobs from DB
        jobstorerun = self.db.getArcJobsInfo(
            f"arcstate='torerun' and cluster='{self.cluster}'",
            COLUMNS
        )
        if not jobstorerun:
            return

        self.log.info("Resuming {len(jobstorerun)} jobs")

        # aggregate jobs by proxyid
        jobsdict = {}
        for row in jobstorerun:
            if not row["proxyid"] in jobsdict:
                jobsdict[row["proxyid"]] = []
            jobsdict[row["proxyid"]].append(row)

        for proxyid, jobs in jobsdict.items():

            # create a list of jobdicts to be rerun
            torerun = []
            for job in jobs:
                jobdict = {
                    "arcid": job["IDFromEndpoint"],
                    "id": job["id"],
                    "appjobid": job["appjobid"]
                }
                torerun.append(jobdict)

            proxypath = os.path.join(self.db.proxydir, f"proxiesid{proxyid}")
            conn = None
            try:
                # create proxy authenticated connection
                context = ssl.SSLContext(ssl.PROTOCOL_TLS)
                context.load_cert_chain(proxypath, keyfile=proxypath)
                url = urlparse(self.cluster)
                conn = http.client.HTTPSConnection(url.netloc, context=context)
                self.log.debug(f"Connected to cluster {url.netloc}")

                # get delegations for jobs
                # TODO: one delegation for each job is assumed
                try:
                    delegations = getJobsDelegations(conn, "/arex/rest/1.0", torerun)
                except ACTError as e:  # TODO: what to do with such jobs?
                    self.log.error(f"Unable to retreive delegations: {e}")
                    continue

                # renew delegations
                torestart = []
                for delegation, job in zip(delegations, torerun):
                    if int(delegation["status-code"]) != 200:
                        job["msg"] = f"{delegation['status-code']} {delegation['reason']}"
                        continue
                    try:
                        renewDelegation(conn, "/arex/rest/1.0", delegation["delegation_id"], proxypath)
                    except ACTError as e:
                        job["msg"] = str(e)
                    else:
                        torestart.append(job)

                # restart in ARC
                try:
                    restartJobs(conn, torestart)
                except ACTError as e:
                    self.log.error(f"Unable to restart jobs: {e}")
                    continue

                # log results and update DB
                for job in torerun:
                    tstamp = self.db.getTimeStamp()
                    if "msg" in job:
                        self.log.error(f"Error rerunning appjobid({job['appjobid']}), id({job['id']}): {job['msg']}")
                        self.db.updateArcJobLazy(job["id"], {"arcstate": "failed", "tarcstate": tstamp})
                    else:
                        self.log.info("Successfully rerun appjobid(job['appjobid']), id(job['id']")
                        self.db.updateArcJobLazy(job["id"], {"arcstate": "submitted", "tarcstate": tstamp})
                self.db.Commit()

            except ssl.SSLError as e:
                self.log.error(f"Could not create SSL context for proxy {proxypath}: {e}")
            except http.client.HTTPException as e:
                self.log.error(f"Could not connect to cluster {url.netloc}: {e}")
            except ACTError as e:
                self.log.error(f"Error cleaning ARC jobs: {e}")
            finally:
                if conn:
                    conn.close()

    def process(self):

        # process jobs which have to be cancelled
        self.processToCancel()
        # process jobs which have to be resubmitted
        self.processToResubmit()
        # process jobs which have to be rerun
        self.processToRerun()
        # submit new jobs
        self.submit()
        # check jobs which failed to submit
        self.checkFailedSubmissions()


# Main
if __name__ == '__main__':
    asb=aCTSubmitter()
    asb.run()
    asb.finish()

