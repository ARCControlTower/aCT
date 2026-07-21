from act.db.aCTDB import aCTDB
from sqlalchemy import select, update, insert, delete
from sqlalchemy.sql import func
import datetime
import re
import os
import arc
from act.arc.dbModels import JobDescription, ArcJob, Proxy

class aCTDBArc(aCTDB):

    def __init__(self, log):
        aCTDB.__init__(self, log)

        self.proxydir = self.conf.voms.proxystoredir

        '''
        arcjobs: columns are attributes of arc.Job plus the following:
          - id:
          - created: timestamp of creation of the record
          - modified: timestamp of last record update
          - arcstate: tosubmit, submitting, submitted, running, finishing, tocancel,
                      cancelling, cancelled, finished, failed, tofetch, torerun,
                      toresubmit, done, donefailed, lost, toclean
            "to" states are set by application engine or ARC engine for retries
          - tarcstate: time stamp of last arcstate
          - tstate: time stamp of last arc Job state change
          - cluster: hostname of the cluster chosen for the job
          - clusterlist: comma separated list of clusters on which the job may
            run. Can be empty.
          - jobdesc: Row id in jobdescriptions table
          - attemptsleft: Number of attempts left to run the job
          - downloadfiles: Semicolon-separated list of specific files to download
            after job finished. If empty download all in job desc.
          - rerunnable:
          - proxyid: id of corresponding proxies entry of proxy to use for this job
          - appjobid: job identifier of application. Used in log messages to track
            a job through the system
          - priority: ARC job priority, extracted from the job description
          - fairshare: A string representing a share. Job submission for the same
            cluster will be spread evenly over shares.
        jobdescriptions: job description added by the application engine
          - id: primary key
          - jobdescription: job description text
        proxies: columns are the following:
          - id:
          - proxy:
          - proxypath: path to file containing the proxy
          - dn: dn of the proxy
          - attribute: attribute of the proxy
          - proxytype: type of proxy, e.g., 'local' or 'myproxy'
          - myproxyid: id from myproxy
          - expirytime: timestamp for when proxy is expiring
        '''

    def insertArcJobDescription(self, session, jobdesc, proxyid='', maxattempts=0, clusterlist='', appjobid='', downloadfiles='', fairshare=''):
        '''
        Add a new job description for the ARC engine to process. If specified
        the job will be sent to a cluster in the given list.
        '''
        # extract priority from job desc (also checks if desc is valid)
        jobdescs = arc.JobDescriptionList()
        if not arc.JobDescription.Parse(str(jobdesc), jobdescs):
            self.log.error("%s: Failed to prepare job description" % appjobid)
            return None
        priority = jobdescs[0].Application.Priority
        if priority == -1: # use nicer default priority
            priority = 50

        # todo: find some useful default for proxyid
        jobdescid = session.execute(insert(JobDescription).values(jobdescription=jobdesc)).inserted_primary_key[0]
        tstmp = self.getTimeStamp()
        arcjobid = session.execute(insert(ArcJob).values(
            created=tstmp,
            arcstate='tosubmit',
            tarcstate=tstmp,
            tstate=tstmp,
            cluster='',
            clusterlist=clusterlist,
            jobdesc=jobdescid,
            attemptsleft=maxattempts,
            proxyid=proxyid,
            appjobid=appjobid,
            downloadfiles=downloadfiles,
            priority=priority,
            fairshare=fairshare
        )).inserted_primary_key[0]
        return arcjobid

    def getActiveClusters(self):
        '''
        Return a list and count of clusters
        '''
        with self.Session() as session:
            rows = session.execute(select(ArcJob.cluster, func.count(ArcJob.id).label('counts')).where(ArcJob.cluster!='').group_by(ArcJob.cluster)).all()
        return rows

    def getClusterLists(self):
        '''
        Return a list and count of clusterlists for jobs to submit
        '''
        with self.Session() as session:
            # submitting state is included here so that a submitter process is not
            # killed while submitting jobs
            rows = session.execute(select(ArcJob.clusterlist, func.count(ArcJob.id).label('counts')) \
                                   .where(ArcJob.arcstate.in_(['tosubmit', 'submitting', 'torerun', 'toresubmit', 'tocancel', 'cancelling'])) \
                                    .group_by(ArcJob.clusterlist)).all()
        return rows

    def _writeProxyFile(self, proxypath, proxy):
        with open(proxypath, 'w') as f:
            f.write(proxy)
        # make sure permissions are correct
        os.chmod(proxypath, 0o600)

    def insertProxy(self, proxy, session, dn, expirytime, attribute='', proxytype='local', myproxyid=''):
        '''
        Add new proxy.
          - proxy: string representation of proxy file
          - dn: DN of proxy
          - expirytime: timestamp for end of life of proxy
          - attribute: attribute of proxy
          - proxytype: type of proxy, default 'local'
          - myproxyid: id from myproxy
        Returns id of db entrance
        '''
        proxyid = session.execute(insert(Proxy).values(proxy=proxy, dn=dn, expirytime=expirytime, attribute=attribute, proxytype=proxytype, myproxyid=myproxyid)).inserted_primary_key[0]
        proxypath = os.path.join(self.proxydir,"proxiesid"+str(proxyid))
        session.execute(update(Proxy).where(Proxy.id==proxyid).values(proxypath=proxypath))
        self._writeProxyFile(proxypath, proxy)
        return proxyid
    
    def updateProxy(self, id, session, desc):
        '''
        Update proxy fields specified in desc.
        '''
        session.execute(update(Proxy).where(Proxy.id==id).values(**desc))
        if 'proxy' in desc:
            proxy = self.getProxiesInfo(session, {'id':id}, ['proxypath', 'proxy'])
            self._writeProxyFile(proxy.proxypath, str(proxy.proxy, encoding='utf-8') if type(proxy.proxy) == bytes else proxy.proxy)

    def getProxyPath(self, session, id):
        '''
        Get the path to the proxy file of a proxy
        '''
        row = session.execute(select(Proxy.proxypath, Proxy.proxy).where(Proxy.id==id)).one_or_none()
        try:
            proxypath = row.proxypath
            if not os.path.isfile(proxypath) and row.proxy:
                self._writeProxyFile(proxypath, row.proxy)
            return proxypath
        except Exception as x:
            self.log.error("Could not find proxyid in proxies table. %s", x)

    def getProxiesInfo(self, session, filter, columns):
        '''
        Return a list of column: value row objects for proxies matching select.
        '''
        selected_columns = [getattr(Proxy, col) for col in columns]
        stmt = select(*selected_columns).where(*[getattr(Proxy, k) == v for k, v in filter.items()])
        result = session.execute(stmt).first()
        return result

    def deleteProxy(self, session, id):
        '''
        Delete proxy from proxies table.
        '''
        session.execute(delete(Proxy).where(Proxy.id==id))

    def setJobsArcstate(self, jobs, arcstate):
        stmt = update(ArcJob)
        tstamp = self.getTimeStamp()
        if isinstance(jobs, list):
            stmt = stmt.where(ArcJob.id.in_(jobs))
        else:
            stmt = stmt.where(ArcJob.id==jobs)
        return stmt.values(arcstate=arcstate, tarcstate=tstamp)

if __name__ == '__main__':
    import logging, sys
    log = logging.getLogger()
    out = logging.StreamHandler(sys.stdout)
    log.addHandler(out)

    adb = aCTDBArc(log)
    adb.createTables()

    usercfg = arc.UserConfig("", "")
    usercfg.Timeout(10)

    # Simple job description which outputs hostname to stdout
    jobdescstring = "&(executable=/bin/hostname)(stdout=stdout)"

    # Parse job description
    jobdescs = arc.JobDescriptionList()
    if not arc.JobDescription.Parse(jobdescstring, jobdescs):
        logging.error("Invalid job description")
        exit(1)

    # Use top-level NorduGrid information index to find resources
    index = arc.Endpoint("ldap://index1.nordugrid.org:2135/Mds-Vo-name=nordugrid,o=grid",
                         arc.Endpoint.REGISTRY,
                         "org.nordugrid.ldapegiis")
    services = arc.EndpointList(1, index)

    # Do the submission
    #jobs = arc.JobList()
    #submitter = arc.Submitter(usercfg)
    #if submitter.BrokeredSubmit(services, jobdescs, jobs) != arc.SubmissionStatus.NONE:
    #    logging.error("Failed to submit job")
    #    exit(1)

    #adb.insertArcJob(1, jobs[0])
    #dbjob = adb.getArcJob(1)
    #print dbjob[1].JobID, dbjob[1].State.GetGeneralState()

