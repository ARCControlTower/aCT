from act.db.aCTDBNEW import aCTDB
from sqlalchemy import select, update, insert, delete
from sqlalchemy.sql import func
import datetime
import re
import os
import arc
from act.arc.dbModels import JobDescription, ArcJob, Proxy

class aCTDBArc(aCTDB):

    def __init__(self, log, db=None):
        aCTDB.__init__(self, log, 'arcjobs', db=db)

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

    def insertArcJob(self, job):
        '''
        Add new arc Job object. Only used for testing and recreating db.
        '''
        c=self.db.getCursor()
        jobdesc = str(job.JobDescriptionDocument)
        s = "insert into jobdescriptions (jobdescription) values (%s)"
        c.execute(s, [jobdesc])
        c.execute("SELECT LAST_INSERT_ID()")
        jobdescid = c.fetchone()['LAST_INSERT_ID()']

        j = self._job2db(job)
        tstamp = self.getTimeStamp()
        c.execute("insert into arcjobs (created,tstate,jobdesc"+",".join(j.keys())+") values ('"+str(tstamp)+"','"+str(tstamp)+"','"+str(jobdescid)+"','"+"','".join(j.values())+"')")
        c.execute("SELECT LAST_INSERT_ID()")
        row = c.fetchone()
        self.Commit()
        return row


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
        jobdescid = session.execute(insert(JobDescription).values(jobdescription=jobdesc).returning(JobDescription.id)).scalar_one()
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
        ).returning(ArcJob.id)).scalar_one()
        return arcjobid

    def deleteArcJob(self, id):
        '''
        Delete job from ARC table.
        '''
        c=self.db.getCursor()
        c.execute("select jobdesc from arcjobs where id = %s", (id,))
        row = c.fetchone()
        if row:
            c.execute("delete from jobdescriptions where id = %s", (row['jobdesc'],))
        c.execute("delete from arcjobs where id = %s", (id,))
        self.Commit()

    def updateArcJob(self, id, desc, job=None):
        '''
        Update arc job fields specified in desc and fields represented by arc
        Job if job is specified.
        '''
        self.updateArcJobLazy(id, desc, job)
        self.Commit()

    def updateArcJobLazy(self, id, desc, job=None):
        '''
        Update arc job fields specified in desc and fields represented by arc
        Job if job is specified. Does not commit after executing update.
        '''
        c = self.db.getCursor()
        c.execute("select id from arcjobs where id=%d limit 1" % id)
        row = c.fetchone()
        if row is None:
            self.log.warning("Arc job id %d no longer exists" % id)
            return

        desc['modified']=self.getTimeStamp()
        s = "update arcjobs set " + ",".join(['%s=%%s' % (k) for k in desc.keys()])
        if job:
            s += "," + ",".join(['%s=%%s' % (k) for k in self._job2db(job).keys()])
        s+=" where id="+str(id)
        if job:
            c.execute(s, list(desc.values()) + list(self._job2db(job).values()))
        else:
            c.execute(s, list(desc.values()))

    def updateArcJobs(self, desc, select):
        '''
        Update arc job fields specified in desc and matching the select statement.
        '''
        self.updateArcJobsLazy(desc, select)
        self.Commit()

    def updateArcJobsLazy(self, desc, select):
        '''
        Update arc job fields specified in desc and matching the select statement.
        Does not commit after executing update.
        '''
        desc['modified']=self.getTimeStamp()
        s = "update arcjobs set " + ",".join(['%s=%%s' % (k) for k in desc.keys()])
        s+=" where "+select
        c=self.db.getCursor()
        c.execute(s, list(desc.values()))

    def getArcJobInfo(self,id,columns=[]):
        '''
        Return a dictionary of column name: value for the given id and columns
        '''
        c=self.db.getCursor()
        c.execute("SELECT "+self._column_list2str(columns)+" FROM arcjobs WHERE id="+str(id))
        row=c.fetchone()
        if not row:
            return {}
        # mysql SELECT returns list, we want dict
        if not isinstance(row,dict):
            row = dict(zip([col[0] for col in c.description], row))
        return row

    def getArcJobsInfo(self, select, columns=[], tables="arcjobs", lock=False):
        '''
        Return a list of column: value dictionaries for jobs matching select.
        If lock is True the row will be locked if possible.
        '''
        c=self.db.getCursor()
        if lock:
            res = self.db.getMutexLock('arcjobs', timeout=20)
            if not res:
                self.log.debug("Could not get lock: %s"%str(res))
                return []
            if str(res) == "0":
                self.log.debug("Could not get lock: %s"%str(res))
                return []
            else:
                self.log.debug("Got lock: %s"%str(res))
        c.execute("SELECT "+self._column_list2str(columns)+" FROM "+tables+" WHERE "+select)
        rows=c.fetchall()
        return rows

    def getArcJobs(self,select):
        '''
        Return a dictionary of {proxyid: [(id, appjobid, arc.Job, created), ...]} for jobs matching select
        '''
        c=self.db.getCursor()
        c.execute("SELECT id, proxyid, appjobid, created, "+",".join(self.jobattrs.keys())+" FROM arcjobs WHERE "+select)
        rows=c.fetchall()
        d = {}
        if isinstance(rows, tuple):
            rows = dict(zip([col[0] for col in c.description], zip(*[list(row) for row in rows])))
            for row in rows:
                d[row[0]] = self._db2job(dict(zip([col[0] for col in c.description], row[1:])))
        # mysql returns list of dictionaries
        if isinstance(rows, list):
            for row in rows:
                if not row['proxyid'] in d:
                    d[row['proxyid']] = []
                d[row['proxyid']].append((row['id'], row['appjobid'], self._db2job(row), row['created']))

        return d

    def getArcJobDescription(self, jobdescid):
        '''
        Return the job description for the given id in jobdescriptions
        '''
        c=self.db.getCursor()
        c.execute("SELECT jobdescription from jobdescriptions where id="+str(jobdescid))
        row = c.fetchone()
        if not row:
            return None
        return row['jobdescription']

    def getNArcJobs(self, select):
        '''
        Return the count of jobs in the table matching select
        '''
        c=self.db.getCursor()
        c.execute("SELECT COUNT(*) FROM arcjobs WHERE "+select)
        row = c.fetchone()
        return row['COUNT(*)']

    def getGroupedJobs(self, groupby):
        '''
        Return counts of jobs grouped by given column(s)
        '''
        c = self.db.getCursor()
        c.execute(f"SELECT count(*), {groupby} FROM arcjobs GROUP BY {groupby}")
        rows = c.fetchall()
        return rows

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

    def _db2job(self, dbinfo):
        '''
        Convert a dictionary of DB key value into arc Job object
        '''
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

    def _job2db(self, job):
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
            if attr in d:
                d[attr] = ''.join([i for i in d[attr] if ord(i) < 128])
        return d

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
        proxyid = session.execute(insert(Proxy).values(proxy=proxy, dn=dn, expirytime=expirytime, attribute=attribute, proxytype=proxytype, myproxyid=myproxyid).returning(Proxy.id)).scalar_one()
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

    def getProxyPath(self, id):
        '''
        Get the path to the proxy file of a proxy
        '''
        c=self.db.getCursor()
        c.execute("SELECT proxypath FROM proxies WHERE id="+str(id))
        row = c.fetchone()
        try:
            proxypath=row['proxypath']
            if not os.path.isfile(proxypath):
                self._writeProxyFile(proxypath, self.getProxy(id))
            return proxypath
        except Exception as x:
            self.log.error("Could not find proxyid in proxies table. %s", x)

    def getProxy(self, id):
        '''
        Get the string representation of a proxy
        '''
        c=self.db.getCursor()
        c.execute("SELECT proxy FROM proxies WHERE id="+str(id))
        row = c.fetchone()
        try:
            proxy = row['proxy']
            return str(proxy, encoding='utf-8') if type(proxy) == bytes else proxy
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

