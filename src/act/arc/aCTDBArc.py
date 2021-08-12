import re
import os
import arc
from act.db.aCTDB import aCTDB

class aCTDBArc(aCTDB):

    def __init__(self, log):
        aCTDB.__init__(self, log, 'arcjobs')

        self.proxydir = self.conf.get(["voms","proxystoredir"])

        # mapping from Job class attribute types to column types
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


    def createTables(self):
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

        # in MySQL the first timestamp specified gets automatically updated to
        # current time for each change.
        create="""CREATE TABLE arcjobs (
            id INTEGER PRIMARY KEY AUTO_INCREMENT,
            modified TIMESTAMP,
            created TIMESTAMP,
            arcstate VARCHAR(12),
            tarcstate TIMESTAMP,
            tstate TIMESTAMP,
            cluster VARCHAR(255),
            clusterlist VARCHAR(1024),
            jobdesc INT(11),
            attemptsleft INTEGER,
            downloadfiles VARCHAR(255),
            proxyid INTEGER,
            appjobid VARCHAR(16),
            priority SMALLINT,
            fairshare VARCHAR(50),
            """+",".join(['%s %s' % (k, self.jobattrmap[v]) for k, v in self.jobattrs.items()])+")"

        # First check if table already exists
        c = self.db.getCursor()
        c.execute("show tables like 'arcjobs'")
        row = c.fetchone()
        self.Commit()
        if row:
            answer = input("Table arcjobs already exists!\nAre you sure you want to recreate it? (y/n) ")
            if answer != 'y':
                return True
            c.execute("drop table arcjobs")

        # Create arcjobs
        self.log.info("creating arcjobs table")
        try:
            c.execute(create)
            self.Commit()
        except Exception as x:
            self.log.error("failed create table %s" %x)
            return False

        # Create job description table
        self.log.info("creating jobdescriptions table")
        create="""CREATE TABLE jobdescriptions (
            id INTEGER PRIMARY KEY AUTO_INCREMENT,
            jobdescription mediumtext)
            """
        try:
            c.execute("drop table jobdescriptions")
        except:
            pass
        try:
            c.execute(create)
            # add indexes
            c.execute("ALTER TABLE arcjobs ADD INDEX (arcstate)")
            self.Commit()
        except Exception as x:
            self.log.error("failed create table %s" %x)
            return False

        # Create proxies table (can be dropped without asking)
        self.log.info("creating proxies table")
        create="""CREATE TABLE proxies (
            id INTEGER PRIMARY KEY AUTO_INCREMENT,
            proxy BLOB,
            expirytime DATETIME,
            proxypath VARCHAR(255),
            dn VARCHAR(255),
            attribute VARCHAR(255),
            proxytype VARCHAR(255),
            myproxyid VARCHAR(255) )"""
        try:
            c.execute("drop table proxies")
        except:
            pass
        try:
            c.execute(create)
            self.Commit()
        except Exception as x:
            self.log.error("failed create table %s" %x)
            return False

        return True

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
        c.execute("insert into arcjobs (created,tstate,jobdesc"+",".join(j.keys())+") values ('"+str(self.getTimeStamp())+"','"+str(self.getTimeStamp())+"','"+str(jobdescid)+"','"+"','".join(j.values())+"')")
        c.execute("SELECT LAST_INSERT_ID()")
        row = c.fetchone()
        self.Commit()
        return row


    def insertArcJobDescription(self, jobdesc, proxyid='', maxattempts=0, clusterlist='', appjobid='', downloadfiles='', fairshare=''):
        '''
        Add a new job description for the ARC engine to process. If specified
        the job will be sent to a cluster in the given list.
        '''
        # extract priority from job desc (also checks if desc is valid)
        jobdescs = arc.JobDescriptionList()
        if not arc.JobDescription_Parse(str(jobdesc), jobdescs):
            self.log.error("%s: Failed to prepare job description" % appjobid)
            return None
        priority = jobdescs[0].Application.Priority
        if priority == -1: # use nicer default priority
            priority = 50

        # todo: find some useful default for proxyid
        c=self.db.getCursor()

        s = "insert into jobdescriptions (jobdescription) values (%s)"
        c.execute(s, [jobdesc])
        c.execute("SELECT LAST_INSERT_ID()")
        jobdescid = c.fetchone()['LAST_INSERT_ID()']

        desc = {}
        desc['created'] = self.getTimeStamp()
        desc['arcstate'] = "tosubmit"
        desc['tarcstate']  = desc['created']
        desc['tstate'] = desc['created']
        desc['cluster']  = ''
        desc['clusterlist'] = clusterlist
        desc['jobdesc'] = jobdescid
        desc['attemptsleft'] = maxattempts
        desc['proxyid'] = proxyid
        desc['appjobid'] = appjobid
        desc['downloadfiles'] = downloadfiles
        desc['priority'] = priority
        desc['fairshare'] = fairshare
        s="insert into arcjobs" + " ( " + ",".join(['%s' % (k) for k in desc.keys()]) + " ) " + " values " + \
            " ( " + ",".join(['%s' % (k) for k in ["%s"] * len(desc.keys()) ]) + " ) "
        c.execute(s, list(desc.values()))
        c.execute("SELECT LAST_INSERT_ID()")
        row = c.fetchone()
        self.Commit()
        return row

    def deleteArcJob(self, id):
        '''
        Delete job from ARC table.
        '''
        c=self.db.getCursor()
        c.execute("select jobdesc from arcjobs where id="+str(id))
        row = c.fetchone()
        if row:
            c.execute("delete from jobdescriptions where id="+str(row['jobdesc']))
        c.execute("delete from arcjobs where id="+str(id))
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
        c=self.db.getCursor()
        c.execute("SELECT cluster, COUNT(*) FROM arcjobs WHERE cluster!='' GROUP BY cluster")
        rows=c.fetchall()
        return rows

    def getClusterLists(self):
        '''
        Return a list and count of clusterlists for jobs to submit
        '''
        c=self.db.getCursor()
        # submitting state is included here so that a submitter process is not
        # killed while submitting jobs
        c.execute("SELECT clusterlist, COUNT(*) FROM arcjobs WHERE arcstate in ('tosubmit', 'submitting', 'torerun', 'toresubmit', 'tocancel', 'cancelling') GROUP BY clusterlist")
        rows=c.fetchall()
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

    def insertProxy(self, proxy, dn, expirytime, attribute='', proxytype='local', myproxyid=''):
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
        c=self.db.getCursor()
        s="INSERT INTO proxies (proxy, dn, attribute, proxytype, myproxyid, expirytime) VALUES ('"\
                  +proxy+"','"+dn+"','"+attribute+"','"+proxytype+"','"+myproxyid+"','"+expirytime+"')"
        c.execute(s)
        c.execute("SELECT LAST_INSERT_ID()")
        row = c.fetchone()
        id=row['LAST_INSERT_ID()']
        proxypath=os.path.join(self.proxydir,"proxiesid"+str(id))
        c.execute("UPDATE proxies SET proxypath='"+proxypath+"' WHERE id="+str(id))
        self.Commit()
        self._writeProxyFile(proxypath, proxy)
        return id

    def updateProxy(self, id, desc):
        '''
        Update proxy fields specified in desc.
        '''
        s="UPDATE proxies SET "+",".join(['%s=\'%s\'' % (k, v) for k, v in desc.items()])
        s+=" WHERE id="+str(id)
        c=self.db.getCursor()
        c.execute(s)
        self.Commit()
        # rewrite proxy file if proxy was updated
        if 'proxy' in desc:
            self._writeProxyFile(self.getProxyPath(id), self.getProxy(id))

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

    def getProxiesInfo(self, select, columns=[], lock=False, expect_one=False):
        '''
        Return a list of column: value dictionaries for proxies matching select.
        If lock is True the row will be locked if possible. If expect_one is true
        only one row will be returned.
        '''
        if lock:
            select += self.db.addLock()
        c=self.db.getCursor()
        c.execute("SELECT "+self._column_list2str(columns)+" FROM proxies WHERE "+select)
        if expect_one:
            return c.fetchone()
        else:
            return c.fetchall()

    def deleteProxy(self, id):
        '''
        Delete proxy from proxies table.
        '''
        # remove file first
        proxypath=self.getProxyPath(id)
        if os.path.isfile(proxypath):
            os.remove(proxypath)
        c=self.db.getCursor()
        c.execute("DELETE FROM proxies WHERE id="+str(id))
        self.Commit()

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
    if not arc.JobDescription_Parse(jobdescstring, jobdescs):
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
