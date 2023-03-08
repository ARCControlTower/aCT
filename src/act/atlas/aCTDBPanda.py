from act.db.aCTDB import aCTDB

class aCTDBPanda(aCTDB):

    def __init__(self, log):
        aCTDB.__init__(self, log, 'pandajobs')

    def createTables(self):
        '''
         pandajobs:
           - id: Auto-incremented counter
           - modified: Auto-updated modification time
           - created: Creation time of panda job
           - pandaid: Panda job ID
           - pandajob: String of panda job description
           - siteName: Panda Resource
           - prodSourceLabel: Type of job (managed, test, etc)
           - arcjobid: Row ID of job in arcjobs table
           - condorjobid: Row ID of job in condorjobs table
           - pandastatus: Panda job status corresponding to state on the panda server
                 sent: job is retrieved from panda
                 starting: job is in aCT but not yet running
                 running: job is running on worker node
                 transferring: job is finished but uploading output files or in aCT post-processing
                 finished: job finished successfully
                 failed: job failed (code or grid failure)
           - actpandastatus: aCT internal state of panda jobs
                 In addition to above states:
                 tovalidate: job has finished or failed and output files should
                   be validated or cleaned
                 toresubmit: job will be resubmitted but first output files
                   should be cleaned
                 done: aCT is finished with this job, nothing more needs to be done
                 donefailed: aCT is finished, job failed
                 tobekilled: panda requests that the job is cancelled
                 cancelled: job was cancelled in ARC, still need to send final heartbeat
                 donecancelled: job was cancelled, nothing more needs to be done
           - theartbeat: Timestamp of last heartbeat (pstatus set)
           - priority: Job priority
           - node: Worker node on which the job is running
           - startTime: Job start time
           - endTime: Job end time
           - computingElement: CE where the job is running
           - proxyid: ID of proxy in proxies table to use for this job
           - sendhb: Flag to say whether or not to send heartbeat
           - corecount: Number of cores used by job
           - metadata: Generic json metadata sent by the client
           - error: Error string from a failed job

        pandaarchive:
          - Selected fields from above list:
            - pandaid, siteName, actpandastatus, startTime, endTime
        '''

        str="""
        create table pandajobs (
        id INTEGER PRIMARY KEY AUTO_INCREMENT,
        modified TIMESTAMP,
        created TIMESTAMP,
        pandajob mediumtext,
        pandaid bigint,
        siteName VARCHAR(255),
        prodSourceLabel VARCHAR(255),
        arcjobid integer,
        condorjobid integer,
        pandastatus VARCHAR(255),
        actpandastatus VARCHAR(255),
        theartbeat timestamp,
        priority integer,
        node VARCHAR(255),
        startTime TIMESTAMP,
        endTime TIMESTAMP,
        computingElement VARCHAR(255),
        proxyid integer,
        sendhb TINYINT(1) DEFAULT 1,
        corecount integer,
        metadata BLOB,
        error mediumtext,
        UNIQUE (pandaid)
    )
"""

        # First check if table already exists
        c = self.db.getCursor()
        c.execute("show tables like 'pandajobs'")
        row = c.fetchone()
        self.Commit()
        if row:
            answer = input("Table pandajobs already exists!\nAre you sure you want to recreate it? (y/n) ")
            if answer != 'y':
                return True
            c.execute("drop table pandajobs")

        try:
            c.execute(str)
            # add indexes
            c.execute("ALTER TABLE pandajobs ADD INDEX (arcjobid)")
            c.execute("ALTER TABLE pandajobs ADD INDEX (condorjobid)")
            c.execute("ALTER TABLE pandajobs ADD INDEX (pandaid)")
            c.execute("ALTER TABLE pandajobs ADD INDEX (pandastatus)")
            c.execute("ALTER TABLE pandajobs ADD INDEX (actpandastatus)")
            c.execute("ALTER TABLE pandajobs ADD INDEX (siteName)")
        except Exception as x:
            self.log.error("failed create table %s" %x)
            return False

        str="""
        create table pandaarchive (
        pandaid bigint,
        siteName VARCHAR(255),
        actpandastatus VARCHAR(255),
        startTime TIMESTAMP DEFAULT 0,
        endTime TIMESTAMP
    )
"""

        try:
            c.execute("drop table pandaarchive")
        except:
            self.log.warning("no pandaarchive table")
        try:
            c.execute(str)
        except Exception as x:
            self.log.error("failed create table %s" %x)
            return False

        self.Commit()
        return True


    def insertJob(self,pandaid,pandajob,desc={}):
        desc['created']=self.getTimeStamp()
        desc['pandaid']=pandaid
        desc['pandajob']=pandajob
        s="insert into pandajobs (" + ",".join([k for k in desc.keys()]) + ") values (" + ",".join(['%s' for k in desc.keys()]) + ")"
        c=self.db.getCursor()
        c.execute(s,list(desc.values()))
        c.execute("SELECT LAST_INSERT_ID()")
        row = c.fetchone()
        self.Commit()
        return row

    def insertJobArchiveLazy(self,desc={}):
        s="insert into pandaarchive (" + ",".join([k for k in desc.keys()]) + ") values (" + ",".join(['%s' for k in desc.keys()]) + ")"
        c=self.db.getCursor()
        c.execute(s,list(desc.values()))

    def deleteJob(self,pandaid):
        c=self.db.getCursor()
        c.execute("delete from pandajobs where pandaid="+str(pandaid))
        self.Commit()

    def updateJob(self,pandaid,desc):
        self.updateJobLazy(pandaid,desc)
        self.Commit()

    def updateJobLazy(self,pandaid,desc):
        desc['modified']=self.getTimeStamp()
        s="UPDATE pandajobs SET " + ",".join(['%s=%%s' % (k) for k in desc.keys()])
        s+=" WHERE pandaid="+str(pandaid)
        c=self.db.getCursor()
        c.execute(s,list(desc.values()))

    def updateJobs(self, select, desc):
        self.updateJobsLazy(select, desc)
        self.Commit()

    def updateJobsLazy(self, select, desc):
        desc['modified']=self.getTimeStamp()
        s="UPDATE pandajobs SET " + ",".join(['%s=%%s' % (k) for k in desc.keys()])
        s+=" WHERE "+select
        c=self.db.getCursor()
        c.execute(s,list(desc.values()))

    def getJob(self,pandaid,columns=[]):
        c=self.db.getCursor()
        c.execute("SELECT "+self._column_list2str(columns)+" FROM pandajobs WHERE pandaid="+str(pandaid))
        row=c.fetchone()
        return row

    def getJobs(self,select,columns=[]):
        c=self.db.getCursor()
        c.execute("SELECT "+self._column_list2str(columns)+" FROM pandajobs WHERE "+select)
        rows=c.fetchall()
        return rows

    def getNJobs(self,select):
        c=self.db.getCursor()
        c.execute("select count(*) from pandajobs where " + select)
        njobs=c.fetchone()['count(*)']
        return int(njobs)

    def getJobReport(self):
        c=self.db.getCursor()
        c.execute("select arcjobid,arcstatus from pandajobs")
        rows=c.fetchall()
        return rows

if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.DEBUG)
    adb = aCTDBPanda(logging.getLogger())
    adb.createTables()
