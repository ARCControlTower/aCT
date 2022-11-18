from act.db.aCTDB import aCTDB

class aCTDBLDMX(aCTDB):

    def __init__(self, log):
        aCTDB.__init__(self, log, 'ldmxjobs')

    def createTables(self):
        '''
         ldmxjobs:
           - id: Auto-incremented counter
           - modified: Auto-updated modification time
           - created: Creation time of ldmx job
           - sitename: the site on which the job ran or is submitted to
           - arcjobid: Row ID of job in arcjobs table
           - description: LDMX job description (config file)
           - template: LDMX job template file
           - ldmxstatus: LDMX job status
                 new: job has been entered but not processed yet
                 waiting: job is waiting to be submitted
                 queueing: job is submitted to CE
                 running: job is running in the site batch system
                 finishing: batch job has finished but is in post-processing
                 registering: job output is being registered in Rucio
                 finished: job finished successfully
                 failed: job failed
                 toresubmit: job will be cancelled and resubmitted
                 tocancel: job will be cancelled
                 cancelling: job is being cancelled
                 cancelled: job was cancelled
           - priority: Job priority
           - starttime: Job start time
           - endtime: Job end time
           - computingelement: CE where the job is running
           - proxyid: ID of proxy in proxies table to use for this job
           - batchid: Batch ID of job
           - userid: UID of user owning the job

        ldmxarchive:
          - Selected fields from above list:
            - id, siteName, ldmxstatus, starttime, endtime

        ldmxusers:
          - id: uid of user
          - name: Real name
          - role: Role of user
          - login: Login on aCT machine
          - ruciouser: Rucio username
        '''

        table_create = """
        create table ldmxjobs (
        id INTEGER PRIMARY KEY AUTO_INCREMENT,
        modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        sitename VARCHAR(255),
        arcjobid integer,
        description VARCHAR(255),
        template VARCHAR(255),
        ldmxstatus VARCHAR(255),
        priority integer,
        starttime TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        endtime TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        computingElement VARCHAR(255),
        proxyid integer,
        batchid VARCHAR(255),
        userid INTEGER
        )
"""

        # First check if table already exists
        c = self.db.getCursor()
        c.execute("show tables like 'ldmxjobs'")
        row = c.fetchone()
        self.Commit()
        if row:
            answer = input("Table ldmxjobs already exists!\nAre you sure you want to recreate it? (y/n) ")
            if answer != 'y':
                return True
            c.execute("drop table ldmxjobs")

        try:
            c.execute(table_create)
            # add indexes
            c.execute("ALTER TABLE ldmxjobs ADD INDEX (arcjobid)")
            c.execute("ALTER TABLE ldmxjobs ADD INDEX (ldmxstatus)")
            c.execute("ALTER TABLE ldmxjobs ADD INDEX (sitename)")
            c.execute("ALTER TABLE ldmxjobs ADD INDEX (userid)")
        except Exception as x:
            self.log.error(f"Failed to create table ldmxjobs: {x}")
            return False

        archive_table_create = """
        create table ldmxarchive (
        id bigint,
        sitename VARCHAR(255),
        ldmxstatus VARCHAR(255),
        starttime TIMESTAMP NOT NULL,
        endtime TIMESTAMP NOT NULL,
        batchid VARCHAR(255)
        )
"""

        try:
            c.execute("drop table ldmxarchive")
        except:
            self.log.warning("no ldmxarchive table")
        try:
            c.execute(archive_table_create)
        except Exception as x:
            self.log.error(f"Failed to create table ldmxarchive: {x}")
            return False

        users_table_create = """
        create table ldmxusers (
            id INTEGER,
            name VARCHAR(255),
            role VARCHAR(255),
            login VARCHAR(255),
            ruciouser VARCHAR(255)
        )
"""
        try:
            c.execute("drop table ldmxusers")
        except:
            self.log.warning("no ldmxusers table")
        try:
            c.execute(users_table_create)
        except Exception as x:
            self.log.error(f"Failed to create table ldmxusers: {x}")
            return False

        self.Commit()
        return True


    def insertJob(self, description, template, proxyid, user, batchid=None, priority=0):
        '''Insert new job description'''
        desc = {'description': description,
                'template': template,
                'proxyid': proxyid,
                'batchid': batchid,
                'priority': priority,
                'ldmxstatus': 'new'}
        s = f"insert into ldmxjobs ({','.join([k for k in desc.keys()])}) values ({','.join(['%s' for k in desc.keys()])})"
        c = self.db.getCursor()
        c.execute(s, list(desc.values()))
        c.execute("SELECT LAST_INSERT_ID()")
        row = c.fetchone()
        self.Commit()
        return row

    def insertJobArchiveLazy(self, desc={}):
        s = f"insert into ldmxarchive ({','.join([k for k in desc.keys()])}) values ({','.join(['%s' for k in desc.keys()])})"
        c = self.db.getCursor()
        c.execute(s, list(desc.values()))

    def deleteJob(self, id):
        c = self.db.getCursor()
        c.execute(f"delete from ldmxjobs where id={id}")
        self.Commit()

    def updateJob(self, id, desc):
        self.updateJobLazy(id, desc)
        self.Commit()

    def updateJobLazy(self, id, desc):
        s = f"UPDATE ldmxjobs SET {','.join(['%s=%%s' % (k) for k in desc.keys()])} WHERE id={id}"
        c = self.db.getCursor()
        c.execute(s, list(desc.values()))

    def updateJobs(self, select, desc):
        self.updateJobsLazy(select, desc)
        self.Commit()

    def updateJobsLazy(self, select, desc):
        s = f"UPDATE ldmxjobs SET {','.join(['%s=%%s' % (k) for k in desc.keys()])} WHERE {select}"
        c = self.db.getCursor()
        c.execute(s, list(desc.values()))

    def getJob(self, id, columns=[]):
        c = self.db.getCursor()
        c.execute(f"SELECT {self._column_list2str(columns)} FROM ldmxjobs WHERE id={id}")
        row = c.fetchone()
        return row

    def getJobs(self, select, columns=[]):
        c = self.db.getCursor()
        c.execute(f"SELECT {self._column_list2str(columns)} FROM ldmxjobs WHERE {select}")
        rows = c.fetchall()
        return rows

    def getNJobs(self, select):
        c = self.db.getCursor()
        c.execute(f"SELECT count(*) FROM ldmxjobs WHERE {select}")
        njobs = c.fetchone()['count(*)']
        return int(njobs)

    def getGroupedJobs(self, groupby):
        c = self.db.getCursor()
        c.execute(f"SELECT count(*), {groupby} FROM ldmxjobs GROUP BY {groupby}")
        rows = c.fetchall()
        return rows

    def getGroupedArchiveJobs(self, groupby, limit=None):
        c = self.db.getCursor()
        if limit:
            c.execute(f"SELECT count(*), {groupby} FROM ldmxarchive WHERE " \
                      f"{self.db.timeStampGreaterThan('endtime', limit)} " \
                      f"GROUP BY {groupby}")
        else:
            c.execute(f"SELECT count(*), {groupby} FROM ldmxarchive GROUP BY {groupby}")
        rows = c.fetchall()
        return rows

    def insertUser(self, uid, login, role, name='', ruciouser=''):
        desc = {'id': uid,
                'login': login,
                'role': role,
                'name': name or login,
                'ruciouser': ruciouser or login}
        s = f"insert into ldmxusers ({','.join([k for k in desc.keys()])}) values ({','.join(['%s' for k in desc.keys()])})"
        c = self.db.getCursor()
        c.execute(s, list(desc.values()))
        c.execute("SELECT LAST_INSERT_ID()")
        row = c.fetchone()
        self.Commit()
        return row

    def getUser(self, uid):
        c = self.db.getCursor()
        c.execute(f"SELECT * FROM ldmxusers WHERE id={uid}")
        row = c.fetchone()
        return row

    def getUsers(self):
        c = self.db.getCursor()
        c.execute("SELECT * FROM ldmxusers")
        rows = c.fetchall()
        return rows

    def deleteUser(self, uid):
        c = self.db.getCursor()
        c.execute(f"delete from ldmxusers where id={uid}")
        self.Commit()

if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.DEBUG)
    adb = aCTDBLDMX(logging.getLogger())
    adb.createTables()
