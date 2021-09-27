import os
import shutil
try:
    import selinux
except:
    selinux = None

from act.ldmx.aCTLDMXProcess import aCTLDMXProcess

class aCTLDMXStatus(aCTLDMXProcess):
    '''
    Check the status of submitted and running jobs, handle resubmission or
    cleanup of failed and cancelled jobs.
    '''

    def __init__(self):
        aCTLDMXProcess.__init__(self)


    def checkSubmittedJobs(self):
        '''
        Look for newly submitted, running or finishing jobs
        '''

        # Map of arc states to LDMX states
        statemap = {'submitted': 'queueing',
                    'running':   'running',
                    'finishing': 'finishing',
                    'finished':  'finishing',
                    'done':      'finishing'}

        select = "ldmxstatus='waiting' and arcstate in ('submitted', 'running', 'finishing', 'finished', 'done') and arcjobs.id=ldmxjobs.arcjobid"
        columns = ['arcstate', 'cluster', 'state', 'ldmxjobs.id']
        submittedjobs = self.dbarc.getArcJobsInfo(select, columns, tables='arcjobs,ldmxjobs')

        for job in submittedjobs:
            self.log.info(f"Job {job['id']}: waiting -> {statemap[job['arcstate']]} (ARC state {job['state']})")
            desc = {'ldmxstatus': statemap[job['arcstate']],
                    'computingelement': job['cluster'] or 'None',
                    'sitename': self.endpoints.get(job['cluster'], 'None')}
            self.dbldmx.updateJobLazy(job['id'], desc)

        select = "ldmxstatus='queueing' and arcstate in ('running', 'finishing', 'finished', 'done') and arcjobs.id=ldmxjobs.arcjobid"
        queueingjobs = self.dbarc.getArcJobsInfo(select, columns, tables='arcjobs,ldmxjobs')

        for job in queueingjobs:
            self.log.info(f"Job {job['id']}: queueing -> {statemap[job['arcstate']]} (ARC state {job['state']})")
            desc = {'ldmxstatus': statemap[job['arcstate']],
                    'computingelement': job['cluster'] or 'None',
                    'sitename': self.endpoints.get(job['cluster'], 'None')}
            self.dbldmx.updateJobLazy(job['id'], desc)

        # Get post-batch ARC statuses
        select = "ldmxstatus='running' and arcstate in ('finishing', 'finished', 'done') and arcjobs.id=ldmxjobs.arcjobid"
        finishingjobs = self.dbarc.getArcJobsInfo(select, columns, tables='arcjobs,ldmxjobs')

        for job in finishingjobs:
            self.log.info(f"Job {job['id']}: running -> finishing (ARC state {job['state']})")
            desc = {'ldmxstatus': 'finishing',
                    'computingelement': job['cluster'] or 'None',
                    'sitename': self.endpoints.get(job['cluster'], 'None')}
            self.dbldmx.updateJobLazy(job['id'], desc)

        if submittedjobs or queueingjobs:
            self.dbldmx.Commit()


    def checkToCancelJobs(self):
        '''
        Look for jobs marked to cancel and cancel the arc jobs
        '''
        select = "ldmxstatus='tocancel'"
        columns = ['id', 'arcjobid', 'description', 'template']
        cancelledjobs = self.dbldmx.getJobs(select, columns)

        if not cancelledjobs:
            return

        for job in cancelledjobs:
            self.log.info(f"Job {job['id']} requested to cancel, killing arc job")

            # Check if there is an arc job
            columns = ['id', 'JobID']
            arcjob = self.dbarc.getArcJobInfo(job['arcjobid'], columns) if job['arcjobid'] else None
            if arcjob:
                self.log.info(f"Cancelling arc job {arcjob['id']}")
                select = "id='{}'".format(job['arcjobid'])
                desc = {"arcstate": "tocancel", "tarcstate": self.dbarc.getTimeStamp()}
                self.dbarc.updateArcJobs(desc, select)
                if arcjob['JobID']:
                    # Job was submitted to wait for it to be cancelled
                    self.dbldmx.updateJobLazy(job['id'], {'ldmxstatus': 'cancelling'})
                else:
                    # No job submitted, mark cancelled
                    self.log.info(f"Job {job['id']} was not submitted to arc, marking cancelled")
                    self.dbldmx.updateJobLazy(job['id'], {'ldmxstatus': 'cancelled'})
            else:
                self.log.info(f"Job {job['id']} has no arc job, marking cancelled")
                self.dbldmx.updateJobLazy(job['id'], {'ldmxstatus': 'cancelled'})

            self.cleanInputFiles(job)

        self.dbldmx.Commit()


    def checkToResubmitJobs(self):
        '''
        Look for jobs marked to resubmit, cancel the arc jobs and set to waiting
        '''
        select = "ldmxstatus='toresubmit'"
        columns = ['id', 'arcjobid']
        toresubmitjobs = self.dbldmx.getJobs(select, columns)

        if not toresubmitjobs:
            return

        for job in toresubmitjobs:
            self.log.info(f"Job {job['id']} requested to resubmit, killing arc job")

            # Check if there is an arc job
            columns = ['id']
            arcjob = self.dbarc.getArcJobInfo(job['arcjobid'], columns) if job['arcjobid'] else None
            if arcjob:
                self.log.info(f"Cancelling arc job {arcjob['id']}")
                select = "id='{}'".format(job['arcjobid'])
                desc = {"arcstate": "tocancel", "tarcstate": self.dbarc.getTimeStamp()}
                self.dbarc.updateArcJobs(desc, select)
            else:
                self.log.info(f"Job {job['id']} has no arc job")

            ldmxdesc = {'ldmxstatus': 'new', 'arcjobid': None,
                        'sitename': None, 'computingelement': None}
            self.dbldmx.updateJobLazy(job['id'], ldmxdesc)

        self.dbldmx.Commit()


    def checkFailedJobs(self):
        '''
        Look for failed jobs and set them to clean
        TODO: whether to try and keep logs
        '''
        # Get outputs to download for failed jobs
        select = "arcstate='failed'"
        columns = ['id']
        arcjobs = self.dbarc.getArcJobsInfo(select, columns)
        if arcjobs:
            for aj in arcjobs:
                select = "id='{}'".format(str(aj["id"]))
                desc = {"arcstate": "tofetch", "tarcstate": self.dbarc.getTimeStamp()}
                self.dbarc.updateArcJobsLazy(desc, select)
            self.dbarc.Commit()

        # Look for failed final states in ARC which are still starting or running in LDMX
        select = "arcstate in ('donefailed', 'cancelled', 'lost') and arcjobs.id=ldmxjobs.arcjobid"
        columns = ['arcstate', 'arcjobs.id', 'cluster', 'JobID', 'ldmxjobs.created', 'stdout', 'ldmxstatus',
                   'description', 'template', 'sitename', 'ldmxjobs.proxyid', 'batchid', 'Error', 'appjobid']

        jobstoupdate = self.dbarc.getArcJobsInfo(select, columns=columns, tables='arcjobs,ldmxjobs')

        if not jobstoupdate:
            return

        failedjobs = [job for job in jobstoupdate if job['arcstate'] == 'donefailed']
        if failedjobs:
            self.log.debug(f"Found {len(failedjobs)} failed jobs ({','.join([str(j['appjobid']) for j in failedjobs])})")
        lostjobs = [job for job in jobstoupdate if job['arcstate'] == 'lost']
        if lostjobs:
            self.log.debug(f"Found {len(lostjobs)} lost jobs ({','.join([str(j['appjobid']) for j in lostjobs])})")
        cancelledjobs = [job for job in jobstoupdate if job['arcstate'] == 'cancelled']
        if cancelledjobs:
            self.log.debug(f"Found {len(cancelledjobs)} cancelled jobs ({','.join([str(j['appjobid']) for j in cancelledjobs])})")

        desc = {"arcstate": "toclean", "tarcstate": self.dbarc.getTimeStamp()}
        for aj in failedjobs:
            self.copyOutputFiles(aj)
            self.checkForResubmission(aj)
            select = f"id={aj['id']}"
            self.dbarc.updateArcJobsLazy(desc, select)
            self.dbldmx.updateJobsLazy(f"arcjobid={aj['id']}", {'ldmxstatus': 'failed',
                                                                'computingelement': aj.get('cluster'),
                                                                'sitename': self.endpoints.get(aj.get('cluster'))})

        for aj in lostjobs:
            select = f"id={aj['id']}"
            self.dbarc.updateArcJobsLazy(desc, select)
            self.dbldmx.updateJobsLazy(f"arcjobid={aj['id']}", {'ldmxstatus': 'failed',
                                                                'computingelement': aj.get('cluster'),
                                                                'sitename': self.endpoints.get(aj.get('cluster'))})
            self.cleanInputFiles(aj)

        for aj in cancelledjobs:
            select = f"id={aj['id']}"
            if aj['JobID']:
                self.dbarc.updateArcJobsLazy(desc, select)
            else: # job was not submitted so just delete
                self.dbarc.deleteArcJob(aj['id'])
            self.dbldmx.updateJobsLazy(f"arcjobid={aj['id']}", {'ldmxstatus': 'cancelled',
                                                                'computingelement': aj.get('cluster'),
                                                                'sitename': self.endpoints.get(aj.get('cluster'))})
            self.cleanInputFiles(aj)

        self.dbarc.Commit()
        self.dbldmx.Commit()

    def cleanupLeftovers(self):
        """
        Clean jobs left behind in arcjobs table:
         - arcstate=tocancel or cancelling when cluster is empty
         - arcstate=done or cancelled or lost or donefailed when id not in ldmxjobs
         - arcstate=done and ldmxstate=cancelling (job finished before it got cancel request)
         - ldmxstate=cancelling and no arc jobs (job was cancelled before being submitted)
        """
        select = "arcstate in ('tocancel', 'cancelling', 'toclean') and (cluster='' or cluster is NULL)"
        jobs = self.dbarc.getArcJobsInfo(select, ['id', 'appjobid'])
        for job in jobs:
            self.log.info(f"{job['appjobid']}: Deleting from arcjobs unsubmitted job {job['id']}")
            self.dbarc.deleteArcJob(job['id'])

        select = "(arcstate='done' or arcstate='lost' or arcstate='cancelled' or arcstate='donefailed') \
                  and arcjobs.id not in (select arcjobid from ldmxjobs where arcjobid is not NULL)"
        jobs = self.dbarc.getArcJobsInfo(select, ['id', 'appjobid', 'arcstate', 'JobID'])
        cleandesc = {"arcstate":"toclean", "tarcstate": self.dbarc.getTimeStamp()}
        for job in jobs:
            # done jobs should not be there, log a warning
            if job['arcstate'] == 'done':
                self.log.warning(f"{job['appjobid']}: Removing orphaned done job {job['id']}")
            else:
                self.log.info(f"{job['appjobid']}: Cleaning left behind {job['arcstate']} job {job['id']}")
            self.dbarc.updateArcJobLazy(job['id'], cleandesc)
            if job['JobID'] and job['JobID'].rfind('/') != -1:
                sessionid = job['JobID'][job['JobID'].rfind('/'):]
                localdir = self.tmpdir + sessionid
                shutil.rmtree(localdir, ignore_errors=True)
        if jobs:
            self.dbarc.Commit()

        select = "arcstate='done' and ldmxstatus='cancelling' and arcjobs.id=ldmxjobs.arcjobid"
        jobs = self.dbarc.getArcJobsInfo(select, ['arcjobs.id', 'appjobid'], tables='arcjobs,ldmxjobs')
        for job in jobs:
            self.log.info(f"{job['appjobid']}: Cleaning finished job which was already cancelled")
            self.dbarc.updateArcJob(job['id'], {'arcstate': 'cancelled'})

        select = "ldmxstatus='cancelling' and arcjobid not in (select id from arcjobs)"
        jobs = self.dbldmx.getJobs(select, ['id'])
        for job in jobs:
            self.log.info(f"{job['id']}: Cleaning cancelling job which was never submitted")
            self.dbldmx.updateJobs(f"id={job['id']}", {'ldmxstatus': 'cancelled'})

    def checkForResubmission(self, arcjob):
        '''
        Check error message against retryable errors and submit a new job
        '''

        if arcjob['ldmxstatus'] in ['tocancel', 'cancelling']:
            self.log.info(f"{arcjob['appjobid']}: was already cancelled")
            self.cleanInputFiles(arcjob)
            return

        self.log.info(f"{arcjob['appjobid']}: error: {arcjob['Error']}")
        resub = [err for err in self.arcconf.getList(['errors','toresubmit','arcerrors','item']) if err in arcjob['Error']]
        if not resub:
            self.log.info(f"{arcjob['appjobid']} failed with permanent error")
            self.cleanInputFiles(arcjob)
            return

        self.log.info(f"{arcjob['appjobid']} will be resubmitted")
        self.dbldmx.insertJob(arcjob['description'], arcjob['template'],
                              arcjob['proxyid'], batchid=arcjob['batchid'])


    def copyOutputFiles(self, arcjob):
        '''
        Copy job stdout and errors log to final location and make a copy
        in the failed folder
        '''

        if not arcjob.get('JobID'):
            self.log.info('Job did not run, no output to copy')
            return

        sessionid = arcjob['JobID'][arcjob['JobID'].rfind('/')+1:]
        date = arcjob['created'].strftime('%Y-%m-%d')
        localdir = os.path.join(self.tmpdir, sessionid)
        gmlogerrors = os.path.join(localdir, "gmlog", "errors")
        jobstdout = arcjob['stdout']

        outdir = os.path.join(self.conf.get(['joblog','dir']), date)
        outdfailed = os.path.join(outdir, 'failed', arcjob['sitename'] or 'None')
        os.makedirs(outdir, 0o755, exist_ok=True)
        os.makedirs(outdfailed, 0o755, exist_ok=True)

        try:
            arcjoblog = os.path.join(outdir, "%s.log" % arcjob['appjobid'])
            shutil.copy(gmlogerrors, arcjoblog)
            os.chmod(arcjoblog, 0o644)
            if selinux:
                selinux.restorecon(arcjoblog) #pylint: disable=E1101
            arcjoblog = os.path.join(outdfailed, "%s.log" % arcjob['appjobid'])
            shutil.move(gmlogerrors, arcjoblog)
            os.chmod(arcjoblog, 0o644)
            if selinux:
                selinux.restorecon(arcjoblog) #pylint: disable=E1101
        except Exception as e:
            self.log.error(f'Failed to copy {gmlogerrors}: {e}')

        if jobstdout:
            try:
                arcjobout = os.path.join(outdir, '%s.out' % arcjob['appjobid'])
                shutil.copy(os.path.join(localdir, jobstdout), arcjobout)
                os.chmod(arcjobout, 0o644)
                if selinux:
                    selinux.restorecon(arcjobout) #pylint: disable=E1101
                arcjobout = os.path.join(outdfailed, '%s.out' % arcjob['appjobid'])
                shutil.move(os.path.join(localdir, jobstdout), arcjobout)
                os.chmod(arcjobout, 0o644)
                if selinux:
                    selinux.restorecon(arcjobout) #pylint: disable=E1101
            except Exception as e:
                self.log.error(f'Failed to copy file {os.path.join(localdir, jobstdout)}, {str(e)}')


    def cleanInputFiles(self, job):
        '''
        Clean job input files in tmp dir
        '''
        try:
            os.remove(job['description'])
            os.remove(job['template'])
            self.log.debug(f'Removed {job["description"]} and {job["template"]}')
        except:
            pass


    def process(self):

        self.checkToCancelJobs()
        self.checkToResubmitJobs()
        self.checkSubmittedJobs()
        self.checkFailedJobs()
        self.cleanupLeftovers()


if __name__ == '__main__':

    ar = aCTLDMXStatus()
    ar.run()
    ar.finish()
