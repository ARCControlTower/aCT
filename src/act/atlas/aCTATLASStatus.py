# Handler for filling pandajobs information from arcjobs information. Also
# deals with post-processing of jobs and error handling.

import datetime
import json
import os
import re
import shutil
from urllib.parse import urlparse

from act.atlas.aCTATLASProcess import aCTATLASProcess
from act.atlas.aCTPandaJob import aCTPandaJob


class aCTATLASStatus(aCTATLASProcess):

    def checkJobstoKill(self):
        """
        Get starting jobs for offline sites and kill them.
        Check for jobs with pandastatus tobekilled and cancel them in ARC:
        - pandastatus NULL: job was killed by panda so nothing to report
        - pandastatus something else: job was manually killed, so create pickle
          and report failed back to panda
        """

        offlinesites = [s for s,a in self.sites.items() if a['status'] == 'offline']

        if offlinesites:

            offlinesitesselect = "('%s')" % "','".join(offlinesites)
            jobs = self.dbpanda.getJobs("(actpandastatus='starting' or actpandastatus='sent') and sitename in %s" % offlinesitesselect,
                                        ['pandaid', 'arcjobid', 'siteName', 'id'])

            for job in jobs:
                self.log.info("Cancelling starting job for %d for offline site %s" % (job['pandaid'], job['siteName']))
                select = 'id=%s' % job['id']
                self.dbpanda.updateJobsLazy(select, {'actpandastatus': 'failed', 'pandastatus': 'failed',
                                                     'error': 'Starting job was killed because queue went offline'})
                if job['arcjobid']:
                    self.dbarc.updateArcJob(job['arcjobid'], {'arcstate': 'tocancel'})

            self.dbpanda.Commit()

        # Get jobs killed by panda
        jobs = self.dbpanda.getJobs("actpandastatus='tobekilled' and siteName in %s limit 100" % self.sitesselect,
                                    ['pandaid', 'arcjobid', 'pandastatus', 'id', 'siteName'])
        if not jobs:
            return

        for job in jobs:
            self.log.info("Cancelling arc job for %d", job['pandaid'])
            select = 'id=%s' % job['id']

            # Check if arcjobid is set before cancelling the job
            if not job['arcjobid']:
                self.dbpanda.updateJobsLazy(select, {'actpandastatus': 'cancelled'})
                continue

            # Put timings in the DB
            arcselect = "arcjobid='%s' and arcjobs.id=pandajobs.arcjobid and sitename in %s" % (job['arcjobid'], self.sitesselect)
            columns = ['arcjobs.EndTime', 'UsedTotalWallTime', 'stdout', 'JobID', 'appjobid', 'siteName', 'cluster', 'metadata',
                       'ExecutionNode', 'pandaid', 'UsedTotalCPUTime', 'ExitCode', 'arcjobs.Error', 'sendhb', 'pandajobs.created', 'corecount']

            arcjobs = self.dbarc.getArcJobsInfo(arcselect, columns=columns, tables='arcjobs,pandajobs')
            desc = {}
            if arcjobs:
                desc['endTime'] = datetime.datetime.utcnow()
                desc['startTime'] = datetime.datetime.utcnow()

            self.processFailed(arcjobs)
            # Check if job was manually killed
            if job['pandastatus'] is not None:
                self.log.info('%s: Manually killed, will report failure to panda' % job['pandaid'])
                # Skip validator since there is no metadata.xml
                desc['actpandastatus'] = 'failed'
                desc['pandastatus'] = 'failed'
                desc['error'] = 'Job was killed in aCT'
                if self.sites[job['siteName']]['truepilot']:
                    desc['sendhb'] = 0
            else:
                desc['actpandastatus'] = 'cancelled'
            self.dbpanda.updateJobsLazy(select, desc)

            # Finally cancel the arc job
            self.dbarc.updateArcJob(job['arcjobid'], {'arcstate': 'tocancel'})

        self.dbpanda.Commit()

    def getStartTime(self, endtime, walltime):
        """
        Get starttime from endtime-walltime where endtime is datetime.datetime and walltime is in seconds
        If endtime is none then use current time
        """
        if not endtime:
            return datetime.datetime.utcnow() - datetime.timedelta(0, walltime)
        return endtime-datetime.timedelta(0, walltime)


    def updateStartingJobs(self):
        """
        Check for sent jobs that have been submitted to ARC and update
        actpandastatus to starting, and also for jobs that were requeued
        from running.
        """
        select = "((arcjobs.arcstate in ('submitted', 'holding') and pandajobs.actpandastatus='sent') or"
        select += " (arcjobs.arcstate in ('tosubmit', 'submitting', 'submitted', 'holding') and pandajobs.actpandastatus='running'))"
        select += " and arcjobs.id=pandajobs.arcjobid and pandajobs.sitename in %s limit 100000" % self.sitesselect
        columns = ["arcjobs.id", "arcjobs.cluster", "arcjobs.appjobid"]
        jobstoupdate=self.dbarc.getArcJobsInfo(select, columns=columns, tables="arcjobs,pandajobs")

        if len(jobstoupdate) == 0:
            return
        else:
            self.log.debug("Found %d submitted jobs (%s)" % (len(jobstoupdate), ','.join([j['appjobid'] for j in jobstoupdate])))

        for aj in jobstoupdate:
            select = "arcjobid='"+str(aj["id"])+"'"
            desc = {}
            desc["pandastatus"] = "starting"
            desc["actpandastatus"] = "starting"
            if aj['cluster']:
                desc["computingElement"] = urlparse(aj['cluster']).hostname
            self.dbpanda.updateJobsLazy(select, desc)
        self.dbpanda.Commit()


    def updateRunningJobs(self,state):
        """
        Check for new running jobs and update pandajobs with
        - pandastatus
        - node
        - computingElement
        - startTime
        """

        # do an inner join to pick up all jobs that should be set to running
        # todo: pandajobs.starttime will not be updated if a job is resubmitted
        # internally by the ARC part.
        if state == "running":
            select = "arcjobs.id=pandajobs.arcjobid and arcjobs.arcstate in ('running') and pandajobs.actpandastatus in ('starting', 'sent')"
        if state == "finishing":
            select = "arcjobs.id=pandajobs.arcjobid and arcjobs.arcstate in ('finishing') and pandajobs.actpandastatus in ('starting', 'sent', 'running')"
        select += " and pandajobs.sitename in %s limit 100000" % self.sitesselect

        columns = ["arcjobs.id", "arcjobs.UsedTotalWalltime", "arcjobs.ExecutionNode",
                   "arcjobs.cluster", "arcjobs.RequestedSlots", "pandajobs.pandaid", "pandajobs.siteName", "arcjobs.appjobid", "arcjobs.tstate"]
        jobstoupdate=self.dbarc.getArcJobsInfo(select, columns=columns, tables="arcjobs,pandajobs")

        if len(jobstoupdate) == 0:
            return
        else:
            self.log.debug("Found %s: %d jobs (%s)" % (state, len(jobstoupdate), ','.join([j['appjobid'] for j in jobstoupdate])))

        for aj in jobstoupdate:
            select = "arcjobid='"+str(aj["id"])+"'"
            desc = {}
            desc["pandastatus"] = "running"
            desc["actpandastatus"] = "running"
            if state == "finishing" and datetime.datetime.utcnow() - aj["tstate"] > datetime.timedelta(minutes=10):
                desc["pandastatus"] = "transferring"
                desc["actpandastatus"] = "transferring"
            if len(aj["ExecutionNode"]) > 255:
                desc["node"] = aj["ExecutionNode"][:254]
                self.log.warning("%s: Truncating wn hostname from %s to %s" % (aj['pandaid'], aj['ExecutionNode'], desc['node']))
            else:
                desc["node"] = aj["ExecutionNode"]
            desc["computingElement"] = urlparse(aj['cluster']).hostname
            desc["startTime"] = self.getStartTime(datetime.datetime.utcnow(), aj['UsedTotalWalltime'])
            desc["corecount"] = aj['RequestedSlots']
            # When true pilot job has started running, turn of aCT heartbeats
            if self.sites[aj['siteName']]['truepilot']:
                self.log.info("%s: Job is running so stop sending heartbeats", aj['pandaid'])
                desc['sendhb'] = 0
            else:
                # Update APFmon (done by wrapper for truepilot)
                self.apfmon.updateJob(aj['pandaid'], 'running')

            try:
                self.dbpanda.updateJobsLazy(select, desc)
            except:
                desc['startTime'] = datetime.datetime.utcnow()
                self.dbpanda.updateJobsLazy(select, desc)

        self.dbpanda.Commit()


    def updateFinishedJobs(self):
        """
        Check for new finished jobs, update pandajobs with
        - pandastatus
        - startTime
        - endTime
        """

        # don't get jobs already having actpandastatus states treated by
        # validator to avoid race conditions
        select = "arcjobs.id=pandajobs.arcjobid and arcjobs.arcstate='done'"
        select += " and pandajobs.actpandastatus != 'tovalidate'"
        select += " and pandajobs.actpandastatus != 'toresubmit'"
        select += " and pandajobs.actpandastatus != 'toclean'"
        select += " and pandajobs.actpandastatus != 'finished'"
        select += " and pandajobs.sitename in %s limit 100000" % self.sitesselect
        columns = ["arcjobs.id", "arcjobs.UsedTotalWallTime", "arcjobs.EndTime", "arcjobs.appjobid", "pandajobs.sendhb", "pandajobs.siteName"]
        jobstoupdate=self.dbarc.getArcJobsInfo(select, tables="arcjobs,pandajobs", columns=columns)

        if len(jobstoupdate) == 0:
            return
        else:
            self.log.debug("Found %d finished jobs (%s)" % (len(jobstoupdate), ','.join([j['appjobid'] for j in jobstoupdate])))


        for aj in jobstoupdate:
            select = "arcjobid='"+str(aj["id"])+"'"
            desc = {}
            desc["pandastatus"] = "transferring"
            desc["actpandastatus"] = "tovalidate"
            desc["startTime"] = self.getStartTime(aj['EndTime'], aj['UsedTotalWallTime'])
            desc["endTime"] = aj["EndTime"]
            # True pilot job may have gone straight to finished, turn off aCT heartbeats if necessary
            if self.sites[aj['siteName']]['truepilot'] and aj["sendhb"] == 1:
                self.log.info("%s: Job finished so stop sending heartbeats", aj['appjobid'])
                desc['sendhb'] = 0
            if not self.sites[aj['siteName']]['truepilot']:
                # Update APFmon (done by wrapper for truepilot)
                self.apfmon.updateJob(aj['appjobid'], 'exiting', exitcode=0)
            try:
                self.dbpanda.updateJobsLazy(select, desc)
            except:
                desc['startTime'] = datetime.datetime.utcnow()
                desc['endTime'] = datetime.datetime.utcnow()
                self.dbpanda.updateJobsLazy(select, desc)
        self.dbpanda.Commit()


    def checkFailed(self, arcjobs):
        failedjobs = []
        resubmitting=False
        for aj in arcjobs:
            if self.sites[aj['siteName']]['truepilot']:
                self.log.info('%s: No resubmission for true pilot job', aj['appjobid'])
                failedjobs += [aj]
                continue
            resubmit=False
            # todo: errors part of aCTConfigARC should probably be moved to aCTConfigAPP.
            for error in self.arcconf.errors.toresubmit.arcerrors:
                if aj['Error'].find(error) != -1:
                    resubmit=True
            if resubmit:
                self.log.info("%s: Resubmitting %d %s %s" % (aj['appjobid'],aj['arcjobid'],aj['JobID'],aj['Error']))
                select = "arcjobid='"+str(aj["arcjobid"])+"'"
                jd={}
                # Validator processes this state before setting back to starting
                jd['pandastatus'] = 'starting'
                jd['actpandastatus'] = 'toresubmit'
                self.dbpanda.updateJobsLazy(select,jd)
                resubmitting=True
            else:
                failedjobs += [aj]
        if resubmitting:
            self.dbpanda.Commit()
            self.dbarc.Commit()
        return failedjobs

    def createPilotLog(self, outd, pandaid):
        '''
        Create the pilot log messages to appear on panda logger. Takes the gmlog
        'failed' file and errors from the pilot log if available. Creates a
        local copy under tmp/failedlogs.
        '''
        nlines=20
        log=""
        try:
            f=open(outd+"/gmlog/failed","r")
            log+="---------------------------------------------------------------\n"
            log+="GMLOG: failed\n"
            log+="---------------------------------------------------------------\n"
            log+=''.join(f.readlines())
            f.close()
        except:
            pass


        import glob
        lf=glob.glob(outd+"/log*")
        try:
            f=open(lf[0],"r")
            lines=f.readlines()
            log+="---------------------------------------------------------------\n"
            log+="LOGFILE: tail\n"
            log+="---------------------------------------------------------------\n"
            lns=[]
            for l in lines:
                if re.match('.*error',l,re.IGNORECASE):
                    lns.append(l)
                if re.match('.*warning',l,re.IGNORECASE):
                    lns.append(l)
                if re.match('.*failed',l,re.IGNORECASE):
                    lns.append(l)
            log+=''.join(lns[:nlines])
            # copy logfiles to failedlogs dir
            try:
                f=open(os.path.join(self.tmpdir, "failedlogs", str(pandaid)+".log"),"w")
                f.write(log)
                f.close()
            except:
                pass
        except:
            pass
        return log


    def processFailed(self, arcjobs):
        """
        process jobs for which pilot failed (batch exit code is non-zero)
        """
        if not arcjobs:
            return

        self.log.info("processing %d failed jobs" % len(arcjobs))
        for aj in arcjobs:
            jobid=aj['JobID']
            if not jobid:
                # Job was not even submitted, there is no more information
                self.log.warning("%s: Job has not been submitted yet so no information to report", aj['appjobid'])
                continue

            sessionid=jobid[jobid.rfind('/')+1:]
            date = aj['created'].strftime('%Y-%m-%d')
            outd = os.path.join(self.conf.joblog.dir, date, aj['siteName'])
            # Make sure the path to outd exists
            try:
                os.makedirs(outd, 0o755)
            except:
                pass
            # copy from tmp to outd. tmp dir will be cleaned in validator
            localdir = os.path.join(self.tmpdir, sessionid)
            gmlogerrors = os.path.join(localdir, "gmlog", "errors")
            arcjoblog = os.path.join(outd, "%s.log" % aj['appjobid'])
            if not os.path.exists(arcjoblog):
                try:
                    shutil.copy(gmlogerrors, arcjoblog)
                    os.chmod(arcjoblog, 0o644)
                except:
                    self.log.error("Failed to copy %s" % gmlogerrors)

            pilotlog = aj['stdout']
            if not pilotlog and os.path.exists(localdir):
                pilotlogs = [f for f in os.listdir(localdir)]
                for f in pilotlogs:
                    if f.find('.log'):
                        pilotlog = f
            if pilotlog:
                try:
                    shutil.copy(os.path.join(localdir, pilotlog),
                                os.path.join(outd, '%s.out' % aj['appjobid']))
                    os.chmod(os.path.join(outd, '%s.out' % aj['appjobid']), 0o644)
                except Exception as e:
                    self.log.warning("%s: Failed to copy job output for %s: %s" % (aj['appjobid'], jobid, str(e)))

            try:
                smeta = json.loads(aj['metadata'].decode())
            except:
                smeta = None

            # fill info for the final heartbeat
            pupdate = aCTPandaJob()
            pupdate.jobId = aj['appjobid']
            pupdate.state = 'failed'
            pupdate.siteName = aj['siteName']
            pupdate.computingElement = urlparse(aj['cluster']).hostname
            try:
                pupdate.schedulerID = smeta['schedulerid']
            except:
                pupdate.schedulerID = self.conf.panda.schedulerid
            pupdate.pilotID = f"{self.conf.joblog.urlprefix}/{date}/{aj['siteName']}/{aj['appjobid']}.out|Unknown|Unknown|Unknown|Unknown"
            if len(aj["ExecutionNode"]) > 255:
                pupdate.node = aj["ExecutionNode"][:254]
                self.log.warning("%s: Truncating wn hostname from %s to %s" % (aj['pandaid'], aj['ExecutionNode'], pupdate.node))
            else:
                pupdate.node = aj["ExecutionNode"]
            pupdate.node = aj['ExecutionNode']
            pupdate.pilotLog = self.createPilotLog(outd, aj['pandaid'])
            pupdate.cpuConsumptionTime = aj['UsedTotalCPUTime']
            pupdate.cpuConsumptionUnit = 'seconds'
            pupdate.cpuConversionFactor = 1
            pupdate.coreCount = aj['corecount'] or 1
            pupdate.pilotTiming = "0|0|%s|0" % aj['UsedTotalWallTime']
            pupdate.errorCode = 9000
            pupdate.errorDiag = aj['Error']
            # set start/endtime
            if aj['EndTime']:
                pupdate.startTime = self.getStartTime(aj['EndTime'], aj['UsedTotalWallTime']).strftime('%Y-%m-%d %H:%M:%S')
                pupdate.endTime = aj['EndTime'].strftime('%Y-%m-%d %H:%M:%S')
                # Sanity check for efficiency > 100%
                cputimepercore = pupdate.cpuConsumptionTime / pupdate.coreCount
                if aj['UsedTotalWallTime'] < cputimepercore:
                    self.log.warning('%s: Adjusting reported walltime %d to CPU time %d' %
                                      (aj['appjobid'], aj['UsedTotalWallTime'], cputimepercore))
                    pupdate.startTime = (aj['EndTime'] - datetime.timedelta(0, cputimepercore)).strftime('%Y-%m-%d %H:%M:%S')
            else:
                # Set walltime to cputime per core
                pupdate.startTime = self.getStartTime(datetime.datetime.utcnow(), aj['UsedTotalCPUTime'] / pupdate.coreCount).strftime('%Y-%m-%d %H:%M:%S')
                pupdate.endTime = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
            # save the heartbeat file to be used by aCTAutopilot panda update
            try:
                if smeta and smeta.get('harvesteraccesspoint'):
                    hbfile = os.path.join(smeta['harvesteraccesspoint'], 'jobReport.json')
                else:
                    hbfile = os.path.join(self.tmpdir, "heartbeats", str(aj['pandaid'])+".json")
                pupdate.writeToFile(hbfile)
            except Exception as e:
                self.log.warning("%s: Failed to write file %s: %s" % (aj['appjobid'], hbfile, str(e)))


    def updateFailedJobs(self):
        """
        Query jobs in arcstate failed, set to tofetch
        Query jobs in arcstate donefailed, cancelled and lost and not finished in panda.
        If they should be resubmitted, set arcjobid to null in pandajobs and
        cleanupLeftovers() will take care of cleaning up the old jobs.
        If not do post-processing and fill status in pandajobs
        """
        # Get outputs to download for failed jobs
        select = "arcstate='failed'"
        columns = ['id']
        arcjobs = self.dbarc.getArcJobsInfo(select, columns)
        if arcjobs:
            for aj in arcjobs:
                select = "id='"+str(aj["id"])+"'"
                desc = {"arcstate":"tofetch", "tarcstate": self.dbarc.getTimeStamp()}
                self.dbarc.updateArcJobsLazy(desc, select)
            self.dbarc.Commit()

        # Look for failed final states in ARC which are still starting or running in panda
        select = "(arcstate='donefailed' or arcstate='cancelled' or arcstate='lost')"
        select += " and actpandastatus in ('sent', 'starting', 'running', 'transferring')"
        select += " and pandajobs.arcjobid = arcjobs.id and siteName in %s limit 100000" % self.sitesselect
        columns = ['arcstate', 'arcjobid', 'appjobid', 'JobID', 'arcjobs.Error', 'arcjobs.EndTime',
                   'siteName', 'ExecutionNode', 'pandaid', 'UsedTotalCPUTime', 'pandajobs.created',
                   'UsedTotalWallTime', 'ExitCode', 'sendhb', 'stdout', 'metadata', 'cluster', 'corecount']

        jobstoupdate=self.dbarc.getArcJobsInfo(select, columns=columns, tables='arcjobs,pandajobs')

        if len(jobstoupdate) == 0:
            return

        failedjobs = [job for job in jobstoupdate if job['arcstate']=='donefailed']
        if len(failedjobs) != 0:
            self.log.debug("Found %d failed jobs (%s)" % (len(failedjobs), ','.join([j['appjobid'] for j in failedjobs])))
        lostjobs = [job for job in jobstoupdate if job['arcstate']=='lost']
        if len(lostjobs) != 0:
            self.log.debug("Found %d lost jobs (%s)" % (len(lostjobs), ','.join([j['appjobid'] for j in lostjobs])))
        cancelledjobs = [job for job in jobstoupdate if job['arcstate']=='cancelled']
        if len(cancelledjobs) != 0:
            self.log.debug("Found %d cancelled jobs (%s)" % (len(cancelledjobs), ','.join([j['appjobid'] for j in cancelledjobs])))

        failedjobs=self.checkFailed(failedjobs)
        # process all failed jobs that couldn't be resubmitted
        self.processFailed(failedjobs)

        for aj in failedjobs:
            select = "arcjobid='"+str(aj["arcjobid"])+"'"
            desc = {}
            desc["pandastatus"] = "transferring"
            desc["actpandastatus"] = "toclean" # to clean up any output
            desc["endTime"] = aj["EndTime"]
            desc["startTime"] = self.getStartTime(aj['EndTime'], aj['UsedTotalWallTime'])
            desc['error'] = aj['Error']
            # True pilot job may have gone straight to failed, turn off aCT heartbeats if necessary
            if self.sites[aj['siteName']]['truepilot'] and aj["sendhb"] == 1:
                self.log.info("%s: Job finished so stop sending heartbeats", aj['appjobid'])
                desc['sendhb'] = 0
            if not self.sites[aj['siteName']]['truepilot']:
                # Update APFmon (done by wrapper for truepilot)
                self.apfmon.updateJob(aj['appjobid'], 'exiting', exitcode=aj['ExitCode'])
            try:
                self.dbpanda.updateJobsLazy(select, desc)
            except:
                desc['startTime'] = datetime.datetime.utcnow()
                desc['endTime'] = datetime.datetime.utcnow()
                self.dbpanda.updateJobsLazy(select, desc)

        for aj in lostjobs:
            # There is no cleaning to do for lost jobs so just resubmit them
            select = "arcjobid='"+str(aj["arcjobid"])+"'"
            desc={}

            # For truepilot, just set to clean and transferring to clean up arc job
            if self.sites[aj['siteName']]['truepilot']:
                self.log.info("%s: Job is lost, cleaning up arc job", aj['appjobid'])
                desc['sendhb'] = 0
                desc['pandastatus'] = 'transferring'
                desc['actpandastatus'] = 'toclean'
                desc['error'] = 'Job was lost from ARC CE'
            else:
                self.log.info("%s: Resubmitting lost job %d %s %s" % (aj['appjobid'], aj['arcjobid'],aj['JobID'],aj['Error']))
                desc['pandastatus'] = 'starting'
                desc['actpandastatus'] = 'starting'
                desc['arcjobid'] = None
            self.dbpanda.updateJobsLazy(select,desc)

        for aj in cancelledjobs:
            # Jobs were unexpectedly killed in arc, resubmit and clean
            select = "arcjobid='"+str(aj["arcjobid"])+"'"
            desc = {}
            # For truepilot, just set to clean and transferring to clean up arc job
            if self.sites[aj['siteName']]['truepilot']:
                self.log.info("%s: Job was cancelled, cleaning up arc job", aj['appjobid'])
                desc['sendhb'] = 0
                desc['pandastatus'] = 'transferring'
                desc['actpandastatus'] = 'toclean'
                desc['error'] = aj['Error']
            else:
                self.log.info("%s: Resubmitting cancelled job %d %s" % (aj['appjobid'], aj['arcjobid'],aj['JobID']))
                desc["pandastatus"] = "starting"
                desc["actpandastatus"] = "starting"
                desc["arcjobid"] = None
            self.dbpanda.updateJobsLazy(select, desc)

        if failedjobs or lostjobs or cancelledjobs:
            self.dbpanda.Commit()


    def cleanupLeftovers(self):
        """
        Clean jobs left behind in arcjobs table:
         - arcstate=tocancel or cancelling when cluster is empty
         - arcstate=done or cancelled or lost or donefailed when id not in pandajobs
         - arcstate=cancelled and actpandastatus=cancelled/donecancelled/failed/donefailed
        """
        select = "arcstate in ('tocancel', 'cancelling', 'toclean') and (cluster='' or cluster is NULL)"
        jobs = self.dbarc.getArcJobsInfo(select, ['id', 'appjobid'])
        for job in jobs:
            self.log.info("%s: Deleting from arcjobs unsubmitted job %d", job['appjobid'], job['id'])
            self.dbarc.deleteArcJob(job['id'])

        select = "(arcstate='done' or arcstate='lost' or arcstate='cancelled' or arcstate='donefailed') \
                  and arcjobs.id not in (select arcjobid from pandajobs where arcjobid is not NULL)"
        jobs = self.dbarc.getArcJobsInfo(select, ['id', 'appjobid', 'arcstate', 'JobID'])
        cleandesc = {"arcstate":"toclean", "tarcstate": self.dbarc.getTimeStamp()}
        for job in jobs:
            # done jobs should not be there, log a warning
            if job['arcstate'] == 'done':
                self.log.warning("%s: Removing orphaned done job %d", job['appjobid'], job['id'])
            else:
                self.log.info("%s: Cleaning left behind %s job %d", job['appjobid'], job['arcstate'], job['id'])
            self.dbarc.updateArcJobLazy(job['id'], cleandesc)
            if job['JobID'] and job['JobID'].rfind('/') != -1:
                sessionid = job['JobID'][job['JobID'].rfind('/'):]
                localdir = self.tmpdir + sessionid
                shutil.rmtree(localdir, ignore_errors=True)
        if jobs:
            self.dbarc.Commit()

        select = "arcstate='cancelled' and (actpandastatus in ('cancelled', 'donecancelled', 'failed', 'donefailed')) " \
                 "and pandajobs.arcjobid = arcjobs.id and siteName in %s" % self.sitesselect
        cleandesc = {"arcstate":"toclean", "tarcstate": self.dbarc.getTimeStamp()}
        jobs = self.dbarc.getArcJobsInfo(select, ['arcjobs.id', 'arcjobs.appjobid', 'arcjobs.JobID'], tables='arcjobs, pandajobs')
        for job in jobs:
            self.log.info("%s: Cleaning cancelled job %d", job['appjobid'], job['id'])
            self.dbarc.updateArcJobLazy(job['id'], cleandesc)
            if job['JobID'] and job['JobID'].rfind('/') != -1:
                sessionid = job['JobID'][job['JobID'].rfind('/'):]
                localdir = self.tmpdir + sessionid
                shutil.rmtree(localdir, ignore_errors=True)
        if jobs:
            self.dbarc.Commit()


    def process(self):
        """
        Main loop
        """
        self.log.info("Running")
        self.setSites()
        # Check for jobs that panda told us to kill and cancel them in ARC
        self.checkJobstoKill()
        # Check status of arcjobs
        # Query jobs that were submitted since last time
        self.updateStartingJobs()
        # Query jobs in running arcstate with tarcstate sooner than last run
        self.updateRunningJobs('running')
        # Report finishing as transferring
        self.updateRunningJobs('finishing')
        # Query jobs in arcstate done and update pandajobs
        # Set to toclean
        self.updateFinishedJobs()
        # Query jobs in arcstate failed, set to tofetch
        # Query jobs in arcstate done, donefailed, cancelled and lost, set to toclean.
        # If they should be resubmitted, set arcjobid to null in pandajobs
        # If not do post-processing and fill status in pandajobs
        self.updateFailedJobs()
        # Clean up jobs left behind in arcjobs table
        self.cleanupLeftovers()
