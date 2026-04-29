# Handler for filling pandajobs information from arcjobs information. Also
# deals with post-processing of jobs and error handling.

import datetime
import json
import os
import re
import shutil
import gc

from urllib.parse import urlparse
from sqlalchemy import select, update, or_, and_, delete
from act.atlas.aCTATLASProcess import aCTATLASProcess
from act.atlas.aCTPandaJob import aCTPandaJob
from act.atlas.dbModels import PandaJob
from act.arc.dbModels import ArcJob, JobDescription


class aCTATLASStatus(aCTATLASProcess):

    def checkJobstoKill(self):
        """
        Get starting jobs for offline sites and kill them.
        Check for jobs with pandastatus tobekilled and cancel them in ARC:
        - pandastatus NULL: job was killed by panda so nothing to report
        - pandastatus something else: job was manually killed, so create pickle
          and report failed back to panda

        Signal handling strategy:
        - exit is checked before every job update
        """
        offlinesites = [site for site, a in self.sites.items() if a["status"] == "offline"]
        if offlinesites:
            with self.db.Session.begin() as session:
                jobs = session.execute(select(PandaJob.pandaid, PandaJob.arcjobid, PandaJob.siteName, PandaJob.id) \
                                    .where(PandaJob.actpandastatus.in_(['starting', 'sent']), PandaJob.siteName.in_(offlinesites))).all()
                session.execute(update(PandaJob).where(PandaJob.id.in_([job.id for job in jobs])).values(actpandastatus='failed', pandastatus='failed', error="Starting job was killed because queue went offline"))
                session.execute(update(ArcJob).where(ArcJob.id.in_([job.arcjobid for job in jobs if job.arcjobid is not None])).values(arcstate='tocancel'))
                for job in jobs:
                    self.stopOnFlag()
                    self.log.info(f"Cancelling \"starting\" appjob({job.pandaid}) for offline site {job.siteName}")

        # TODO: HARDCODED limit
        # Get jobs killed by panda
        with self.db.Session.begin() as session:
            jobs = session.execute(select(PandaJob.pandaid, PandaJob.arcjobid, PandaJob.pandastatus, PandaJob.id, PandaJob.siteName) \
                                   .where(PandaJob.actpandastatus=='tobekilled', PandaJob.siteName.in_(self.sitesselect)) \
                                   .limit(100)).all()

            if not jobs:
                return

            for job in jobs:

                self.stopOnFlag()
                gc.collect()

                self.log.info(f"Cancelling arcjob({job.id}) for appjob({job.pandaid})")

                # Check if arcjobid is set before cancelling the job
                if not job.arcjobid:
                    session.execute(update(PandaJob).where(PandaJob.id==job.id).values(actpandastatus='cancelled'))
                    continue

                # Put timings in the DB
                arcjobs = session.execute(select(ArcJob.EndTime, ArcJob.UsedTotalWallTime, ArcJob.StdOut, 
                                                 ArcJob.JobID, ArcJob.appjobid, ArcJob.cluster, ArcJob.ExecutionNode,
                                                 ArcJob.UsedTotalCPUTime, ArcJob.ExitCode, ArcJob.Error,
                                                 PandaJob.siteName, PandaJob.metadata_, PandaJob.pandaid,
                                                 PandaJob.sendhb, PandaJob.created, PandaJob.corecount) \
                                                    .join(PandaJob.arcjob) \
                                                    .where(PandaJob.id==job.id, PandaJob.siteName.in_(self.sitesselect))).all()

                desc = {}
                if arcjobs:
                    desc["endTime"] = self.db.getTimeStamp()
                    desc["startTime"] = self.db.getTimeStamp()
                    self.processFailed(arcjobs)

                # Check if job was manually killed
                if job.pandastatus is not None:
                    self.log.info(f"appjob({job.pandaid}): Manually killed, will report failure to panda")
                    # Skip validator since there is no metadata.xml
                    desc["actpandastatus"] = "failed"
                    desc["pandastatus"] = "failed"
                    desc["error"] = "Job was killed in aCT"
                    if self.sites[job.siteName]["truepilot"]:
                        desc["sendhb"] = 0
                else:
                    desc["actpandastatus"] = "cancelled"
                session.execute(update(PandaJob).where(PandaJob.id==job.id).values(**desc))

                # Finally cancel the arc job
                session.execute(update(ArcJob).where(ArcJob.id==job.arcjobid).values(tarcstate=self.db.getTimeStamp(), arcstate='tocancel'))

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

        Signal handling strategy:
        - exit is checked before every job update
        """
        # TODO: HARDCODED limit
        with self.db.Session.begin() as session:
            jobstoupdate = session.execute(select(ArcJob.id, ArcJob.cluster, ArcJob.appjobid) \
                            .join(PandaJob.arcjob) \
                            .where(or_(
                                and_(ArcJob.arcstate.in_(['submitted', 'holding']), PandaJob.actpandastatus=='sent'), 
                                and_(ArcJob.arcstate.in_(['tosubmit', 'submitting', 'submitted', 'holding']), PandaJob.actpandastatus=='running')),
                                PandaJob.siteName.in_(self.sitesselect))
                            .limit(100000)).all()

            if len(jobstoupdate) == 0:
                return
            else:
                idstr = ",".join([job.appjobid for job in jobstoupdate])
                self.log.info(f"Found {len(jobstoupdate)} submitted jobs ({idstr})")

            for aj in jobstoupdate:
                self.stopOnFlag()
                gc.collect()
                desc = {}
                desc["pandastatus"] = "starting"
                desc["actpandastatus"] = "starting"
                if aj.cluster:
                    desc["computingElement"] = urlparse(aj.cluster).hostname
                session.execute(update(PandaJob).where(PandaJob.arcjobid==aj.id).values(**desc))

    def updateRunningJobs(self, state):
        """
        Check for new running jobs.

        pandajobs are updated with:
        - pandastatus
        - node
        - computingElement
        - startTime

        Signal handling strategy:
        - exit is checked before every job update
        """
        # TODO: HARDCODED limit
        # do an inner join to pick up all jobs that should be set to running
        # TODO: pandajobs.starttime will not be updated if a job is resubmitted
        # internally by the ARC part.
        states = ['starting', 'sent']
        if state == 'finishing':
            states.append('running')

        with self.db.Session.begin() as session:
            jobstoupdate = session.execute(select(ArcJob.id, ArcJob.UsedTotalWallTime, ArcJob.ExecutionNode,
                                                  ArcJob.cluster, ArcJob.RequestedSlots, ArcJob.appjobid, ArcJob.tstate,
                                                  PandaJob.pandaid, PandaJob.siteName) \
                                                    .join(PandaJob.arcjob) \
                                                    .where(ArcJob.arcstate==state, PandaJob.actpandastatus.in_(states)) \
                                                    .limit(100000)).all()

            if len(jobstoupdate) == 0:
                return
            else:
                idstr = ",".join([job.appjobid for job in jobstoupdate])
                self.log.info(f"Found {state}: {len(jobstoupdate)} jobs ({idstr})")

            for aj in jobstoupdate:

                self.stopOnFlag()
                gc.collect()

                desc = {}
                desc["pandastatus"] = "running"
                desc["actpandastatus"] = "running"
                if state == "finishing" and datetime.datetime.utcnow() - aj.tstate > datetime.timedelta(minutes=10):
                    desc["pandastatus"] = "transferring"
                    desc["actpandastatus"] = "transferring"
                if len(aj.ExecutionNode) > 255:
                    desc["node"] = aj.ExecutionNode[:254]
                    self.log.warning(f"appjob({aj.pandaid}): Truncating wn hostname from {aj.ExecutionNode} to {desc['node']}")
                else:
                    desc["node"] = aj.ExecutionNode
                desc["computingElement"] = urlparse(aj.cluster).hostname
                desc["startTime"] = self.getStartTime(datetime.datetime.utcnow(), aj.UsedTotalWallTime)
                desc["corecount"] = aj.RequestedSlots

                # When true pilot job has started running, turn of aCT heartbeats
                if self.sites[aj.siteName]["truepilot"]:
                    self.log.info(f"appjob({aj.pandaid}): Job is running so stop sending heartbeats")
                    desc["sendhb"] = 0
                else:
                    # Update APFmon (done by wrapper for truepilot)
                    self.apfmon.updateJob(aj.pandaid, "running")

                session.execute(update(PandaJob).where(PandaJob.arcjobid==aj.id).values(**desc))

    def updateFinishedJobs(self):
        """
        Check for new finished jobs.

        pandajobs are updated with:
        - pandastatus
        - startTime
        - endTime

        Signal handling strategy:
        - exit is checked before every job update
        """
        # TODO: HARDCODED limit
        # don't get jobs already having actpandastatus states treated by
        # validator to avoid race conditions

        with self.db.Session.begin() as session:
            jobstoupdate = session.execute(select(ArcJob.id, ArcJob.UsedTotalWallTime, ArcJob.EndTime,
                                            ArcJob.appjobid, PandaJob.sendhb, PandaJob.siteName) \
                                            .join(PandaJob.arcjob) \
                                            .where(ArcJob.arcstate=='done', PandaJob.siteName.in_(self.sitesselect),
                                                PandaJob.actpandastatus.not_in(['tovalidate', 'toresubmit', 'toclean', 'finished', 'validating', 'cleaning', 'resubmitting'])) \
                                            .limit(100000)).all()

            if len(jobstoupdate) == 0:
                return
            else:
                idstr = ",".join([job.appjobid for job in jobstoupdate])
                self.log.info(f"Found {len(jobstoupdate)} finished jobs ({idstr})")

            for aj in jobstoupdate:

                self.stopOnFlag()
                gc.collect()

                desc = {}
                desc["pandastatus"] = "transferring"
                desc["actpandastatus"] = "tovalidate"
                desc["startTime"] = self.getStartTime(aj.EndTime, aj.UsedTotalWallTime)
                desc["endTime"] = aj.EndTime
                # True pilot job may have gone straight to finished, turn off aCT heartbeats if necessary
                if self.sites[aj.siteName]["truepilot"] and aj.sendhb == 1:
                    self.log.info(f"appjob({aj.appjobid}): Job finished so stop sending heartbeats")
                    desc["sendhb"] = 0

                if not self.sites[aj.siteName]["truepilot"]:
                    # Update APFmon (done by wrapper for truepilot)
                    self.apfmon.updateJob(aj.appjobid, "exiting", exitcode=0)
                session.execute(update(PandaJob).where(PandaJob.arcjobid==aj.id).values(**desc))

    def checkFailed(self, arcjobs):
        """
        Resubmit jobs on specific ARC errors and return the rest.

        Signal handling strategy:
        - exit is checked before every job update
        """
        failedjobs = []
        #resubmitting=False

        with self.db.Session.begin() as session:
            for aj in arcjobs:

                self.stopOnFlag()
                gc.collect()

                if self.sites[aj.siteName]["truepilot"]:
                    self.log.info(f"appjob({aj.appjobid}): No resubmission for true pilot job")
                    failedjobs += [aj]
                    continue
                resubmit = False
                # TODO: errors part of aCTConfigARC should probably be moved to aCTConfigAPP.
                for error in self.arcconf.errors.toresubmit.arcerrors or []:
                    if error in aj.Error:
                        resubmit = True
                if resubmit:
                    self.log.info(f"appjob({aj.appjobid}): Resubmitting arcjob({aj.arcjobid}) arcid({aj.JobID}) {aj.Error}")
                    jd = {}
                    # Validator processes this state before setting back to starting
                    jd["pandastatus"] = "starting"
                    jd["actpandastatus"] = "toresubmit"
                    session.execute(update(PandaJob).where(PandaJob.arcjobid==aj.id).values(**jd))
                    #resubmitting=True
                else:
                    failedjobs += [aj]

        return failedjobs

    def createPilotLog(self, outd, pandaid):
        """
        Create the pilot log messages to appear on panda logger. Takes the gmlog
        'failed' file and errors from the pilot log if available. Creates a
        local copy under tmp/failedlogs.
        """
        nlines = 20
        log = ""
        try:
            f = open(outd+"/gmlog/failed", "r")
            self.log.info(f"GMLOGFAILED: {outd}")
            log += "---------------------------------------------------------------\n"
            log += "GMLOG: failed\n"
            log += "---------------------------------------------------------------\n"
            log += "".join(f.readlines())
            f.close()
        except:
            pass

        import glob
        lf = glob.glob(outd+"/log*")
        try:
            if(os.path.size(lf[0]) > 1048576):
                self.log.info(f"Log file too big: {lf[0]}")
                raise Exception("Log file too big")
            f = open(lf[0],"r")
            lines = f.readlines()
            log += "---------------------------------------------------------------\n"
            log += "LOGFILE: tail\n"
            log += "---------------------------------------------------------------\n"
            lns = []
            for l in lines:
                if re.match(".*error", l, re.IGNORECASE):
                    lns.append(l)
                if re.match(".*warning", l, re.IGNORECASE):
                    lns.append(l)
                if re.match(".*failed", l, re.IGNORECASE):
                    lns.append(l)
            log += "".join(lns[:nlines])
            # copy logfiles to failedlogs dir
            try:
                f = open(os.path.join(self.tmpdir, "failedlogs", f"{pandaid}.log"), "w")
                f.write(log)
                f.close()
            except:
                pass
        except:
            pass
        return log

    def processFailed(self, arcjobs):
        """
        Process jobs for which pilot failed.

        Such jobs have batch exit code non-zero.
        """
        if not arcjobs:
            return
        arcjobs = [row._asdict() for row in arcjobs]
        self.log.info(f"processing {len(arcjobs)} failed jobs")
        for aj in arcjobs:

            self.stopOnFlag()
            gc.collect()

            jobid = aj["JobID"]
            if not jobid:
                # Job was not even submitted, there is no more information
                self.log.warning(f"appjob({aj['appjobid']}): Job has not been submitted yet so no information to report")
                continue

            sessionid = jobid[jobid.rfind("/")+1:]
            date = aj["created"].strftime("%Y-%m-%d")
            outd = os.path.join(self.conf.joblog.dir, date, aj["siteName"])
            # Make sure the path to outd exists
            os.makedirs(outd, 0o755, exist_ok=True)
            # copy from tmp to outd. tmp dir will be cleaned in validator
            localdir = os.path.join(self.tmpdir, sessionid)
            gmlogerrors = os.path.join(localdir, "gmlog", "errors")
            arcjoblog = os.path.join(outd, f"{aj['appjobid']}.log")
            if not os.path.exists(arcjoblog):
                try:
                    shutil.copy(gmlogerrors, arcjoblog)
                    os.chmod(arcjoblog, 0o644)
                except:
                    self.log.error(f"Failed to copy {gmlogerrors}")

            pilotlog = aj["StdOut"]
            if not pilotlog and os.path.exists(localdir):
                pilotlogs = [f for f in os.listdir(localdir)]
                for f in pilotlogs:
                    if f.find(".log"):
                        pilotlog = f
            if pilotlog:
                try:
                    shutil.copy(os.path.join(localdir, pilotlog),
                                os.path.join(outd, f"{aj['appjobid']}.out"))
                    os.chmod(os.path.join(outd, f"{aj['appjobid']}.out"), 0o644)
                except Exception as e:
                    self.log.warning(f"appjob({aj['appjobid']}): Failed to copy job output for arcid({jobid}): {e}")

            try:
                smeta = json.loads(aj["metadata_"].decode())
            except:
                smeta = None

            # fill info for the final heartbeat
            pupdate = aCTPandaJob()
            pupdate.jobId = aj["appjobid"]
            pupdate.state = "failed"
            pupdate.siteName = aj["siteName"]
            pupdate.computingElement = urlparse(aj["cluster"]).hostname
            try:
                pupdate.schedulerID = smeta["schedulerid"]
            except:
                pupdate.schedulerID = self.conf.panda.schedulerid
            pupdate.pilotID = f"{self.conf.joblog.urlprefix}/{date}/{aj['siteName']}/{aj['appjobid']}.out|Unknown|Unknown|Unknown|Unknown"
            if len(aj["ExecutionNode"]) > 255:
                pupdate.node = aj["ExecutionNode"][:254]
                self.log.warning(f"appjob({aj['pandaid']}): Truncating wn hostname from {aj['ExecutionNode']} to {pupdate.node}")
            else:
                pupdate.node = aj["ExecutionNode"]
            pupdate.node = aj["ExecutionNode"]
            pupdate.pilotLog = self.createPilotLog(localdir, aj["pandaid"])
            pupdate.cpuConsumptionTime = aj["UsedTotalCPUTime"]
            pupdate.cpuConsumptionUnit = "seconds"
            pupdate.cpuConversionFactor = 1
            pupdate.coreCount = aj["corecount"] or 1
            pupdate.pilotTiming = f"0|0|{aj['UsedTotalWallTime']}|0"
            pupdate.errorCode = 9000
            pupdate.errorDiag = aj["Error"]
            # set start/endtime
            if aj["EndTime"]:
                pupdate.startTime = self.getStartTime(aj["EndTime"], aj["UsedTotalWallTime"]).strftime("%Y-%m-%d %H:%M:%S")
                pupdate.endTime = aj["EndTime"].strftime("%Y-%m-%d %H:%M:%S")
                # Sanity check for efficiency > 100%
                cputimepercore = pupdate.cpuConsumptionTime / pupdate.coreCount
                if aj["UsedTotalWallTime"] < cputimepercore:
                    self.log.warning(f"appjob({aj['appjobid']}): Adjusting reported walltime {aj['UsedTotalWallTime']} to CPU time {cputimepercore}")
                    pupdate.startTime = (aj["EndTime"] - datetime.timedelta(0, cputimepercore)).strftime("%Y-%m-%d %H:%M:%S")
            else:
                # Set walltime to cputime per core
                pupdate.startTime = self.getStartTime(datetime.datetime.utcnow(), aj["UsedTotalCPUTime"] / pupdate.coreCount).strftime("%Y-%m-%d %H:%M:%S")
                pupdate.endTime = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            # save the heartbeat file to be used by aCTAutopilot panda update
            try:
                if smeta and smeta.get("harvesteraccesspoint"):
                    hbfile = os.path.join(smeta["harvesteraccesspoint"], "jobReport.json")
                else:
                    hbfile = os.path.join(self.tmpdir, "heartbeats", f"{aj['pandaid']}.json")
                pupdate.writeToFile(hbfile)
            except Exception as e:
                self.log.warning(f"appjob({aj['appjobid']}): Failed to write file {hbfile}: {e}")

    def updateFailedJobs(self):
        """
        Handle jobs in different unsuccessful states.

        Set jobs in arcstate failed to tofetch. Query jobs in arcstate
        donefailed, cancelled and lost as well as not finished jobs in panda.
        If they should be resubmitted, set arcjobid to null in pandajobs and
        cleanupLeftovers() will take care of cleaning up the old jobs.
        If not do post-processing and fill status in pandajobs.

        Signal handling strategy:
        - exit is checked before every job update
        """
        # fetch failed jobs
        with self.db.Session.begin() as session:
            session.execute(update(ArcJob).where(ArcJob.arcstate=='failed').values(arcstate='tofetch', tarcstate=self.db.getTimeStamp()))

        # TODO: HARDCODED limit
        # Look for failed final states in ARC which are still starting or running in panda
        with self.db.Session() as session:
            jobstoupdate = session.execute(select(ArcJob.id, ArcJob.arcstate, ArcJob.appjobid, ArcJob.JobID, ArcJob.Error,
                                                  ArcJob.EndTime, ArcJob.ExecutionNode, ArcJob.UsedTotalCPUTime,
                                                  ArcJob.UsedTotalWallTime, ArcJob.ExitCode, ArcJob.StdOut, ArcJob.cluster,
                                                  PandaJob.arcjobid, PandaJob.siteName, PandaJob.pandaid,
                                                  PandaJob.created, PandaJob.sendhb, PandaJob.metadata_, PandaJob.corecount)
                                            .join(PandaJob.arcjob) \
                                            .where(ArcJob.arcstate.in_(['donefailed', 'cancelled', 'lost']), 
                                                    PandaJob.actpandastatus.in_(['sent', 'starting', 'running', 'transferring']),
                                                    PandaJob.siteName.in_(self.sitesselect))
                                            .limit(1000)).all()

        if len(jobstoupdate) == 0:
            return

        # get donefailed jobs
        failedjobs = [job for job in jobstoupdate if job.arcstate == "donefailed"]
        if len(failedjobs) != 0:
            idstr = ",".join([job.appjobid for job in failedjobs])
            self.log.info(f"Found {len(failedjobs)} failed jobs ({idstr})")

        # get lost jobs
        lostjobs = [job for job in jobstoupdate if job.arcstate == "lost"]
        if len(lostjobs) != 0:
            idstr = ",".join([job.appjobid for job in lostjobs])
            self.log.info(f"Found {len(lostjobs)} lost jobs ({idstr})")

        # get cancelled jobs
        cancelledjobs = [job for job in jobstoupdate if job.arcstate == "cancelled"]
        if len(cancelledjobs) != 0:
            idstr = ",".join([job.appjobid for job in cancelledjobs])
            self.log.info(f"Found {len(cancelledjobs)} cancelled jobs ({idstr})")

        # try to resubmit on certain errors
        failedjobs = self.checkFailed(failedjobs)

        # process all failed jobs that couldn't be resubmitted
        self.processFailed(failedjobs)

        with self.db.Session.begin() as session:
            for aj in failedjobs:
                self.stopOnFlag()
                gc.collect()
                desc = {}
                desc["pandastatus"] = "transferring"
                desc["actpandastatus"] = "toclean" # to clean up any output
                desc["endTime"] = aj.EndTime
                desc["startTime"] = self.getStartTime(aj.EndTime, aj.UsedTotalWallTime)
                desc["error"] = aj.Error
                # True pilot job may have gone straight to failed, turn off aCT heartbeats if necessary
                if self.sites[aj.siteName]["truepilot"] and aj.sendhb == 1:
                    self.log.info(f"appjob({aj.appjobid}): Job finished so stop sending heartbeats")
                    desc["sendhb"] = 0

                if not self.sites[aj.siteName]["truepilot"]:
                    # Update APFmon (done by wrapper for truepilot)
                    self.apfmon.updateJob(aj.appjobid, "exiting", exitcode=aj.ExitCode)
                session.execute(update(PandaJob).where(PandaJob.arcjobid==aj.id).values(**desc))

        with self.db.Session.begin() as session:
            # clean lost pilot jobs or resubmit other lost jobs
            for aj in lostjobs:
                self.stopOnFlag()
                gc.collect()
                desc = {}

                # For truepilot, just set to clean and transferring to clean up arc job
                if self.sites[aj.siteName]["truepilot"]:
                    self.log.info(f"appjob({aj.appjobid}): Job is lost, cleaning up arc job")
                    desc["sendhb"] = 0
                    desc["pandastatus"] = "transferring"
                    desc["actpandastatus"] = "toclean"
                    desc["error"] = "Job was lost from ARC CE"
                else:
                    self.log.info(f"appjob({aj.appjobid}): Resubmitting lost arcjob({aj.arcjobid}) arcid({aj.JobID}) {aj.Error}")
                    desc["pandastatus"] = "starting"
                    desc["actpandastatus"] = "starting"
                    desc["arcjobid"] = None
                session.execute(update(PandaJob).where(PandaJob.arcjobid==aj.id).values(**desc))

        with self.db.Session.begin() as session:
            # clean cancelled pilot jobs and resubmit other cancelled jobs
            for aj in cancelledjobs:
                self.stopOnFlag()
                gc.collect()
                # Jobs were unexpectedly killed in arc, resubmit and clean
                desc = {}
                # For truepilot, just set to clean and transferring to clean up arc job
                if self.sites[aj.siteName]["truepilot"]:
                    self.log.info(f"appjob({aj.appjobid}): Job was cancelled, cleaning up ARC job")
                    desc["sendhb"] = 0
                    desc["pandastatus"] = "transferring"
                    desc["actpandastatus"] = "toclean"
                    desc["error"] = aj.Error

                # Jobs that fail submitting to ARC are cancelled. Such jobs should
                # not be resubmitted.
                elif not aj.JobID:
                    desc["pandastatus"] = "transferring"
                    desc["actpandastatus"] = "toclean"

                else:
                    self.log.info(f"appjob({aj.appjobid}): Resubmitting cancelled arcjob({aj.arcjobid}) arcid({aj.JobID})")
                    desc["pandastatus"] = "starting"
                    desc["actpandastatus"] = "starting"
                    desc["arcjobid"] = None
                session.execute(update(PandaJob).where(PandaJob.arcjobid==aj.id).values(**desc))

    def cleanupLeftovers(self):
        """
        Clean jobs left behind in arcjobs table.

        The following jobs are left behind:
        - arcstate=tocancel or cancelling when cluster is empty
        - arcstate=done or cancelled or lost or donefailed when id not in pandajobs
        - arcstate=cancelled and actpandastatus=cancelled/donecancelled/failed/donefailed

        Signal handling strategy:
        - exit is checked before every job update
        """
        # Even though the transaction probably gets rolled back
        # automatically, it is nice to handle it explicitly. Also, this
        # simplifies the method with one nested block.
        with self.db.Session.begin() as session:
            jobs = session.execute(select(ArcJob.id, ArcJob.appjobid, ArcJob.jobdesc) \
                                   .where(ArcJob.arcstate.in_(['tocancel', 'cancelling', 'toclean']), ((ArcJob.cluster == '') | (ArcJob.cluster.is_(None))))).all()
            if jobs:
                session.execute(delete(JobDescription).where(JobDescription.id.in_([job.jobdesc for job in jobs])))
                session.execute(delete(ArcJob).where(ArcJob.id.in_([job.id for job in jobs])))
                for job in jobs:
                    self.stopOnFlag()
                    gc.collect()
                    self.log.info(f"appjob({job.appjobid}): Deleting from arcjobs unsubmitted arcjob({job.id})")

        with self.db.Session.begin() as session:
            jobs = session.execute(select(ArcJob.id, ArcJob.appjobid, ArcJob.arcstate, ArcJob.JobID) \
                            .outerjoin(PandaJob, ArcJob.id == PandaJob.arcjobid) \
                            .where(ArcJob.arcstate.in_(['done', 'lost', 'cancelled', 'donefailed']), PandaJob.arcjobid.is_(None))).all()
            cleandesc = {"arcstate": "toclean", "tarcstate": self.db.getTimeStamp()}
            for job in jobs:
                self.stopOnFlag()
                gc.collect()
                # done jobs should not be there, log a warning
                if job.arcstate == "done":
                    self.log.warning(f"appjob({job.appjobid}): Removing orphaned done arcjob({job.id})")
                else:
                    self.log.info(f"appjob({job.appjobid}): Cleaning left behind {job.arcstate} arcjob({job.id})")
                session.execute(update(ArcJob).where(ArcJob.id==job.id).values(**cleandesc))
                if job.JobID and job.JobID.rfind("/") != -1:
                    sessionid = job.JobID[job.JobID.rfind("/"):]
                    localdir = self.tmpdir + sessionid
                    shutil.rmtree(localdir, ignore_errors=True)

        with self.db.Session.begin() as session:
            jobs = session.execute(select(ArcJob.id, ArcJob.appjobid, ArcJob.JobID) \
                                    .join(PandaJob.arcjob) \
                                    .where(ArcJob.arcstate=='cancelled', PandaJob.actpandastatus.in_(['cancelled', 'donecancelled', 'failed', 'donefailed']),
                                           PandaJob.siteName.in_(self.sitesselect))).all()
            cleandesc = {"arcstate": "toclean", "tarcstate": self.db.getTimeStamp()}
            for job in jobs:
                self.stopOnFlag()
                gc.collect()
                self.log.info(f"appjob({job.appjobid}): Cleaning cancelled arcjob({job.id})")
                session.execute(update(ArcJob).where(ArcJob.id==job.id).values(**cleandesc))
                if job.JobID and job.JobID.rfind("/") != -1:
                    sessionid = job.JobID[job.JobID.rfind("/"):]
                    localdir = self.tmpdir + sessionid
                    shutil.rmtree(localdir, ignore_errors=True)

    def process(self):
        """
        Main loop
        """
        self.log.info("Running")
        #gc.set_debug(gc.DEBUG_STATS)# | gc.DEBUG_COLLECTABLE | gc.DEBUG_UNCOLLECTABLE)
        self.setSites()
        # Check for jobs that panda told us to kill and cancel them in ARC
        self.checkJobstoKill()
        # Check status of arcjobs
        # Query jobs that were submitted since last time
        self.updateStartingJobs()
        # Query jobs in running arcstate with tarcstate sooner than last run
        self.updateRunningJobs("running")
        # Report finishing as transferring
        self.updateRunningJobs("finishing")
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
