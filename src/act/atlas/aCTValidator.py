import datetime
import errno
import json
import os
import queue
import re
import shutil
import threading
import time
from enum import Enum

import arc
from act.atlas.aCTATLASProcess import aCTATLASProcess
from act.atlas.aCTPandaJob import aCTPandaJob
from act.common import aCTUtils
from act.common.aCTProxy import aCTProxy


class aCTValidator(aCTATLASProcess):
    '''
    Validate output files for finished jobs, cleanup output files for failed jobs.
    '''

    def setup(self):
        super().setup()

        # Use production role proxy for checking and removing files
        # Get DN from configured proxy file
        cred_type = arc.initializeCredentialsType(arc.initializeCredentialsType.SkipCredentials)
        uc = arc.UserConfig(cred_type)
        uc.ProxyPath(self.arcconf.voms.proxypath)
        cred = arc.Credential(uc)
        dn = cred.GetIdentityName()

        actp = aCTProxy(self.log)
        # Beware hard-coded production role
        proxyfile = actp.path(dn, '/atlas/Role=production')
        if not proxyfile:
            raise Exception('Could not find proxy with production role in proxy table')
        self.log.info('set proxy path to %s' % proxyfile)

        self.uc = arc.UserConfig(cred_type)
        self.uc.ProxyPath(str(proxyfile))
        self.uc.UtilsDirPath(str(arc.UserConfig.ARCUSERDIRECTORY))

        self.checkManager = OutputCheckManager(self.log, self.uc)
        self.failedRemmgr = FileRemoveManager(self.log, self.uc)
        self.resubRemmgr = FileRemoveManager(self.log, self.uc)
        self.heartbeatDownloader = HeartbeatDownloader(self.tmpdir, self.log, self.uc)

        self.recoverIntermediateStates()

    def finish(self):
        """Properly handle all threads and job states."""
        # signal threads to terminate
        self.log.debug("Terminating OutputCheckManager")
        self.checkManager.terminate.set()
        self.log.debug("Terminating FileRemoveManager for jobs to clean")
        self.failedRemmgr.terminate.set()
        self.log.debug("Terminating FileRemoveManager for jobs to resubmit")
        self.resubRemmgr.terminate.set()
        self.log.debug("Terminating HeartbeatDownloader")
        self.heartbeatDownloader.terminate.set()

        # wait for threads to finish
        self.checkManager.thread.join()
        self.log.debug("OutputCheckManager joined")
        self.failedRemmgr.thread.join()
        self.log.debug("FileRemoveManager for jobs to clean joined")
        self.resubRemmgr.thread.join()
        self.log.debug("FileRemoveManager for jobs to resubmit joined")
        self.heartbeatDownloader.thread.join()
        self.log.debug("HeartbeatDownloader joined")

        self.recoverIntermediateStates()

        super().finish()

    def recoverIntermediateStates(self):
        """
        Recover jobs that were left in intermediate state.

        When aCT validator has to stop, the easiest way to handle the jobs
        that are in the middle of validation or cleaning is to put them
        back into original state and retry on the next run. Even if the
        process ends abruptly without being able to restore jobs, the
        restoration can still be done later, when the process starts again.
        """
        select = " (actpandastatus='validating') "
        desc = {"actpandastatus": "tovalidate"}
        self.dbpanda.updateJobs(select, desc)

        select = " (actpandastatus='cleaning') "
        desc = {"actpandastatus": "toclean"}
        self.dbpanda.updateJobs(select, desc)

        select = " (actpandastatus='resubmitting') "
        desc = {"actpandastatus": "toresubmit"}
        self.dbpanda.updateJobs(select, desc)

    def copyFinishedFiles(self, arcjobid, extractmetadata):
        """
        - if extractmetadata: (normal arc jobs, not true pilot jobs)
           - store heartbeat file under tmp/pickle or under harvester access
             point if specified
        - copy .job.log file to jobs/date/pandaqueue/pandaid.out
        - copy gmlog errors to jobs/date/pandaqueue/pandaid.log
        """

        columns = ['JobID', 'appjobid', 'cluster', 'UsedTotalWallTime', 'arcjobs.EndTime',
                   'ExecutionNode', 'stdout', 'fairshare', 'pandajobs.created', 'metadata']
        select = "arcjobs.id=%d AND arcjobs.id=pandajobs.arcjobid" % arcjobid
        aj = self.dbarc.getArcJobsInfo(select, columns=columns, tables='arcjobs,pandajobs')
        if not aj or 'JobID' not in aj[0] or not aj[0]['JobID']:
            self.log.error('No JobID in arcjob %s: %s'%(str(arcjobid), str(aj)))
            return False
        aj = aj[0]
        jobid = aj['JobID']
        sessionid = jobid[jobid.rfind('/')+1:]
        date = aj['created'].strftime('%Y-%m-%d')
        if extractmetadata:
            try:
                jobinfo = aCTPandaJob(filename=os.path.join(self.tmpdir, sessionid, 'heartbeat.json'))
            except Exception as x:
                self.log.error("%s: failed to load heartbeat file for arcjob %s: %s" %(aj['appjobid'], jobid, x))
                jobinfo = aCTPandaJob(jobinfo={'jobId': aj['appjobid'], 'state': 'finished'})

            # update heartbeat and dump to tmp/heartbeats
            jobinfo.computingElement = arc.URL(str(aj['cluster'])).Host()
            if hasattr(jobinfo, 'startTime') and hasattr(jobinfo, 'endTime'):
                # take values from the pilot
                jobinfo.startTime = datetime.datetime.utcfromtimestamp(jobinfo.startTime).strftime('%Y-%m-%d %H:%M:%S')
                jobinfo.endTime = datetime.datetime.utcfromtimestamp(jobinfo.endTime).strftime('%Y-%m-%d %H:%M:%S')
            else:
                # Use ARC values
                if aj['EndTime']:
                    # datetime cannot be serialised to json so use string (for harvester)
                    jobinfo.startTime = (aj['EndTime'] - datetime.timedelta(0, aj['UsedTotalWallTime'])).strftime('%Y-%m-%d %H:%M:%S')
                    jobinfo.endTime = aj['EndTime'].strftime('%Y-%m-%d %H:%M:%S')
                    # Sanity check for efficiency > 100%
                    cputimepercore = getattr(jobinfo, 'cpuConsumptionTime', 0) / getattr(jobinfo, 'coreCount', 1)
                    if aj['UsedTotalWallTime'] < cputimepercore:
                        self.log.warning('%s: Adjusting reported walltime %d to CPU time %d' %
                                          (aj['appjobid'], aj['UsedTotalWallTime'], cputimepercore))
                        jobinfo.startTime = (aj['EndTime'] - datetime.timedelta(0, cputimepercore)).strftime('%Y-%m-%d %H:%M:%S')
                else:
                    self.log.warning('%s: no endtime found' % aj['appjobid'])
            if len(aj["ExecutionNode"]) > 255:
                jobinfo.node = aj["ExecutionNode"][:254]
                self.log.warning("%s: Truncating wn hostname from %s to %s" % (aj['appjobid'], aj['ExecutionNode'], jobinfo.node))
            else:
                jobinfo.node = aj["ExecutionNode"]

            try:
                smeta = json.loads(aj['metadata'].decode())
            except:
                smeta = None

            if smeta and smeta.get('harvesteraccesspoint'):
                # de-serialise the metadata to json
                try:
                    jobinfo.metaData = json.loads(jobinfo.metaData)
                except Exception as e:
                    self.log.warning("%s: no metaData in pilot metadata: %s" % (aj['appjobid'], str(e)))
                jobinfo.writeToFile(os.path.join(smeta['harvesteraccesspoint'], 'jobReport.json'))
            else:
                jobinfo.writeToFile(os.path.join(self.tmpdir, "heartbeats", "%s.json" % aj['appjobid']))

        # copy to joblog dir files downloaded for the job: gmlog errors and pilot log
        outd = os.path.join(self.conf.joblog.dir, date, aj['fairshare'])
        os.makedirs(outd, 0o755, exist_ok=True)

        localdir = os.path.join(self.tmpdir, sessionid)
        gmlogerrors = os.path.join(localdir, "gmlog", "errors")
        arcjoblog = os.path.join(outd, "%s.log" % aj['appjobid'])
        if not os.path.exists(arcjoblog):
            try:
                shutil.move(gmlogerrors, arcjoblog)
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
                shutil.move(os.path.join(localdir, pilotlog),
                            os.path.join(outd, '%s.out' % aj['appjobid']))
                os.chmod(os.path.join(outd, '%s.out' % aj['appjobid']), 0o644)
            except Exception as e:
                self.log.error("Failed to copy file %s: %s" % (os.path.join(localdir,pilotlog), str(e)))
                return False

        return True

    def extractOutputFilesFromMetadata(self, arcjobid):
        aj = self.dbarc.getArcJobInfo(arcjobid, columns=["JobID", "appjobid"])
        if not aj or 'JobID' not in aj or not aj['JobID']:
            self.log.error("failed to find arcjobid %s in database" % arcjobid)
            return {}

        jobid=aj['JobID']
        sessionid=jobid[jobid.rfind('/')+1:]
        try:
            jobinfo = aCTPandaJob(filename=os.path.join(self.tmpdir, sessionid, 'heartbeat.json'))
            metadata = getattr(jobinfo, 'xml') # travis doesn't like jobinfo.xml
        except Exception as x:
            self.log.error("%s: failed to extract metadata for arcjob %s: %s" %(aj['appjobid'], sessionid, x))
            return {}

        try:
            outputfiles = json.loads(metadata)
        except Exception as e:
            self.log.error("%s: failed to load output file info for arcjob %s: %s" % (aj['appjobid'], sessionid, str(e)))
            return {}

        surls = {}
        for attrs in outputfiles.values():
            try:
                size = attrs['fsize']
                adler32 = attrs['adler32']
                surl = attrs['surl']
                # davs -> srm for ndgf
                res=re.match('davs://dav.ndgf.org:443/(.+)',surl)
                if res is not None:
                    surl = 'srm://srm.ndgf.org:8443/'+res.group(1)
                se = arc.URL(str(surl)).Host()
            except Exception as x:
                self.log.error('%s: %s' % (aj['appjobid'], x))
            else:
                checksum = "adler32:"+ (adler32 or '00000001')
                if se not in surls:
                    surls[se]= []
                surls[se] += [{"surl":surl, "fsize":size, "checksum":checksum, "arcjobid":arcjobid}]

        return surls


    def cleanDownloadedJob(self, arcjobid):
        '''
        Remove directory to which job was downloaded.
        '''

        job = self.dbarc.getArcJobInfo(arcjobid, columns=['JobID','appjobid'])
        if job and job['JobID']:
            sessionid = job['JobID'][job['JobID'].rfind('/'):]
            localdir = self.tmpdir + sessionid
            shutil.rmtree(localdir, ignore_errors=True)
            pandaid=job['appjobid']
            pandainputdir = os.path.join(self.tmpdir, 'inputfiles', str(pandaid))
            shutil.rmtree(pandainputdir, ignore_errors=True)


    def validateFinishedJobs(self):
        """
        Check for jobs with actpandastatus tovalidate and pandastatus running
        Check if the output files in pilot heartbeat json are valid.
        If yes, move to actpandastatus to finished, if not, move pandastatus
        and actpandastatus to failed.

        Signal handling strategy:
        - exit is checked before every job update
        """
        # TODO: HARDCODED limit
        # get all jobs with pandastatus running and actpandastatus tovalidate
        select = "(pandastatus='transferring' and actpandastatus='tovalidate') and siteName in %s limit 1000" % self.sitesselect
        columns = ["arcjobid", "pandaid", "siteName", "metadata"]
        jobstoupdate=self.dbpanda.getJobs(select, columns=columns)

        arcdesc = {"arcstate": "toclean", "tarcstate": self.dbarc.getTimeStamp()}

        # Skip validation for the true pilot jobs, just copy logs, set to done and clean arc job
        toremove = []
        pandadesc = {"pandastatus": None, "actpandastatus": "done"}
        for job in jobstoupdate:
            self.stopOnFlag()
            if self.sites[job['siteName']]['truepilot']:
                self.log.info(f"{job['pandaid']}: Pilot job, skip validation")
                if not self.copyFinishedFiles(job["arcjobid"], False):
                    self.log.warning(f"{job['pandaid']}: Failed to copy log files")
                self.cleanDownloadedJob(job['arcjobid'])
                self.dbarc.updateArcJob(job['arcjobid'], arcdesc)
                select = f"arcjobid={job['arcjobid']}"
                self.dbpanda.updateJobs(select, pandadesc)
            else:
                toremove.append(job)

        # pull out output file info from pilot heartbeat json into dict, order by SE
        surls = {}
        pandadesc = {"actpandastatus": "failed", "pandastatus": "failed"}
        for job in toremove:
            self.stopOnFlag()
            jobsurls = self.extractOutputFilesFromMetadata(job["arcjobid"])
            if not jobsurls:
                # Problem extracting files, fail job, clean ARC job
                # and job files.
                self.log.error(f"{job['pandaid']}: Cannot validate output of arcjob {job['arcjobid']}")
                self.cleanDownloadedJob(job['arcjobid'])
                self.dbarc.updateArcJob(job['arcjobid'], arcdesc)
                select = f"arcjobid={job['arcjobid']}"
                self.dbpanda.updateJobs(select, pandadesc)
            else:
                select = f"arcjobid={job['arcjobid']}"
                desc = {"actpandastatus": "validating"}
                self.dbpanda.updateJobs(select, desc)
                for se in jobsurls:
                    surls.setdefault(se, []).extend(jobsurls[se])

        # send surls to checker and start it if not active
        if surls:
            self.checkManager.taskQueue.put(surls)
        if not self.checkManager.thread.is_alive():
            self.log.debug("OutputCheckManager not alive, starting")
            self.checkManager.terminate.clear()
            self.checkManager.start()

        # process results from checker
        while not self.checkManager.resultQueue.empty():
            self.stopOnFlag()
            jobid, status = self.checkManager.resultQueue.get()
            if status == JobStatus.OK:
                self.log.info(f"Successful output file check for arcjob {jobid}")
                if not self.copyFinishedFiles(jobid, True):
                    self.log.warning(f"Failed to copy log files for arcjob {jobid}, skipping")
                    # TODO: should we short circuit here? What is wrong with setting toclean?
                    ## id was gone already
                    #continue
                self.cleanDownloadedJob(jobid)
                self.dbarc.updateArcJob(jobid, arcdesc)
                select = f"arcjobid={jobid}"
                desc = {"pandastatus": "finished", "actpandastatus": "finished"}
                self.dbpanda.updateJobs(select, desc)
            elif status == JobStatus.FAILED:
                # output file failed, set toresubmit to clean up output and resubmit
                self.log.info(f"Failed output file check for arcjob {jobid}, resubmitting")
                select = f"arcjobid={jobid}"
                desc = {"pandastatus": "starting", "actpandastatus": "toresubmit"}
                self.dbpanda.updateJobs(select, desc)
            else:
                # Retry next time
                self.log.info(f"Failed output file check for arcjob {jobid}, will retry")
                select = f"arcjobid={jobid}"
                desc = {"actpandastatus": "tovalidate"}
                self.dbpanda.updateJobs(select, desc)

    def cleanFailedJobs(self):
        '''
        Check for jobs with actpandastatus toclean and pandastatus transferring.
        Delete the output files in pilot heartbeat json
        Move actpandastatus to failed.

        Signal handling strategy:
        - exit is checked before every job update
        '''
        # TODO: HARDCODED limit
        # get all jobs with pandastatus transferring and actpandastatus toclean
        select = f"(pandastatus='transferring' and actpandastatus='toclean') and siteName in {self.sitesselect} limit 1000"
        columns = ["arcjobid", "pandaid", "siteName"]
        jobstoupdate = self.dbpanda.getJobs(select, columns=columns)

        cleandesc = {"arcstate": "toclean", "tarcstate": self.dbarc.getTimeStamp()}
        pandaFailDesc = {"actpandastatus": "failed", "pandastatus": "failed"}

        # For truepilot jobs, don't try to clean outputs (too dangerous), just clean arc job
        toremove = []
        for job in jobstoupdate:
            self.stopOnFlag()
            # TODO: should we restore previous true pilot behaviour?
            ## Cleaning a bad storage can block the validator, so skip cleaning in all cases
            #if True:
            if self.sites[job['siteName']]['truepilot']:
                self.log.info(f"{job['pandaid']}: Skip cleanup of output files")
                self.cleanDownloadedJob(job["arcjobid"])
                self.dbarc.updateArcJob(job["arcjobid"], cleandesc)
                select = f"arcjobid={job['arcjobid']}"
                self.dbpanda.updateJobs(select, pandaFailDesc)
                toremove.append(job)

        # pull out output file info from pilot heartbeat json into dict, order by SE
        surls = {}
        for job in toremove:
            self.stopOnFlag()
            jobsurls = self.extractOutputFilesFromMetadata(job["arcjobid"])
            if not jobsurls:
                # Problem extracting files, fail job, clean ARC job and files.
                self.log.error(f"{job['pandaid']}: Cannot remove output of arcjob {job['arcjobid']}")
                self.cleanDownloadedJob(job["arcjobid"])
                self.dbarc.updateArcJob(job["arcjobid"], cleandesc)
                select = f"arcjobid={job['arcjobid']}"
                self.dbpanda.updateJobs(select, pandaFailDesc)
            else:
                select = f"arcjobid={job['arcjobid']}"
                desc = {"actpandastatus": "cleaning"}
                self.dbpanda.updateJobs(select, desc)
                for se in jobsurls:
                    surls.setdefault(se, []).extend(jobsurls[se])

        # send surls to remover and start it if not active
        if surls:
            self.failedRemmgr.taskQueue.put(surls)
        if not self.failedRemmgr.thread.is_alive():
            self.log.debug("FileRemoveManager for jobs to clean not alive, starting")
            self.failedRemmgr.terminate.clear()
            self.failedRemmgr.start()

        # process results from remover
        while not self.failedRemmgr.resultQueue.empty():
            self.stopOnFlag()
            jobid, surl, status = self.failedRemmgr.resultQueue.get()
            if status in (JobStatus.OK, JobStatus.FAILED):
                if status == JobStatus.OK:
                    self.log.info(f"Successfuly removed output files for failed arcjob {jobid}")
                else:
                    # If unretriably failed, there is not much we can do except
                    # continue
                    self.log.info(f"Output file removal failed for arcjob {jobid}, skipping")
                self.cleanDownloadedJob(jobid)
                self.dbarc.updateArcJob(jobid, cleandesc)
                select = f"arcjobid={jobid}"
                self.dbpanda.updateJobs(select, pandaFailDesc)
            else:
                # Retry next time
                self.log.info(f"Output file removal failed for arcjob {jobid}, will retry")
                select = f"arcjobid={jobid}"
                desc = {"actpandastatus": "toclean"}
                self.dbpanda.updateJobs(select, desc)

    def resubmitNonARCJobs(self):
        """Resubmit jobs with no ARC job ID."""
        # TODO: HARDCODED limit
        select = f"(actpandastatus='toresubmit' and arcjobid=NULL) and siteName in {self.sitesselect} limit 1000"
        columns = ["pandaid", "id"]

        jobstoupdate = self.dbpanda.getJobs(select, columns=columns)

        for job in jobstoupdate:
            self.stopOnFlag()
            self.log.info(f"{job['pandaid']}: resubmitting")
            select = f"id={job['id']}"
            desc = {"actpandastatus": "starting", "arcjobid": None}
            self.dbpanda.updateJobs(select, desc)

    def cleanResubmittingJobs(self):
        '''
        Check for jobs with actpandastatus toresubmit and pandastatus starting.
        Delete the output files in pilot heartbeat json
        Move actpandastatus to starting.

        Signal handling strategy:
        - exit is checked before every job update
        '''
        # TODO: HARDCODED limit
        select = "actpandastatus='toresubmit' and arcjobs.id=pandajobs.arcjobid limit 100"
        columns = ["pandajobs.arcjobid", "pandajobs.pandaid", "arcjobs.JobID", "arcjobs.arcstate", "arcjobs.restartstate"]
        jobstoupdate = self.dbarc.getArcJobsInfo(select, columns=columns, tables='arcjobs, pandajobs')

        # Set jobs to resubmitting to prevent them being grabbed next run.
        # Create a list of jobs to remove outputs and a list to fetch
        # heartbeats.
        desc = {"actpandastatus": "resubmitting"}
        toremove = []
        todownload = []
        for job in jobstoupdate:
            select = f"arcjobid={job['arcjobid']}"
            self.dbpanda.updateJobs(select, desc)
            if job["arcstate"] not in ("donefailed", "done", "lost", "cancelled"):
                todownload.append(job)
            else:
                toremove.append(job)

        # Queue heartbeat downloads for manually resubmitted jobs and jobs
        # whose downloads are finished to remove list.
        downloaded = self.downloadHeartbeats(todownload)
        manualIDs = set([job["arcjobid"] for job in downloaded])
        toremove.extend(downloaded)

        # arcjobs db table update dict to clean ARC job
        arcdesc = {'arcstate': 'toclean', 'tarcstate': self.dbarc.getTimeStamp()}

        # pull out output file info from pilot heartbeat json into dict, order by SE
        surls = {}
        for job in toremove:
            self.stopOnFlag()
            jobsurls = self.extractOutputFilesFromMetadata(job["arcjobid"])
            if not jobsurls:
                if job in downloaded or job["restartstate"] != "Finishing" and job["arcstate"] != "done":
                    # Clean job files and ARC job, finish resubmission if
                    # resubmitted manually or job failed before finishing,
                    # since there are likely no output files.
                    self.log.error(f"{job['pandaid']}: Cannot remove output of arcjob {job['arcjobid']}, resubmit finished")
                    self.cleanDownloadedJob(job["arcjobid"])
                    self.dbarc.updateArcJob(job['arcjobid'], arcdesc)
                    select = f"arcjobid={job['arcjobid']}"
                    desc = {"actpandastatus": "starting", "arcjobid": None}
                    self.dbpanda.updateJobs(select, desc)
                else:
                    # Otherwise fail job whose outputs cannot be cleaned.
                    self.log.error(f"{job['pandaid']}: Cannot remove output of arcjob {job['arcjobid']}, setting to failed")
                    self.cleanDownloadedJob(job["arcjobid"])
                    self.dbarc.updateArcJob(job['arcjobid'], arcdesc)
                    select = f"arcjobid={job['arcjobid']}"
                    desc = {"actpandastatus": "failed", "pandastatus": "failed"}
                    self.dbpanda.updateJobs(select, desc)
            else:
                for se in jobsurls:
                    surls.setdefault(se, []).extend(jobsurls[se])

        # send surls to remover and start it if not active
        if surls:
            self.resubRemmgr.taskQueue.put(surls)
        if not self.resubRemmgr.thread.is_alive():
            self.log.debug("FileRemoveManager for jobs to resubmit not alive, starting")
            self.resubRemmgr.terminate.clear()
            self.resubRemmgr.start()

        # process results from remover
        while not self.resubRemmgr.resultQueue.empty():
            self.stopOnFlag()
            jobid, surl, status = self.resubRemmgr.resultQueue.get()

            select = f"arcjobid={jobid}"

            if jobid in manualIDs or status == JobStatus.OK:
                # clean ARC job and finish resubmission for manually
                # resubmitted jobs or jobs whose files were successfully
                # removed
                if jobid in manualIDs and status != JobStatus.OK:
                    self.log.info(f"Failed deleting outputs for manually resubmitted arcjob {jobid}, will clean arcjob and finish resubmit")
                else:
                    self.log.info(f"Successfully deleted outputs for arcjob {jobid}, resubmit finished")
                self.cleanDownloadedJob(jobid)
                self.dbarc.updateArcJob(jobid, arcdesc)
                desc = {"actpandastatus": "starting", "arcjobid": None}
                self.dbpanda.updateJobs(select, desc)

            elif status == JobStatus.FAILED:
                # If we couldn't clean outputs the next try of the job will
                # also fail. Better to return to panda for an increased attempt
                # no.
                self.log.info(f"Failed deleting outputs for arcjob {jobid}, setting to failed")
                self.cleanDownloadedJob(jobid)
                self.dbarc.updateArcJob(jobid, arcdesc)
                desc = {"actpandastatus": "failed", "pandastatus": "failed"}
                self.dbpanda.updateJobs(select, desc)
                self.cleanDownloadedJob(jobid)

            else:
                # set back to toresubmit to retry
                self.log.info(f"Failed deleting outputs for arcjob {jobid}, will retry")
                select = f"arcjobid={job['arcjobid']}"
                desc = {"actpandastatus": "toresubmit"}
                self.dbpanda.updateJobs(select, desc)

    def downloadHeartbeats(self, jobs):
        # send jobs to heartbeat downloader and start it if not active
        if jobs:
            self.heartbeatDownloader.taskQueue.put(jobs)
            if not self.heartbeatDownloader.thread.is_alive():
                self.log.debug("HeartbeatDownloader not alive, starting")
                self.heartbeatDownloader.terminate.clear()
                self.heartbeatDownloader.start()

        # process results from remover
        downloaded = []
        while not self.heartbeatDownloader.resultQueue.empty():
            self.stopOnFlag()
            job = self.heartbeatDownloader.resultQueue.get()
            downloaded.append(job)
        return downloaded

    def process(self):
        self.logger.arclog.setReopen(True)
        self.logger.arclog.setReopen(False)
        self.setSites()
        self.validateFinishedJobs()
        self.cleanFailedJobs()
        self.resubmitNonARCJobs()
        self.cleanResubmittingJobs()

        # Validator suffers from memory leaks in arc bindings, so exit once per day
        if time.time() - self.starttime > 60*60*24:
            self.log.info("Exiting for periodic restart")
            self.stopWithException()


class JobStatus(Enum):
    OK = 1
    RETRY = 2
    FAILED = 3


# there are 2 use cases for waiting:
# - ARCWorker: wait on exit event or task queue
# - SEWorkManager: wait on exit event, task queue or worker results queue
#
# exit events are always set by managing thread
# taskQueue is always filled by managing thread
# worker results are filled by multiple workers
#
# Selector class with cooperating data structures
#
# cooperating data structures implement a uniform API that selector requires and register with the selector
# the thing is, once the selector is supposed to notify, it should be determined if it should be reset.
# In case of event data structure, it should be reset. In case of queue with multiple values or semaphore
# with multiple acquires, it should be kept asserted.


class SEvent(threading.Event):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.select = None

    def register(self, select):
        self.select = select

    def asserted(self):
        return self.is_set()

    def set(self):
        super().set()
        if not self.select:
            raise Exception("No selector registered")
        self.select.notify()


class SQueue(queue.Queue):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.select = None

    def register(self, select):
        self.select = select

    def asserted(self):
        return self.qsize() > 0

    def put(self, *args, **kwargs):
        super().put(*args, **kwargs)
        if not self.select:
            raise Exception("No selector registered")
        self.select.notify()

    def put_nowait(self, *args, **kwargs):
        super().put_nowait(*args, **kwargs)
        if not self.select:
            raise Exception("No selector registered")
        self.select.notify()


class Selector:

    def __init__(self):
        self.event = threading.Event()
        self.lock = threading.Lock()
        self.resources = []

    def register(self, resource):
        resource.register(self)
        self.resources.append(resource)

    def notify(self):
        with self.lock:
            self.event.set()

    def update(self):
        with self.lock:
            shouldAssert = False
            for resource in self.resources:
                if resource.asserted():
                    shouldAssert = True
                    break
            if not shouldAssert:
                self.event.clear()

    def wait(self, timeout=None):
        self.update()
        return self.event.wait(timeout=timeout)


class ThreadWorker:

    # TODO: There should be some form of null logger. For now, we rely on
    #       aCT always providing the logger.
    def __init__(self, log, terminate=None, taskQueue=None, resultQueue=None):
        self.log = log
        self.terminate = terminate if terminate else threading.Event()
        self.taskQueue = taskQueue if taskQueue else queue.Queue()
        self.resultQueue = resultQueue if resultQueue else queue.Queue()
        self.thread = threading.Thread(target=self)

    def start(self):
        if not self.thread.is_alive():
            self.thread = threading.Thread(target=self)
            self.thread.start()

    def __call__(self, *args, **kwargs):
        try:
            self.run(*args, **kwargs)
        except:
            import traceback
            self.log.error(traceback.format_exc())
        finally:
            self.finish()

    def run(*args, **kwargs):
        pass

    def finish(self):
        self.log.debug(f"{self.__class__.__name__}: exited")


class ARCWorker(ThreadWorker):

    def __init__(self, log, credential, timeout=60, terminate=None, taskQueue=None, **kwargs):
        # terminate and task queue have to be "selectable" by default
        super().__init__(
            log,
            terminate=terminate if terminate else SEvent(),
            taskQueue=taskQueue if taskQueue else SQueue(),
            **kwargs
        )
        self.credential = credential
        self.timeout = timeout
        self.selector = Selector()
        self.selector.register(self.terminate)
        self.selector.register(self.taskQueue)

    def run(self, *args, **kwargs):
        while not self.terminate.is_set():
            self.log.debug(f"{self.__class__.__name__}: Termination flag not set, waiting on selector")
            if not self.selector.wait(timeout=self.timeout):
                self.log.debug(f"{self.__class__.__name__}: Selector timeout ({self.timeout}), exiting")
                self.terminate.set()
                continue
            try:
                task = self.taskQueue.get_nowait()
            except queue.Empty:
                self.log.debug(f"{self.__class__.__name__}: ARCWorker empty queue??? What activated the selector???")
            else:
                self.processTask(task)
        self.log.debug(f"{self.__class__.__name__}: Termination flag set, exiting")

    def processTask(self, task):
        pass


class OutputChecker(ARCWorker):

    def processTask(self, surls):
        datapointlist = arc.DataPointList()
        surllist = []
        dummylist = []
        # TODO: HARDCODED
        BULK_LIMIT = 100
        count = 0
        for surl in surls:
            self.log.debug(f"{self.__class__.__name__}: count: {count}, surl: {surl}")
            if self.terminate.is_set():
                return
            dp = aCTUtils.DataPoint(str(surl['surl']), self.credential)
            if not dp or not dp.h or surl['surl'].startswith('davs://srmdav.ific.uv.es:8443'):
                self.log.warning(f"URL {surl['surl']} not supported, skipping validation")
                self.resultQueue.put((surl["arcjobid"], surl["surl"], JobStatus.OK))
                continue
            count += 1
            datapointlist.append(dp.h)
            dummylist.append(dp)  # to not destroy objects
            surllist.append(surl)

            if count % BULK_LIMIT != 0 and count != len(surls):
                continue

            # do bulk call
            files, status = dp.h.Stat(datapointlist)
            if not status and status.GetErrno() != errno.EOPNOTSUPP:
                # If call fails it is generally a server or connection problem
                # and in most cases should be retryable
                host = dp.h.GetURL().Host()
                if status.Retryable():
                    self.log.warning(f"Failed to query files on {host}, will retry later: {status}")
                    for s in surllist:
                        self.resultQueue.put((s["arcjobid"], s["surl"], JobStatus.RETRY))
                else:
                    self.log.error(f"Failed to query files on {host}: {status}")
                    for s in surllist:
                        self.resultQueue.put((s["arcjobid"], s["surl"], JobStatus.FAILED))

            else:
                # files is a list of FileInfo objects. If file is not found or has
                # another error in the listing FileInfo object will be invalid
                for i in range(len(datapointlist)):
                    if self.terminate.is_set():
                        return
                    s = surllist[i]
                    jobid, url = s["arcjobid"], s["surl"]
                    dpurl = datapointlist[i].GetURL().str()
                    if status.GetErrno() == errno.EOPNOTSUPP:
                        # Bulk stat was not supported, do non-bulk here
                        f = arc.FileInfo()
                        st = datapointlist[i].Stat(f)
                        if not st or not f:
                            s = surllist[i]
                            if status.Retryable():
                                self.log.warning(f"Failed to query files on {datapointlist[i].GetURL().Host()}, will retry later: {st}")
                                self.resultQueue.put((jobid, url, JobStatus.RETRY))
                            else:
                                self.log.warning(f"{s['arcjobid']}: Failed to find info on {dpurl}")
                                self.resultQueue.put((jobid, url, JobStatus.FAILED))
                            files.append(None)
                        else:
                            files.append(f)

                    if not files[i]:
                        self.log.warning(f"{jobid}: Failed to find info on {dpurl}")
                        self.resultQueue.put((jobid, url, JobStatus.FAILED))
                    else:
                        fsize, checksum = s["fsize"], s["checksum"]
                        statsize = int(files[i].GetSize())
                        statsum = files[i].GetCheckSum()
                        # compare metadata
                        try:
                            self.log.debug(
                                f"File {dpurl} for {jobid}: expected size {fsize}, checksum {checksum}, "
                                f"actual size {statsize}, checksum {statsum}"
                            )
                        except:
                            self.log.warning(f"Unhandled issue {i}")
                            self.resultQueue.put((jobid, url, JobStatus.FAILED))
                            continue
                        if fsize != statsize:
                            self.log.warning(f"File {dpurl} for {jobid}: size on storage ({statsize}) differs from expected size ({fsize})")
                            self.resultQueue.put((jobid, url, JobStatus.FAILED))
                            continue
                        if not files[i].CheckCheckSum():
                            self.log.warning(f"File {dpurl} for {jobid}: no checksum information available")
                        elif checksum != statsum:
                            self.log.warning(f"File {dpurl} for {jobid}: checksum on storage ({statsum}) differs from expected checksum ({checksum})")
                            self.resultQueue.put((jobid, url, JobStatus.FAILED))
                            continue

                        self.log.info(f"File {dpurl} validated for {jobid}")
                        self.resultQueue.put((jobid, url, JobStatus.OK))

            # Clear lists and go to next round
            datapointlist = arc.DataPointList()
            surllist = []
            dummylist = []


class FileRemover(ARCWorker):

    def processTask(self, surls):
        for surl in surls:
            self.log.debug(f"FILE REMOVER: surl: {surl}")
            if self.terminate.is_set():
                return
            jobid, url = surl["arcjobid"], surl["surl"]
            dp = aCTUtils.DataPoint(str(url), self.credential)
            if not dp.h or url.startswith('root://'):
                self.log.info(f"Removed {url} for {jobid}")
                self.resultQueue.put((jobid, url, JobStatus.OK))
                continue
            status = dp.h.Remove()
            if not status:
                if status.Retryable() and not url.startswith('davs://srmdav.ific.uv.es:8443'):
                    self.log.warning(f"Failed to delete {url} for {jobid}, will retry later: {status}")
                    self.resultQueue.put((jobid, url, JobStatus.RETRY))
                elif status.GetErrno() == errno.ENOENT:
                    self.log.info(f"File {url} for {jobid} does not exist")
                    self.resultQueue.put((jobid, url, JobStatus.OK))
                else:
                    self.log.error(f"Failed to delete {url} for {jobid}: {status}")
                    self.resultQueue.put((jobid, url, JobStatus.FAILED))
            else:
                self.log.info(f"Removed {url} for {jobid}")
                self.resultQueue.put((jobid, url, JobStatus.OK))


class HeartbeatDownloader(ARCWorker):

    def __init__(self, tmpdir, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.tmpdir = tmpdir

    def processTask(self, jobs):
        for job in jobs:
            self.log.debug(f"{self.__class__.__name__}: job: {job}")
            if self.terminate.is_set():
                return
            if 'JobID' not in job or not job['JobID']:
                self.resultQueue.put(job)
                continue

            jobid = job['JobID']
            sessionid = jobid[jobid.rfind('/'):]
            localdir = self.tmpdir + sessionid

            os.makedirs(localdir, 0o755, exist_ok=True)

            source = aCTUtils.DataPoint(f"{jobid}/heartbeat.json", self.credential)
            dest = aCTUtils.DataPoint(f"{localdir}/heartbeat.json", self.credential)
            dm = arc.DataMover()
            status = dm.Transfer(source.h, dest.h, arc.FileCache(), arc.URLMap())
            if not status:
                self.log.debug(f"{job['pandaid']}: Failed to download {source.h.GetURL.str()}: {status}")
                self.resultQueue.put(job)


class SEWorkManager(ThreadWorker):

    def __init__(self, log, credential, terminate=None, taskQueue=None, workerResults=None, **kwargs):
        super().__init__(
            log,
            terminate=terminate if terminate else SEvent(),
            taskQueue=taskQueue if taskQueue else SQueue(),
            **kwargs
        )
        self.credential = credential
        self.jobSurls = {}
        self.jobStatus = {}
        self.workers = {}
        self.workerResults = workerResults if workerResults else SQueue()
        self.selector = Selector()
        self.selector.register(self.terminate)
        self.selector.register(self.taskQueue)
        self.selector.register(self.workerResults)

    def run(self, *args, **kwargs):
        while not self.terminate.is_set():
            self.log.debug(f"{self.__class__.__name__}: Termination flag not set, waiting on selector")
            if not self.selector.wait():
                self.log.debug(f"{self.__class__.__name__}: Selector timeout ({self.timeout}), exiting")
                self.terminate.set()
                continue
            self.log.debug(f"{self.__class__.__name__}: Selector activated on taskQueue or workerResults queue")
            try:
                task = self.taskQueue.get_nowait()
            except queue.Empty:
                pass
            else:
                self.log.debug(f"{self.__class__.__name__}: Got task {task}")
                self.processTask(task)
            self.processWorkerResults()

        self.log.debug(f"{self.__class__.__name__}: Termination flag set, exiting")

    def processTask(self, task):
        pass

    def processWorkerResults(self):
        # process result queue
        while not self.workerResults.empty():
            if self.terminate.is_set():
                self.log.debug(f"{self.__class__.__name__}: Exiting early on terminate flag")
                return
            jobid, surl, status = self.workerResults.get()
            self.log.debug(f"{self.__class__.__name__} RESULT: {jobid} {surl} {status}")
            self.jobSurls[jobid].discard(surl)
            # OK should not overwrite existing status
            if status == JobStatus.OK:
                if jobid not in self.jobStatus:
                    self.jobStatus[jobid] = status
            # RETRY overwrites OK
            elif status == JobStatus.RETRY:
                if self.jobStatus.get(jobid, None) in (None, JobStatus.OK):
                    self.jobStatus[jobid] = status
            # FAILED overwrites both OK and RETRY
            elif status == JobStatus.FAILED:
                self.jobStatus[jobid] = status

        # return status for jobs whose surls are finished validating and remove
        # jobs from dicts
        removeIDs = []
        for jobid, surls in self.jobSurls.items():
            if len(surls) == 0:
                removeIDs.append(jobid)
                self.resultQueue.put((jobid, self.jobStatus[jobid]))
        for jobid in removeIDs:
            del self.jobSurls[jobid]
            del self.jobStatus[jobid]

    def finish(self):
        """Set terminate event for workers and wait for them to finish."""
        for se, worker in self.workers.items():
            self.log.debug(f"{self.__class__.__name__}: Terminating worker for SE {se}")
            worker.terminate.set()
        for worker in self.workers.values():
            worker.thread.join()
            self.log.debug(f"{self.__class__.__name__}: Worker for SE {se} terminated")
        super().finish()


class OutputCheckManager(SEWorkManager):

    def processTask(self, surlDict):
        self.log.debug(f"{self.__class__.__name__}: surlDict: {surlDict}")
        for se, jobchecks in surlDict.items():
            if self.terminate.is_set():
                self.log.debug(f"{self.__class__.__name__}:  Exiting early on terminate flag")
                return
            if se not in self.workers:
                self.workers[se] = OutputChecker(self.log, self.credential, resultQueue=self.workerResults, terminate=self.terminate)
            if not self.workers[se].thread.is_alive():
                self.log.debug(f"OutputChecker for SE {se} not alive, starting")
                self.workers[se].terminate.clear()
                self.workers[se].start()
            tocheck = []
            for jobcheck in jobchecks:
                jobid, surl = jobcheck["arcjobid"], jobcheck["surl"]
                surls = self.jobSurls.setdefault(jobid, set())
                if not surl:
                    self.log.error(f"Missing surl for {surl['arcjobid']}, cannot validate")
                    self.jobStatus[jobid] = JobStatus.FAILED
                elif jobid not in self.jobStatus:
                    surls.add(surl)
                    tocheck.append(jobcheck)
            self.workers[se].taskQueue.put(tocheck)


class FileRemoveManager(SEWorkManager):

    def processTask(self, surlDict):
        self.log.debug(f"{self.__class__.__name__}: surlDict: {surlDict}")
        for se, removeDicts in surlDict.items():
            if self.terminate.is_set():
                self.log.debug(f"{self.__class__.__name__}: Exiting early on terminate flag")
                return
            if se not in self.workers:
                self.workers[se] = FileRemover(self.log, self.credential, resultQueue=self.workerResults, terminate=self.terminate)
            if not self.workers[se].thread.is_alive():
                self.log.debug(f"FileRemover for SE {se} not alive, starting")
                self.workers[se].terminate.clear()
                self.workers[se].start()
            self.workers[se].taskQueue.put(removeDicts)
