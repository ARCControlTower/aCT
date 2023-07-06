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
        self.log.info(f'set proxy path to {proxyfile}')

        self.uc = arc.UserConfig(cred_type)
        self.uc.ProxyPath(str(proxyfile))
        self.uc.UtilsDirPath(str(arc.UserConfig.ARCUSERDIRECTORY))

        self.checkResults = queue.Queue()
        self.cleanRemoves = queue.Queue()
        self.resubRemoves = queue.Queue()

        self.checkSurls = {}
        self.cleanSurls = {}
        self.resubSurls = {}

        self.checkStatus = {}
        self.cleanStatus = {}
        self.resubStatus = {}

        self.checkers = {}
        self.cleaners = {}
        self.resubers = {}

        self.heartbeatDownloader = HeartbeatDownloader(self.tmpdir, self.log, self.uc)

        self.recoverIntermediateStates()

    def finish(self):
        """Properly handle all threads and job states."""
        # signal threads to terminate
        self.log.debug(f"Terminating {len(self.checkers)} OutputCheckers")
        for worker in self.checkers:
            worker.taskQueue.put(ExitMsg())
        self.log.debug(f"Terminating {len(self.cleaners)} FileRemovers")
        for worker in self.cleaners:
            worker.taskQueue.put(ExitMsg())
        self.log.debug(f"Terminating {len(self.resubers)} FileRemovers")
        for worker in self.resubers:
            worker.taskQueue.put(ExitMsg())
        self.log.debug("Terminating HeartbeatDownloader")
        self.heartbeatDownloader.taskQueue.put(ExitMsg())

        # wait threads to finish
        for worker in self.checkers:
            worker.thread.join()
        for worker in self.cleaners:
            worker.thread.join()
        for worker in self.resubers:
            worker.thread.join()
        self.heartbeatDownloader.thread.join()

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
        select = f"arcjobs.id={arcjobid} AND arcjobs.id=pandajobs.arcjobid"
        aj = self.dbarc.getArcJobsInfo(select, columns=columns, tables='arcjobs,pandajobs')
        if not aj or 'JobID' not in aj[0] or not aj[0]['JobID']:
            self.log.error(f'No JobID in arcjob {arcjobid}: {aj}')
            return False
        aj = aj[0]
        jobid = aj['JobID']
        sessionid = jobid[jobid.rfind('/')+1:]
        date = aj['created'].strftime('%Y-%m-%d')
        if extractmetadata:
            try:
                jobinfo = aCTPandaJob(filename=os.path.join(self.tmpdir, sessionid, 'heartbeat.json'))
            except Exception as x:
                self.log.error(f"{aj['appjobid']}: failed to load heartbeat file for arcjob {jobid}: {x}")
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
                        self.log.warning(f'{aj["appjobid"]}: Adjusting reported walltime {aj["UsedTotalWallTime"]} to CPU time {cputimepercore}')
                        jobinfo.startTime = (aj['EndTime'] - datetime.timedelta(0, cputimepercore)).strftime('%Y-%m-%d %H:%M:%S')
                else:
                    self.log.warning(f'{aj["appjobid"]}: no endtime found')
            if len(aj["ExecutionNode"]) > 255:
                jobinfo.node = aj["ExecutionNode"][:254]
                self.log.warning(f"{aj['appjobid']}: Truncating wn hostname from {aj['ExecutionNode']} to {jobinfo.node}")
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
                    self.log.warning(f"{aj['appjobid']}: no metaData in pilot metadata: {e}")
                jobinfo.writeToFile(os.path.join(smeta['harvesteraccesspoint'], 'jobReport.json'))
            else:
                jobinfo.writeToFile(os.path.join(self.tmpdir, "heartbeats", f"{aj['appjobid']}.json"))

        # copy to joblog dir files downloaded for the job: gmlog errors and pilot log
        outd = os.path.join(self.conf.joblog.dir, date, aj['fairshare'])
        os.makedirs(outd, 0o755, exist_ok=True)

        localdir = os.path.join(self.tmpdir, sessionid)
        gmlogerrors = os.path.join(localdir, "gmlog", "errors")
        arcjoblog = os.path.join(outd, f"{aj['appjobid']}.log")
        if not os.path.exists(arcjoblog):
            try:
                shutil.move(gmlogerrors, arcjoblog)
                os.chmod(arcjoblog, 0o644)
            except:
                self.log.error(f"Failed to copy {gmlogerrors}")

        pilotlog = aj['stdout']
        if not pilotlog and os.path.exists(localdir):
            pilotlogs = [f for f in os.listdir(localdir)]
            for f in pilotlogs:
                if f.find('.log'):
                    pilotlog = f
        if pilotlog:
            try:
                shutil.move(os.path.join(localdir, pilotlog),
                            os.path.join(outd, f'{aj["appjobid"]}.out'))
                os.chmod(os.path.join(outd, f'{aj["appjobid"]}.out'), 0o644)
            except Exception as e:
                self.log.error(f"Failed to copy file {os.path.join(localdir,pilotlog)}: {e}")
                return False

        return True

    def extractOutputFilesFromMetadata(self, arcjobid):
        aj = self.dbarc.getArcJobInfo(arcjobid, columns=["JobID", "appjobid"])
        if not aj or 'JobID' not in aj or not aj['JobID']:
            self.log.error(f"failed to find arcjobid {arcjobid} in database")
            return {}

        jobid=aj['JobID']
        sessionid=jobid[jobid.rfind('/')+1:]
        try:
            jobinfo = aCTPandaJob(filename=os.path.join(self.tmpdir, sessionid, 'heartbeat.json'))
            metadata = getattr(jobinfo, 'xml') # travis doesn't like jobinfo.xml
        except Exception as x:
            self.log.error(f"{aj['appjobid']}: failed to extract metadata for arcjob {sessionid}: {x}")
            return {}

        try:
            outputfiles = json.loads(metadata)
        except Exception as e:
            self.log.error(f"{aj['appjobid']}: failed to load output file info for arcjob {sessionid}: {e}")
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
                self.log.error(f'{aj["appjobid"]}: {x}')
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
        select = f"(pandastatus='transferring' and actpandastatus='tovalidate') and siteName in {self.sitesselect} limit 1000"
        columns = ["arcjobid", "pandaid", "siteName", "metadata"]
        jobstoupdate = self.dbpanda.getJobs(select, columns=columns)

        arcdesc = {"arcstate": "toclean", "tarcstate": self.dbarc.getTimeStamp()}

        # Skip validation for the true pilot jobs, just copy logs, set to done and clean arc job
        toremove = []
        for job in jobstoupdate:
            self.stopOnFlag()
            if self.sites[job['siteName']]['truepilot']:
                self.log.info(f"{job['pandaid']}: Pilot job, skip validation")
                if not self.copyFinishedFiles(job["arcjobid"], False):
                    self.log.warning(f"{job['pandaid']}: Failed to copy log files")
                self.cleanDownloadedJob(job['arcjobid'])
                self.dbarc.updateArcJob(job['arcjobid'], arcdesc)
                select = f"arcjobid={job['arcjobid']}"
                desc = {"pandastatus": None, "actpandastatus": "done"}
                self.dbpanda.updateJobs(select, desc)
            else:
                toremove.append(job)

        # pull out output file info from pilot heartbeat json into dict, order by SE
        surls = {}
        for job in toremove:
            self.stopOnFlag()
            jobsurls = self.extractOutputFilesFromMetadata(job["arcjobid"])
            if not jobsurls:
                # Problem extracting files, fail job, clean ARC job
                # and job files.
                self.log.error(f"{job['pandaid']}: Cannot validate output of arcjob {job['arcjobid']}, setting to failed")
                self.cleanDownloadedJob(job['arcjobid'])
                self.dbarc.updateArcJob(job['arcjobid'], arcdesc)
                select = f"arcjobid={job['arcjobid']}"
                desc = {"actpandastatus": "failed", "pandastatus": "failed"}
                self.dbpanda.updateJobs(select, desc)
            else:
                select = f"arcjobid={job['arcjobid']}"
                desc = {"actpandastatus": "validating"}
                self.dbpanda.updateJobs(select, desc)
                for se in jobsurls:
                    surls.setdefault(se, []).extend(jobsurls[se])

        # send surls to output validator threads and process their results
        checkResults = self.checkOutputFiles(surls)

        # process jobs based on their final status after output validation
        for jobid, status in checkResults:
            self.stopOnFlag()
            if status == JobStatus.OK:
                self.log.info(f"Successful output file check for arcjob {jobid}")
                desc = {"pandastatus": "finished", "actpandastatus": "finished"}
                self.dbpanda.updateJobs(f"arcjobid={jobid}", desc)
                if not self.copyFinishedFiles(jobid, True):
                    # id was gone already
                    continue
                self.cleanDownloadedJob(jobid)
                self.dbarc.updateArcJob(jobid, arcdesc)
            elif status == JobStatus.FAILED:
                # output file failed, set toresubmit to clean up output and resubmit
                self.log.info(f"Failed output file check for arcjob {jobid}, resubmitting")
                desc = {"pandastatus": "starting", "actpandastatus": "toresubmit"}
                self.dbpanda.updateJobs(f"arcjobid={jobid}", desc)
            else:
                # Retry next time
                self.log.info(f"Failed output file check for arcjob {jobid}, will retry")
                desc = {"actpandastatus": "tovalidate"}
                self.dbpanda.updateJobs(f"arcjobid={jobid}", desc)

    def checkOutputFiles(self, surls):
        for se, checks in surls.items():
            self.stopOnFlag()

            # create thread worker if it doesn't exist
            if se not in self.checkers:
                self.checkers[se] = OutputChecker(self.log, self.uc, resultQueue=self.checkResults)

            # start thread worker if it is not running
            if not self.checkers[se].thread.is_alive():
                self.log.debug(f"OutputChecker for SE {se} not alive, starting")
                self.checkers[se].start()

            # process and submit surls to the thread workers
            tocheck = []
            for check in checks:
                jobid, surl = check["arcjobid"], check["surl"]
                jobSurls = self.checkSurls.setdefault(jobid, set())
                if not surl:
                    self.log.error(f"Missing surl for {surl['arcjobid']}, cannot validate")
                    self.checkStatus[jobid] = JobStatus.FAILED
                elif jobid not in self.checkStatus:
                    jobSurls.add(surl)
                    tocheck.append(check)
            self.checkers[se].taskQueue.put(ValueMsg(tocheck))

        return self.processJobsResults(self.checkResults, self.checkStatus, self.checkSurls)

    def processJobsResults(self, resultsDict, statusDict, surlDict):
        while not resultsDict.empty():
            self.stopOnFlag()
            jobid, surl, status = resultsDict.get()
            self.log.debug(f"RESULT: {jobid} {surl} {status}")

            # remove from a set of job surls that are worked on
            surlDict[jobid].discard(surl)

            # OK should not overwrite existing status
            if status == JobStatus.OK:
                if jobid not in statusDict:
                    statusDict[jobid] = status
            # RETRY overwrites OK
            elif status == JobStatus.RETRY:
                if statusDict.get(jobid, None) in (None, JobStatus.OK):
                    statusDict[jobid] = status
            # FAILED overwrites both OK and RETRY
            elif status == JobStatus.FAILED:
                statusDict[jobid] = status

        # remove job from status and surl dicts
        finished = []
        for jobid, surls in surlDict.items():
            if len(surls) == 0:
                finished.append((jobid, statusDict[jobid]))
        for jobid, _ in finished:
            del surlDict[jobid]
            del statusDict[jobid]

        return finished

    def removeOutputFiles(self, surls, removerDict, removeResults, removeStatus, removeSurls):
        for se, removeDicts in surls.items():
            self.stopOnFlag()
            if se not in removerDict:
                removerDict[se] = FileRemover(self.log, self.uc, resultQueue=removeResults)
            if not removerDict[se].thread.is_alive():
                self.log.debug(f"FileRemover for SE {se} not alive, starting")
                removerDict[se].start()
            for d in removeDicts:
                jobSurls = removeSurls.setdefault(d["arcjobid"], set())
                jobSurls.add(d["surl"])
            removerDict[se].taskQueue.put(ValueMsg(removeDicts))

        return self.processJobsResults(removeResults, removeStatus, removeSurls)

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
        faildesc = {"actpandastatus": "failed", "pandastatus": "failed"}

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
                self.dbpanda.updateJobs(select, faildesc)
            else:
                toremove.append(job)

        # pull out output file info from pilot heartbeat json into dict, order by SE
        surls = {}
        for job in toremove:
            self.stopOnFlag()
            jobid = job["arcjobid"]
            jobsurls = self.extractOutputFilesFromMetadata(jobid)
            if not jobsurls:
                # Problem extracting files, fail job, clean ARC job and files.
                self.log.error(f"{job['pandaid']}: Cannot remove output of arcjob {jobid}, skipping")
                self.cleanDownloadedJob(jobid)
                self.dbarc.updateArcJob(jobid, cleandesc)
                self.dbpanda.updateJobs(f"arcjobid={jobid}", faildesc)
            else:
                desc = {"actpandastatus": "cleaning"}
                self.dbpanda.updateJobs(f"arcjobid={jobid}", desc)
                for se in jobsurls:
                    surls.setdefault(se, []).extend(jobsurls[se])

        # send surls to remover threads and process their results
        removeResults = self.removeOutputFiles(surls, self.cleaners, self.cleanRemoves, self.cleanStatus, self.cleanSurls)

        # process jobs based on their final status after output removal
        for jobid, status in removeResults:
            self.stopOnFlag()
            if status in (JobStatus.OK, JobStatus.FAILED):
                if status == JobStatus.OK:
                    self.log.info(f"Successfuly removed output files for failed arcjob {jobid}")
                else:
                    # If unretriably failed, there is not much we can do except
                    # continue
                    self.log.info(f"Output file removal failed for arcjob {jobid}, skipping")
                self.cleanDownloadedJob(jobid)
                self.dbarc.updateArcJob(jobid, cleandesc)
                self.dbpanda.updateJobs(f"arcjobid={jobid}", faildesc)
            else:
                # Retry next time
                self.log.info(f"Output file removal failed for arcjob {jobid}, will retry")
                desc = {"actpandastatus": "toclean"}
                self.dbpanda.updateJobs(f"arcjobid={jobid}", desc)

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
        faildesc = {"actpandastatus": "failed", "pandastatus": "failed"}
        resubdesc = {"actpandastatus": "starting", "arcjobid": None}

        # pull out output file info from pilot heartbeat json into dict, order by SE
        surls = {}
        for job in toremove:
            self.stopOnFlag()
            jobsurls = self.extractOutputFilesFromMetadata(job["arcjobid"])
            if not jobsurls:
                jobid = job["arcjobid"]
                if job in downloaded or job["restartstate"] != "Finishing" and job["arcstate"] != "done":
                    # Clean job files and ARC job, finish resubmission if
                    # resubmitted manually or job failed before finishing,
                    # since there are likely no output files.
                    self.log.error(f"{job['pandaid']}: Cannot remove output of arcjob {jobid}, resubmit finished")
                    self.cleanDownloadedJob(jobid)
                    self.dbarc.updateArcJob(jobid, arcdesc)
                    self.dbpanda.updateJobs(f"arcjobid={jobid}", resubdesc)
                else:
                    # Otherwise fail job whose outputs cannot be cleaned.
                    self.log.error(f"{job['pandaid']}: Cannot remove output of arcjob {jobid}, skipping")
                    self.cleanDownloadedJob(jobid)
                    self.dbarc.updateArcJob(jobid, arcdesc)
                    self.dbpanda.updateJobs(f"arcjobid={jobid}", faildesc)
            else:
                for se in jobsurls:
                    surls.setdefault(se, []).extend(jobsurls[se])

        # send surls to remover threads and process their results
        removeResults = self.removeOutputFiles(surls, self.resubers, self.resubRemoves, self.resubStatus, self.resubSurls)

        # process results from remover
        for jobid, status in removeResults:
            self.stopOnFlag()
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
                self.dbpanda.updateJobs(f"arcjobid={jobid}", resubdesc)

            elif status == JobStatus.FAILED:
                # If we couldn't clean outputs the next try of the job will
                # also fail. Better to return to panda for an increased attempt
                # no.
                self.log.info(f"Failed deleting outputs for arcjob {jobid}, setting to failed")
                self.cleanDownloadedJob(jobid)
                self.dbarc.updateArcJob(jobid, arcdesc)
                desc = {"actpandastatus": "failed", "pandastatus": "failed"}
                self.dbpanda.updateJobs(f"arcjobid={jobid}", faildesc)
                self.cleanDownloadedJob(jobid)

            else:
                # set back to toresubmit to retry
                self.log.info(f"Failed deleting outputs for arcjob {jobid}, will retry")
                desc = {"actpandastatus": "toresubmit"}
                self.dbpanda.updateJobs(f"arcjobid={jobid}", desc)

    def downloadHeartbeats(self, jobs):
        # send jobs to heartbeat downloader and start it if not active
        if jobs:
            self.heartbeatDownloader.taskQueue.put(ValueMsg(jobs))
            if not self.heartbeatDownloader.thread.is_alive():
                self.log.debug("HeartbeatDownloader not alive, starting")
                self.heartbeatDownloader.start()

        # process results from remover
        finished = []
        while not self.heartbeatDownloader.resultQueue.empty():
            self.stopOnFlag()
            job = self.heartbeatDownloader.resultQueue.get()
            finished.append(job)
        return finished

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


class BaseMsg:

    def __lt__(self, value):
        return False


class ExitMsg(BaseMsg):

    def __lt__(self, value):
        return True


class ValueMsg(BaseMsg):

    def __init__(self, value):
        self.value = value


class ThreadWorker:

    # TODO: There should be some form of null logger. For now, we rely on
    #       aCT always providing the logger.
    def __init__(self, log, terminate=None, taskQueue=None, resultQueue=None):
        self.log = log
        self.terminate = terminate if terminate else threading.Event()
        self.taskQueue = taskQueue if taskQueue else queue.PriorityQueue()
        self.resultQueue = resultQueue if resultQueue else queue.Queue()
        self.thread = threading.Thread(target=self)

    def start(self):
        if not self.thread.is_alive():
            self.thread = threading.Thread(target=self)
            self.terminate.clear()
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

    def __init__(self, log, credential, timeout=60, **kwargs):
        super().__init__(log, **kwargs)
        self.credential = credential
        self.timeout = timeout

    def run(self, *args, **kwargs):
        while not self.terminate.is_set():
            self.log.debug(f"{self.__class__.__name__}: waiting on queue")
            try:
                task = self.taskQueue.get(timeout=self.timeout)
            except queue.Empty:
                self.terminate.set()
                self.log.debug(f"{self.__class__.__name__}: queue timeout, exiting")
            else:
                self.processTask(task)

    def processTask(self, task):
        if isinstance(task, ExitMsg):
            self.log.debug(f"{self.__class__.__name__}: ExitMsg, exiting")
            self.terminate.set()

        elif isinstance(task, ValueMsg):
            self.processValue(task.value)

    def processValue(self, value):
        pass


class OutputChecker(ARCWorker):

    def processValue(self, surls):
        datapointlist = arc.DataPointList()
        surllist = []
        dummylist = []
        # TODO: HARDCODED
        BULK_LIMIT = 100
        count = 0
        for surl in surls:
            if self.terminate.is_set():
                return
            self.log.debug(f"{self.__class__.__name__}: count: {count}, surl: {surl}")
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

    def processValue(self, surls):
        for surl in surls:
            if self.terminate.is_set():
                return
            self.log.debug(f"{self.__class__.__name__}: surl: {surl}")
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

    def processValue(self, jobs):
        for job in jobs:
            if self.terminate.is_set():
                return
            self.log.debug(f"{self.__class__.__name__}: job: {job}")
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
