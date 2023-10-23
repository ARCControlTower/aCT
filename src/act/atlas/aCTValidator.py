"""Module implements output file validation and cleaning for ATLAS jobs."""


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
from urllib.parse import urlparse

import arc
from act.atlas.aCTATLASProcess import aCTATLASProcess
from act.atlas.aCTPandaJob import aCTPandaJob
from act.common import aCTUtils
from act.common.aCTLogger import aCTLogger
from act.common.aCTProxy import aCTProxy


class aCTValidator(aCTATLASProcess):
    """
    Process for validating or cleaning output files of ATLAS jobs.

    The output files of an ATLAS job are specified in its heartbeat.json output
    file along with metadata like size, checksum and modification timestamp.

    The process handles 3 types of ATLAS jobs:
    - finished jobs: the output files of finished jobs should be validated
      before they are considered successful using the metadata
    - failed jobs: it is required that the output files should be removed for
      some of the jobs that failed and cannot be resubmitted or recovered
      otherwise
    - jobs to resubmit: before ATLAS jobs are resubmitted their output files
      and their ARC jobs should be cleaned

    The output files can reside on different storage sites. One storage site
    being slow or down should not block or slow down handling of jobs that use
    other storage sites. Because of that, the interaction with storage is done
    in a separate thread per storage site and operation. For each storage site
    there is one thread worker for validating the output files, one worker for
    cleaning the output files of failed jobs and one worker for cleaning the
    output files of jobs to be resubmitted. The main thread aggregates results
    from workers to make final updates to jobs. Another worker is used to
    download heartbeat.json files of certain jobs that weren't fetched yet.

    Because of asynchronous nature of thread worker processing, some
    intermediate states are used in the ATLAS job state machine, namely
    "validating", "cleaning" and "resubmitting".

    Stopping the validation or cleaning operation is not a problem as it can be
    repeated later. Therefore, there is no permanent state kept to resume the
    operation. It has to be restarted. Even if the process cannot exit cleanly
    (SIGKILL, kernel panic) the jobs left in intermediate state can be reset
    back to retry the operation when the validator process starts.
    """

    def setup(self):
        """Set up the validator."""
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

        # aCT's proxy cert used for authentication on SEs
        self.uc = arc.UserConfig(cred_type)
        self.uc.ProxyPath(str(proxyfile))
        self.uc.UtilsDirPath(str(arc.UserConfig.ARCUSERDIRECTORY))

        # result queues for worker threads
        self.checkResults = queue.Queue()
        self.cleanRemoves = queue.Queue()
        self.resubRemoves = queue.Queue()

        # dictionaries of (jobid: URL set) to track the processing of URLs
        self.checkSurls = {}
        self.cleanSurls = {}
        self.resubSurls = {}

        # dictionaries of (jobid: status) to aggregate results into final job
        # status
        self.checkStatus = {}
        self.cleanStatus = {}
        self.resubStatus = {}

        # dictionaries of (SE URL: worker object) to manage worker threads
        self.checkers = {}
        self.cleaners = {}
        self.resubers = {}

        # worker object for heartbeat downloads
        self.heartbeatDownloader = HeartbeatDownloader(self.tmpdir, self.log, self.uc)

        self.recoverIntermediateStates()

    def setupLogger(self, name, cluster=None):
        """Set up the logger objects with ARC binding logging."""
        if cluster:
            url = urlparse(cluster)
            logname = f'{name}-{url.hostname}'
        else:
            logname = name
        self.logger = aCTLogger(logname, cluster=cluster, arclog=True)
        self.criticallogger = aCTLogger('aCTCritical', cluster=cluster, arclog=False)
        self.log = self.logger()
        self.criticallog = self.criticallogger()

    def finish(self):
        """Properly handle all threads and job states."""
        # signal threads to terminate
        self.log.debug(f"Terminating {len(self.checkers)} OutputCheckers")
        for worker in self.checkers.values():
            worker.taskQueue.put(ExitMsg())
        self.log.debug(f"Terminating {len(self.cleaners)} FileRemovers")
        for worker in self.cleaners.values():
            worker.taskQueue.put(ExitMsg())
        self.log.debug(f"Terminating {len(self.resubers)} FileRemovers")
        for worker in self.resubers.values():
            worker.taskQueue.put(ExitMsg())
        self.log.debug("Terminating HeartbeatDownloader")
        self.heartbeatDownloader.taskQueue.put(ExitMsg())

        # wait threads to finish
        for worker in self.checkers.values():
            worker.thread.join()
        for worker in self.cleaners.values():
            worker.thread.join()
        for worker in self.resubers.values():
            worker.thread.join()
        if self.heartbeatDownloader.thread.is_alive():
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
            self.log.error(f"appjob({aj['appjobid']}): No ARC ID for arcjob({arcjobid}): {aj}")
            return False
        aj = aj[0]
        jobid = aj['JobID']
        sessionid = jobid[jobid.rfind('/')+1:]
        date = aj['created'].strftime('%Y-%m-%d')
        if extractmetadata:
            try:
                jobinfo = aCTPandaJob(filename=os.path.join(self.tmpdir, sessionid, 'heartbeat.json'))
            except Exception as x:
                self.log.error(f"appjob({aj['appjobid']}): failed to load heartbeat file: {x}")
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
                        self.log.warning(f'appjob({aj["appjobid"]}): Adjusting reported walltime {aj["UsedTotalWallTime"]} to CPU time {cputimepercore}')
                        jobinfo.startTime = (aj['EndTime'] - datetime.timedelta(0, cputimepercore)).strftime('%Y-%m-%d %H:%M:%S')
                else:
                    self.log.warning(f'appjob({aj["appjobid"]}): no endtime found')
            if len(aj["ExecutionNode"]) > 255:
                jobinfo.node = aj["ExecutionNode"][:254]
                self.log.warning(f"appjob({aj['appjobid']}): Truncating wn hostname from {aj['ExecutionNode']} to {jobinfo.node}")
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
                    self.log.warning(f"appjob({aj['appjobid']}): no metaData in pilot metadata: {e}")
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
            self.log.error(f"failed to find arcjob({arcjobid}) in database")
            return {}

        jobid=aj['JobID']
        sessionid=jobid[jobid.rfind('/')+1:]
        try:
            jobinfo = aCTPandaJob(filename=os.path.join(self.tmpdir, sessionid, 'heartbeat.json'))
            metadata = getattr(jobinfo, 'xml') # travis doesn't like jobinfo.xml
        except Exception as x:
            self.log.error(f"appjob({aj['appjobid']}): failed to extract metadata for arcid({jobid}): {x}")
            return {}

        try:
            outputfiles = json.loads(metadata)
        except Exception as e:
            self.log.error(f"appjob({aj['appjobid']}): failed to load output file info for arcid({jobid}): {e}")
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
                self.log.error(f"appjob({aj['appjobid']}): {x}")
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
        Validate finished jobs.

        Finished jobs that are to be validated have pandastatus "transferring"
        and actpandastatus "tovalidate". Validation means checking output
        files specified in job's heartbeat.json output file. Each specified
        output file is queried for its size and checksum and compared to the
        values in heartbeat.json. If everything matches, the job is marked
        finished, otherwise it is failed.
        """
        # TODO: HARDCODED limit
        # get all jobs with pandastatus running and actpandastatus tovalidate
        select = f"(pandastatus='transferring' and actpandastatus='tovalidate') and siteName in {self.sitesselect} limit 1000"
        columns = ["arcjobid", "pandaid", "siteName", "metadata"]
        jobstoupdate = self.dbpanda.getJobs(select, columns=columns)

        cleandesc = {"arcstate": "toclean", "tarcstate": self.dbarc.getTimeStamp()}

        # skip validation for the true pilot jobs, just copy logs, set to done
        # and clean arc job
        toremove = []
        for job in jobstoupdate:
            self.stopOnFlag()
            if self.sites[job['siteName']]['truepilot']:
                self.log.info(f"appjob({job['pandaid']}): Pilot job, skip validation")
                if not self.copyFinishedFiles(job["arcjobid"], False):
                    self.log.warning(f"appjob({job['pandaid']}): Failed to copy log files")
                self.cleanDownloadedJob(job['arcjobid'])
                self.dbarc.updateArcJob(job['arcjobid'], cleandesc)
                select = f"arcjobid={job['arcjobid']}"
                desc = {"pandastatus": None, "actpandastatus": "done"}
                self.dbpanda.updateJobs(select, desc)
            else:
                toremove.append(job)

        # pull out output file info from pilot heartbeat json into dict, order
        # by SE
        surls = {}
        for job in toremove:
            self.stopOnFlag()
            jobsurls = self.extractOutputFilesFromMetadata(job["arcjobid"])
            if not jobsurls:
                # problem extracting files, fail job, clean ARC job
                # and job files
                self.log.error(f"appjob({job['pandaid']}): Cannot validate output, setting to failed")
                self.cleanDownloadedJob(job['arcjobid'])
                self.dbarc.updateArcJob(job['arcjobid'], cleandesc)
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
                self.log.info(f"Successful output file check for arcjob({jobid})")
                desc = {"pandastatus": "finished", "actpandastatus": "finished"}
                self.dbpanda.updateJobs(f"arcjobid={jobid}", desc)
                if not self.copyFinishedFiles(jobid, True):
                    # id was gone already, skip cleaning
                    continue
                self.cleanDownloadedJob(jobid)
                self.dbarc.updateArcJob(jobid, cleandesc)
            elif status == JobStatus.FAILED:
                # output file failed, set toresubmit to clean up output and resubmit
                self.log.error(f"Failed output file check for arcjob({jobid}), resubmitting")
                desc = {"pandastatus": "starting", "actpandastatus": "toresubmit"}
                self.dbpanda.updateJobs(f"arcjobid={jobid}", desc)
            else:
                # Retry next time
                self.log.warning(f"Failed output file check for arcjob({jobid}), will retry")
                desc = {"actpandastatus": "tovalidate"}
                self.dbpanda.updateJobs(f"arcjobid={jobid}", desc)

    def checkOutputFiles(self, surls):
        """
        Send URLs to workers and return final status for jobs.

        There is one worker for each SE. URLs are grouped by SE and sent to the
        corresponding worker. The worker results are processed to the final
        status for the corresponding job. The return value is a list of tuples
        of job ID and its final status.
        """
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
                    # job fails immediately if one of the URLs is missing
                    self.log.error(f"Missing surl for arcjob({jobid}), cannot validate")
                    self.checkStatus[jobid] = JobStatus.FAILED
                elif jobid not in self.checkStatus:
                    jobSurls.add(surl)
                    tocheck.append(check)
            self.checkers[se].taskQueue.put(ValueMsg(tocheck))

        return self.processJobsResults(self.checkResults, self.checkStatus, self.checkSurls)

    def processJobsResults(self, resultsQueue, statusDict, surlDict):
        """
        Process worker results and return final status for jobs.

        The result of every operation and also final job status is classified
        in 3 ways which are defined in JobStatus enum class: OK, RETRY and
        FAILED. Operations on job's output URLs have to be aggregated into the
        final job status. All OK results aggregate into final status OK. Any
        RETRY result overrides OK status to RETRY and any FAILED overrides
        both OK and RETRY to FAILED.

        Final status is tracked per job in statusDict and the list of URLs that
        have to be processed per job are handled in surlDict. Once all the URLs
        are processed for a job, its final status is returned in a tuple along
        with its ID.
        """
        while not resultsQueue.empty():
            self.stopOnFlag()
            jobid, surl, status = resultsQueue.get()
            self.log.debug(f"RESULT: arcjob({jobid}) {surl} {status}")

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

        # gather all jobs that are done and remove them from status and surl
        # dicts
        finished = []
        for jobid, surls in surlDict.items():
            if len(surls) == 0:
                finished.append((jobid, statusDict[jobid]))
        for jobid, _ in finished:
            del surlDict[jobid]
            del statusDict[jobid]

        return finished

    def removeOutputFiles(self, surls, removerDict, removeResults, removeStatus, removeSurls):
        """
        See checkOutputFiles.

        The only difference is in the handling of empty URLs which is done by
        checkOutputFiles.
        """
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
        """
        Delete output files of failed jobs.

        Output files of some of the failed jobs should be cleaned first before
        processing them further. Such jobs have pandastatus "transferring" and
        actpandastatus "toclean". The job's output files that have to be
        cleaned are specified in its heartbeat.json file (see
        validateFinishedJobs). Regardless of the success or failure of the
        cleaning operations the jobs are marked as failed.
        """
        # TODO: HARDCODED limit
        # get all jobs with pandastatus transferring and actpandastatus toclean
        select = f"(pandastatus='transferring' and actpandastatus='toclean') and siteName in {self.sitesselect} limit 1000"
        columns = ["arcjobid", "pandaid", "siteName"]
        jobstoupdate = self.dbpanda.getJobs(select, columns=columns)

        cleandesc = {"arcstate": "toclean", "tarcstate": self.dbarc.getTimeStamp()}
        faildesc = {"actpandastatus": "failed", "pandastatus": "failed"}

        # for truepilot jobs, don't try to clean outputs (too dangerous), just
        # clean arc job
        toremove = []
        for job in jobstoupdate:
            self.stopOnFlag()
            if self.sites[job['siteName']]['truepilot']:
                self.log.info(f"appjob({job['pandaid']}): Pilot job, skip cleanup of output files")
                self.cleanDownloadedJob(job["arcjobid"])
                self.dbarc.updateArcJob(job["arcjobid"], cleandesc)
                select = f"arcjobid={job['arcjobid']}"
                self.dbpanda.updateJobs(select, faildesc)
            else:
                toremove.append(job)

        # pull out output file info from pilot heartbeat json into dict, order
        # by SE
        surls = {}
        for job in toremove:
            self.stopOnFlag()
            jobid = job["arcjobid"]
            jobsurls = self.extractOutputFilesFromMetadata(jobid)
            if not jobsurls:
                # problem extracting files, fail job, clean ARC job and files
                self.log.warning(f"appjob({job['pandaid']}): Cannot remove output, skipping")
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
                    self.log.info(f"Successfuly removed output files for failed arcjob({jobid})")
                else:
                    # If unretriably failed, there is not much we can do except
                    # continue
                    self.log.warning(f"Output file removal failed for arcjob({jobid}), skipping")
                self.cleanDownloadedJob(jobid)
                self.dbarc.updateArcJob(jobid, cleandesc)
                self.dbpanda.updateJobs(f"arcjobid={jobid}", faildesc)
            else:
                # Retry next time
                self.log.warning(f"Output file removal failed for arcjob({jobid}), will retry")
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
            self.log.info(f"appjob({job['pandaid']}): resubmitting")
            select = f"id={job['id']}"
            desc = {"actpandastatus": "starting", "arcjobid": None}
            self.dbpanda.updateJobs(select, desc)

    def cleanResubmittingJobs(self):
        """
        Clean output files of resubmitting jobs.

        Output files and ARC jobs of jobs that are to be resubmitted should be
        cleaned. Such jobs have actpandastatus "toresubmit". The output files
        to be cleaned are specified in job's heartbeat.json (see
        validateFinishedJobs). If the cleaning is successful, the job is marked
        to be resubmitted. Otherwise, the jobs is marked failed.
        """
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

        # Queue heartbeat downloads for manually resubmitted jobs and add jobs
        # whose downloads are finished to remove list.
        downloaded = self.downloadHeartbeats(todownload)
        manualIDs = set([job["arcjobid"] for job in downloaded])
        toremove.extend(downloaded)

        cleandesc = {'arcstate': 'toclean', 'tarcstate': self.dbarc.getTimeStamp()}
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
                    self.log.info(f"appjob({job['pandaid']}): Cannot remove output, resubmit finished")
                    self.cleanDownloadedJob(jobid)
                    self.dbarc.updateArcJob(jobid, cleandesc)
                    self.dbpanda.updateJobs(f"arcjobid={jobid}", resubdesc)
                else:
                    # Otherwise fail job whose outputs cannot be cleaned.
                    self.log.error(f"appjob({job['pandaid']}): Cannot remove output, skipping")
                    self.cleanDownloadedJob(jobid)
                    self.dbarc.updateArcJob(jobid, cleandesc)
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
                    self.log.warning(f"Failed deleting outputs for manually resubmitted arcjob({jobid}), will clean arcjob and finish resubmit")
                else:
                    self.log.info(f"Successfully deleted outputs for arcjob({jobid}), resubmit finished")
                self.cleanDownloadedJob(jobid)
                self.dbarc.updateArcJob(jobid, cleandesc)
                self.dbpanda.updateJobs(f"arcjobid={jobid}", resubdesc)

            elif status == JobStatus.FAILED:
                # If we couldn't clean outputs the next try of the job will
                # also fail. Better to return to panda for an increased attempt
                # no.
                self.log.error(f"Failed deleting outputs for arcjob({jobid}), setting to failed")
                self.cleanDownloadedJob(jobid)
                self.dbarc.updateArcJob(jobid, cleandesc)
                self.dbpanda.updateJobs(f"arcjobid={jobid}", faildesc)
                self.cleanDownloadedJob(jobid)

            else:
                # set back to toresubmit to retry
                self.log.warning(f"Failed deleting outputs for arcjob({jobid}), will retry")
                desc = {"actpandastatus": "toresubmit"}
                self.dbpanda.updateJobs(f"arcjobid={jobid}", desc)

    def downloadHeartbeats(self, jobs):
        """
        Download job heartbeats.

        A list of jobs (dicts) is sent to the worker thread. The sent jobs are
        returned once the worker thread is finished downloading their
        heartbeat.json.
        """
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
        """Process eligible jobs."""
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
    """Enumeration of possible job status values."""

    OK = 1
    RETRY = 2
    FAILED = 3


class BaseMsg:
    """
    Base class for the type of message between threads.

    Queues are used to communicate with the worker threads. A different type of
    message is required to instruct the thread to exit or potentially perform
    a different action. Because the exit needs to happen ASAP, the priority
    queue is used and the exit message has the highest priority. This requires
    the class hierarchy to implement magic methods for comparison.
    """

    def __lt__(self, value):
        """Return False for the lowest priority by default."""
        return False


class ExitMsg(BaseMsg):
    """
    Message type used for instructing termination.

    This should be the highest priority message type to allow for as quick
    exit as possible.
    """

    def __lt__(self, value):
        """Return True for the highest possible priority."""
        return True


class ValueMsg(BaseMsg):
    """
    Message type for any value.

    Inherits the lowest priority from the base class.
    """

    def __init__(self, value):
        """Store the value to be handled by the recipient."""
        self.value = value


class ThreadWorker:
    """
    Base implementation of a thread worker.

    The ThreadWorker object is essentially a callable object run by
    threading.Thread (similar to what aCTProcess is to multiprocessing.Process)
    with attributes useful for the work done by the thread.

    The thread attribute is the reference to the thread that runs the worker.
    The worker object can create and be run by multiple threads during its
    lifetime. That is why the start method is provided.

    Each worker has a task queue where the tasks for the worker are sent.
    Worker puts results into the result queue.

    There are multiple reasons for the distinction between worker and its
    thread. Initially, the inheritance of threading.Thread was considered.
    The mixing of Thread API and whatever custom API is required for aCT
    thread workers is considered less clean and more difficult to understand
    than creating a custom callable.

    Another reason for the distinction is that the life cycle of the thread
    does not match the conceptual lifecycle of the worker. Threads on most OSes
    cannot be restarted or stopped temporarily, which means that the new thread
    needs to be created. However, the workers in aCT can stop when there is no
    work and then start later or restart on crash. There is also an additional
    context of queues and events that are associated with the worker and
    independent from the lifecycle of the OS thread. It appears therefore that
    having a thread as an attribute of the worker is more elegant.
    """

    # TODO: There should be some form of null logger. For now, we rely on
    #       aCT always providing the logger.
    def __init__(self, log, taskQueue=None, resultQueue=None):
        """Initialize the worker object."""
        self.log = log
        self.taskQueue = taskQueue if taskQueue else queue.PriorityQueue()
        self.resultQueue = resultQueue if resultQueue else queue.Queue()
        self.thread = threading.Thread(target=self)

    def start(self):
        """
        Start a new thread if not running already.

        On the first start of a ThreadWorker object the Thread instance is
        wasted because it was never started so it is not alive and a new one is
        created. It is simpler to waste the instance than to keep additional
        state to handle first start.
        """
        if not self.thread.is_alive():
            self.thread = threading.Thread(target=self)
            self.thread.start()

    def __call__(self, *args, **kwargs):
        """
        Call the function that the thread should execute.

        Threads should not silently crash so every exception is caught and
        logged. Also, the finish method should always be run regardless of the
        way the thread execution went.
        """
        try:
            self.run(*args, **kwargs)
        except:
            import traceback
            self.log.error(traceback.format_exc())
        finally:
            self.finish()

    def run(*args, **kwargs):
        """Do nothing by default on thread execution."""
        pass

    def finish(self):
        """Debug log the name of the worker in the end."""
        self.log.debug(f"{self.__class__.__name__}: exited")


class ARCWorker(ThreadWorker):
    """
    Common implementation of main loop for all validator thread workers.

    Event is used to exit the loop. It can potentially be supplied and used
    to terminate the thread from another thread but this should not be done
    because it cannot wake up the thread from sleeping on the taskQueue.

    The timeout attribute is used for exiting after not getting any task.
    Credential is used for ARC bindings to authenticate on the SEs.
    """

    def __init__(self, log, credential, timeout=60, terminate=None, **kwargs):
        """Initialize the validator thread worker."""
        super().__init__(log, **kwargs)
        self.terminate = terminate if terminate else threading.Event()
        self.credential = credential
        self.timeout = timeout

    def start(self):
        """Start the thread if not running."""
        self.terminate.clear()
        super().start()

    def run(self, *args, **kwargs):
        """Process messages until the termination event."""
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
        """Process the message."""
        if isinstance(task, ExitMsg):
            self.log.debug(f"{self.__class__.__name__}: ExitMsg, exiting")
            self.terminate.set()

        elif isinstance(task, ValueMsg):
            self.processValue(task.value)

    def processValue(self, value):
        """Process the regular workload message."""
        pass


class OutputChecker(ARCWorker):
    """Thread worker that validates the output files."""

    def processValue(self, surls):
        """Process a list of URLs and return results in result queue."""
        datapointlist = arc.DataPointList()
        surllist = []
        dummylist = []
        # TODO: HARDCODED
        BULK_LIMIT = 100
        count = 0
        for surl in surls:
            if self.terminate.is_set():
                return
            jobid, url = surl["arcjobid"], surl["surl"]
            self.log.debug(f"{self.__class__.__name__}: count: {count}, surl: {surl}")
            dp = aCTUtils.DataPoint(str(url), self.credential)
            if not dp or not dp.h or url.startswith('davs://srmdav.ific.uv.es:8443'):
                self.log.warning(f"URL {url} not supported, skipping validation")
                self.resultQueue.put((jobid, url, JobStatus.OK))
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
                                self.log.warning(f"arcjob({jobid}): Failed to query files on {datapointlist[i].GetURL().Host()}, will retry later: {st}")
                                self.resultQueue.put((jobid, url, JobStatus.RETRY))
                            else:
                                self.log.error(f"arcjob({jobid}): Failed to find info on {dpurl}")
                                self.resultQueue.put((jobid, url, JobStatus.FAILED))
                            files.append(None)
                        else:
                            files.append(f)

                    if not files[i]:
                        self.log.error(f"arcjob({jobid}): Failed to find info on {dpurl}")
                        self.resultQueue.put((jobid, url, JobStatus.FAILED))
                    else:
                        fsize, checksum = s["fsize"], s["checksum"]
                        statsize = int(files[i].GetSize())
                        statsum = files[i].GetCheckSum()
                        # compare metadata
                        try:
                            self.log.debug(
                                f"arcjob({jobid}): File {dpurl}: expected size {fsize}, checksum {checksum}, "
                                f"actual size {statsize}, checksum {statsum}"
                            )
                        except BaseException as exc:
                            # catching BaseException is better than bare except
                            # and allows to print exception instead of using
                            # traceback or similar
                            self.log.warning(f"arcjob({jobid}): Unhandled issue: {exc}")
                            self.resultQueue.put((jobid, url, JobStatus.FAILED))
                            continue
                        if fsize != statsize:
                            self.log.error(f"arcjob({jobid}): File {dpurl}: size on storage ({statsize}) differs from expected size ({fsize})")
                            self.resultQueue.put((jobid, url, JobStatus.FAILED))
                            continue
                        if not files[i].CheckCheckSum():
                            self.log.warning(f"arcjob({jobid}): File {dpurl}: no checksum information available")
                        elif checksum != statsum:
                            self.log.error(f"arcjob({jobid}): File {dpurl}: checksum on storage ({statsum}) differs from expected checksum ({checksum})")
                            self.resultQueue.put((jobid, url, JobStatus.FAILED))
                            continue

                        self.log.info(f"arcjob({jobid}): File {dpurl}: validated")
                        self.resultQueue.put((jobid, url, JobStatus.OK))

            # Clear lists and go to next round
            datapointlist = arc.DataPointList()
            surllist = []
            dummylist = []


class FileRemover(ARCWorker):
    """Thread worker that removes output files."""

    def processValue(self, surls):
        """Remove a list of URLs and return results in the result queue."""
        for surl in surls:
            if self.terminate.is_set():
                return
            self.log.debug(f"{self.__class__.__name__}: surl: {surl}")
            jobid, url = surl["arcjobid"], surl["surl"]
            dp = aCTUtils.DataPoint(str(url), self.credential)
            if not dp.h or url.startswith('root://'):
                self.log.info(f"arcjob({jobid}): Removed {url}")
                self.resultQueue.put((jobid, url, JobStatus.OK))
                continue
            status = dp.h.Remove()
            if not status:
                if status.Retryable() and not url.startswith('davs://srmdav.ific.uv.es:8443'):
                    self.log.warning(f"arcjob({jobid}): Failed to delete {url}, will retry later: {status}")
                    self.resultQueue.put((jobid, url, JobStatus.RETRY))
                elif status.GetErrno() == errno.ENOENT:
                    self.log.warning(f"arcjob({jobid}): File {url} does not exist")
                    self.resultQueue.put((jobid, url, JobStatus.OK))
                else:
                    self.log.error(f"arcjob({jobid}): Failed to delete {url}: {status}")
                    self.resultQueue.put((jobid, url, JobStatus.FAILED))
            else:
                self.log.info(f"arcjob({jobid}): Removed {url}")
                self.resultQueue.put((jobid, url, JobStatus.OK))


class HeartbeatDownloader(ARCWorker):
    """Thread worker that downloads heartbeat.json files of jobs."""

    def __init__(self, tmpdir, *args, **kwargs):
        """
        Initialize the downloader.

        tmpdir is a location where files should be downloaded.
        """
        super().__init__(*args, **kwargs)
        self.tmpdir = tmpdir

    def processValue(self, jobs):
        """Download heartbeat.json output files for given jobs."""
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
                self.log.error(f"appjob({job['pandaid']}): Failed to download {source.h.GetURL.str()}: {status}")
                self.resultQueue.put(job)
