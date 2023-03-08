from act.atlas.aCTATLASProcess import aCTATLASProcess
from act.common.aCTProxy import aCTProxy
from act.common import aCTUtils
from act.atlas.aCTPandaJob import aCTPandaJob
import datetime
import errno
import os
import shutil
import time
import arc
import json
import re

class aCTValidator(aCTATLASProcess):
    '''
    Validate output files for finished jobs, cleanup output files for failed jobs.
    '''

    def __init__(self):
        aCTATLASProcess.__init__(self, ceflavour=['ARC-CE'])

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

        # Possible file status
        self.ok = 0
        self.retry = 1
        self.failed = 2


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
        try:
            os.makedirs(outd, 0o755)
        except:
            pass

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

    def checkOutputFiles(self, surldict):
        '''
        Check if SURLs are working. Returns a dict of arcjobid:file status
        Do bulk arc.DataPoint.Stat() with max 100 files per request. The list
        of surls passed here all belong to the same SE.
        '''

        if self.arcconf.downtime.srmdown:
            self.log.info("SRM down, will validate later")
            return dict((k['arcjobid'], self.retry) for k in surldict.values())

        result = {}
        datapointlist = arc.DataPointList()
        surllist = []
        dummylist = []
        bulklimit = 100
        for surls in surldict.values():
            count = 0
            for surl in surls:
                count += 1
                if not surl['surl']:
                    self.log.error("Missing surl for %s, cannot validate" % surl['arcjobid'])
                    result[surl['arcjobid']] = self.failed
                    continue
                dp = aCTUtils.DataPoint(str(surl['surl']), self.uc)
                if not dp or not dp.h or surl['surl'].startswith('davs://srmdav.ific.uv.es:8443'):
                    self.log.warning("URL %s not supported, skipping validation" % str(surl['surl']))
                    result[surl['arcjobid']] = self.ok
                    continue
                datapointlist.append(dp.h)
                dummylist.append(dp) # to not destroy objects
                surllist.append(surl)

                if count % bulklimit != 0 and count != len(surls):
                    continue

                # do bulk call
                (files, status) = dp.h.Stat(datapointlist)
                if not status and status.GetErrno() != errno.EOPNOTSUPP:
                    # If call fails it is generally a server or connection problem
                    # and in most cases should be retryable
                    if status.Retryable():
                        self.log.warning("Failed to query files on %s, will retry later: %s" % (dp.h.GetURL().Host(), str(status)))
                        result.update(dict((k['arcjobid'], self.retry) for k in surllist))
                    else:
                        self.log.error("Failed to query files on %s: %s" % (dp.h.GetURL().Host(), str(status)))
                        result.update(dict((k['arcjobid'], self.failed) for k in surllist))

                else:
                    # files is a list of FileInfo objects. If file is not found or has
                    # another error in the listing FileInfo object will be invalid
                    for i in range(len(datapointlist)):
                        if status.GetErrno() == errno.EOPNOTSUPP:
                            # Bulk stat was not supported, do non-bulk here
                            f = arc.FileInfo()
                            st = datapointlist[i].Stat(f)
                            if not st or not f:
                                if status.Retryable():
                                    self.log.warning("Failed to query files on %s, will retry later: %s" % (datapointlist[i].GetURL().Host(), str(st)))
                                    result[surllist[i]['arcjobid']] = self.retry
                                else:
                                    self.log.warning("%s: Failed to find info on %s" % (surllist[i]['arcjobid'], datapointlist[i].GetURL().str()))
                                    result[surllist[i]['arcjobid']] = self.failed
                                files.append(None)
                            else:
                                files.append(f)

                        if not files[i]:
                            self.log.warning("%s: Failed to find info on %s" % (surllist[i]['arcjobid'], datapointlist[i].GetURL().str()))
                            result[surllist[i]['arcjobid']] = self.failed
                        else:
                            # compare metadata
                            try:
                                self.log.debug("File %s for %s: expected size %d, checksum %s, actual size %d, checksum %s" %
                                               (datapointlist[i].GetURL().str(), surllist[i]['arcjobid'], int(surllist[i]['fsize']),
                                               surllist[i]['checksum'], int(files[i].GetSize()), files[i].GetCheckSum()))
                            except:
                                self.log.warning("Unhandled issue %d",i)
                                result[surllist[i]['arcjobid']] = self.failed
                                continue
                            if int(surllist[i]['fsize']) != int(files[i].GetSize()):
                                self.log.warning("File %s for %s: size on storage (%d) differs from expected size (%d)" %
                                                 (datapointlist[i].GetURL().str(), surllist[i]['arcjobid'],
                                                  int(files[i].GetSize()), int(surllist[i]['fsize'])))
                                result[surllist[i]['arcjobid']] = self.failed
                                continue
                            if not files[i].CheckCheckSum():
                                self.log.warning("File %s for %s: no checksum information available" %
                                                 (datapointlist[i].GetURL().str(), surllist[i]['arcjobid']))
                            elif surllist[i]['checksum'] != files[i].GetCheckSum():
                                self.log.warning("File %s for %s: checksum on storage (%s) differs from expected checksum (%s)" %
                                                 (datapointlist[i].GetURL().str(), surllist[i]['arcjobid'],
                                                  files[i].GetCheckSum(), surllist[i]['checksum']))
                                result[surllist[i]['arcjobid']] = self.failed
                                continue

                            self.log.info("File %s validated for %s" % (datapointlist[i].GetURL().str(), surllist[i]['arcjobid']))
                            # don't overwrite previous failed file for this job
                            if surllist[i]['arcjobid'] not in result:
                                result[surllist[i]['arcjobid']] = self.ok

                # Clear lists and go to next round
                datapointlist = arc.DataPointList()
                surllist = []
                dummylist = []

        return result

    def removeOutputFiles(self, surls):
        '''
        Remove SURLs.
        '''
        result = {}

        # As yet there is no bulk remove in ARC
        for surl in surls:
            dp = aCTUtils.DataPoint(str(surl['surl']), self.uc)
            if not dp.h or surl['surl'].startswith('root://'):
                self.log.info("Removed %s for %s" % (surl['surl'], surl['arcjobid']))
                result[surl['arcjobid']] = self.ok
                continue
            status = dp.h.Remove()
            if not status:
                if status.Retryable() and not surl['surl'].startswith('davs://srmdav.ific.uv.es:8443'):
                    self.log.warning("Failed to delete %s for %s, will retry later: %s" %
                                     (surl['surl'], surl['arcjobid'], str(status)))
                    result[surl['arcjobid']] = self.retry
                elif status.GetErrno() == errno.ENOENT:
                    self.log.info("File %s for %s does not exist" % (surl['surl'], surl['arcjobid']))
                    result[surl['arcjobid']] = self.ok
                else:
                    self.log.error("Failed to delete %s for %s: %s" % (surl['surl'], surl['arcjobid'], str(status)))
                    result[surl['arcjobid']] = self.failed
            else:
                self.log.info("Removed %s for %s" % (surl['surl'], surl['arcjobid']))
                result[surl['arcjobid']] = self.ok

        return result


    def downloadSmallFiles(self, jobs):
        '''
        This method is for jobs which should be killed and resubmitted. An attempt
        is made to download heartbeat json but it is fine to fail as the job
        may still be running.
        '''

        for job in jobs:
            if not 'JobID' in job or not job['JobID']:
                continue
            jobid = job['JobID']
            sessionid = jobid[jobid.rfind('/'):]
            localdir = self.tmpdir + sessionid

            try:
                os.makedirs(localdir, 0o755)
            except:
                pass

            source = aCTUtils.DataPoint(str(jobid + '/heartbeat.json'), self.uc)
            dest = aCTUtils.DataPoint(str(localdir + '/heartbeat.json'), self.uc)
            dm = arc.DataMover()
            status = dm.Transfer(source.h, dest.h, arc.FileCache(), arc.URLMap())
            if not status:
                self.log.debug('%s: Failed to download %s: %s' % (job['pandaid'], source.h.GetURL().str(), str(status)))


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
        '''
        Check for jobs with actpandastatus tovalidate and pandastatus running
        Check if the output files in pilot heartbeat json are valid.
        If yes, move to actpandastatus to finished, if not, move pandastatus
        and actpandastatus to failed.
        '''

        # get all jobs with pandastatus running and actpandastatus tovalidate
        select = "(pandastatus='transferring' and actpandastatus='tovalidate') and siteName in %s limit 1000" % self.sitesselect
        columns = ["arcjobid", "pandaid", "siteName", "metadata"]
        jobstoupdate=self.dbpanda.getJobs(select, columns=columns)

        if len(jobstoupdate)==0:
            # nothing to do
            return

        # Skip validation for the true pilot jobs, just copy logs, set to done and clean arc job
        for job in jobstoupdate[:]:
            if self.sites[job['siteName']]['truepilot']:
                self.log.info('%s: Skip validation' % job['pandaid'])
                if not self.copyFinishedFiles(job["arcjobid"], False):
                    self.log.warning("%s: Failed to copy log files" % job['pandaid'])
                # set arcjobs state toclean
                desc = {"arcstate":"toclean", "tarcstate": self.dbarc.getTimeStamp()}
                self.dbarc.updateArcJob(job['arcjobid'], desc)
                # set pandajob done
                select = "arcjobid='"+str(job["arcjobid"])+"'"
                desc = {"pandastatus": None, "actpandastatus": "done"}
                self.dbpanda.updateJobs(select, desc)
                self.cleanDownloadedJob(job['arcjobid'])
                jobstoupdate.remove(job)

        # pull out output file info from pilot heartbeat json into dict, order by SE
        surls = {}
        for job in jobstoupdate:
            jobsurls = self.extractOutputFilesFromMetadata(job["arcjobid"])
            if not jobsurls:
                # Problem extracting files, fail the job
                self.log.error("%s: Cannot validate output of arc job %s" % (job['pandaid'], job["arcjobid"]))
                # set arcjobs state toclean
                desc = {"arcstate":"toclean", "tarcstate": self.dbarc.getTimeStamp()}
                self.dbarc.updateArcJob(job['arcjobid'], desc)
                # set pandajob failed
                select = "arcjobid='"+str(job["arcjobid"])+"'"
                desc = {"actpandastatus": "failed", "pandastatus": "failed"}
                self.dbpanda.updateJobs(select, desc)
                self.cleanDownloadedJob(job['arcjobid'])
            else:
                for se in jobsurls:
                    try:
                        surls[se].extend(jobsurls[se])
                    except:
                        surls[se] = jobsurls[se]
        if not surls:
            # nothing to validate
            return

        # check if surls valid, update pandastatus accordingly
        checkedsurls = self.checkOutputFiles(surls)
        for id, result in checkedsurls.items():
            if result == self.ok:
                select = "arcjobid='"+str(id)+"'"
                desc = {"pandastatus": "finished", "actpandastatus": "finished"}
                self.dbpanda.updateJobsLazy(select, desc)
                if not self.copyFinishedFiles(id, True):
                    # id was gone already
                    continue
                # set arcjobs state toclean
                desc = {"arcstate":"toclean", "tarcstate": self.dbarc.getTimeStamp()}
                self.dbarc.updateArcJobLazy(id, desc)
                self.cleanDownloadedJob(id)
            elif result == self.failed:
                select = "arcjobid='"+str(id)+"'"
                # output file failed, set to toresubmit to clean up output and resubmit
                desc = {"pandastatus": "starting", "actpandastatus": "toresubmit"}
                self.dbpanda.updateJobsLazy(select, desc)
            else:
                # Retry next time
                pass

        self.dbpanda.Commit()
        self.dbarc.Commit()


    def cleanFailedJobs(self):
        '''
        Check for jobs with actpandastatus toclean and pandastatus transferring.
        Delete the output files in pilot heartbeat json
        Move actpandastatus to failed.
        '''
        # get all jobs with pandastatus transferring and actpandastatus toclean
        select = "(pandastatus='transferring' and actpandastatus='toclean') and siteName in %s limit 1000" % self.sitesselect
        columns = ["arcjobid", "pandaid", "siteName"]
        jobstoupdate=self.dbpanda.getJobs(select, columns=columns)

        if len(jobstoupdate)==0:
            # nothing to do
            return

        cleandesc = {"arcstate":"toclean", "tarcstate": self.dbarc.getTimeStamp()}

        # For truepilot jobs, don't try to clean outputs (too dangerous), just clean arc job
        for job in jobstoupdate[:]:
            # Cleaning a bad storage can block the validator, so skip cleaning in all cases
            if True:
            #if self.sites[job['siteName']]['truepilot']:
                self.log.info("%s: Skip cleanup of output files" % job['pandaid'])
                # set arcjobs state toclean
                self.dbarc.updateArcJob(job["arcjobid"], cleandesc)
                # set pandajob failed
                select = "arcjobid='"+str(job["arcjobid"])+"'"
                desc = {"actpandastatus": "failed", "pandastatus": "failed"}
                self.dbpanda.updateJobs(select, desc)
                jobstoupdate.remove(job)
                self.cleanDownloadedJob(job["arcjobid"])

        # pull out output file info from pilot heartbeat json into dict, order by SE
        surls = {}
        for job in jobstoupdate:
            jobsurls = self.extractOutputFilesFromMetadata(job["arcjobid"])
            if not jobsurls:
                # Problem extracting files, just continue to failed
                self.log.error("%s: Cannot remove output of arc job %s" % (job['pandaid'], job["arcjobid"]))
                # set arcjobs state toclean
                self.dbarc.updateArcJob(job["arcjobid"], cleandesc)
                # set pandajob failed
                select = "arcjobid='"+str(job["arcjobid"])+"'"
                desc = {"actpandastatus": "failed", "pandastatus": "failed"}
                self.dbpanda.updateJobs(select, desc)
                self.cleanDownloadedJob(job["arcjobid"])
            else:
                surls.update(jobsurls)

        if not surls:
            # nothing to clean
            return

        for se in surls:
            removedsurls = self.removeOutputFiles(surls[se])
            for id, result in list(removedsurls.items()):
                # If failed, not much we can do except continue
                if result == self.ok or result == self.failed:
                    # set arcjobs state toclean
                    self.dbarc.updateArcJobLazy(id, cleandesc)
                    # set pandajob failed
                    select = "arcjobid='"+str(id)+"'"
                    desc = {"actpandastatus": "failed", "pandastatus": "failed"}
                    self.dbpanda.updateJobsLazy(select, desc)
                    self.cleanDownloadedJob(id)
                else:
                    # Retry next time
                    pass

        self.dbpanda.Commit()
        self.dbarc.Commit()


    def cleanResubmittingJobs(self):
        '''
        Check for jobs with actpandastatus toresubmit and pandastatus starting.
        Delete the output files in pilot heartbeat json
        Move actpandastatus to starting.
        '''

        # First check for resubmitting jobs with no arcjob id defined
        select = "(actpandastatus='toresubmit' and arcjobid=NULL) and siteName in %s limit 1000" % self.sitesselect
        columns = ["pandaid", "id"]

        jobstoupdate=self.dbpanda.getJobs(select, columns=columns)

        for job in jobstoupdate:
            self.log.info('%s: resubmitting' % job['pandaid'])
            select = "id="+str(job['id'])
            desc = {"actpandastatus": "starting", "arcjobid": None}
            self.dbpanda.updateJobs(select, desc)

        # Get all other jobs with pandastatus starting and actpandastatus toresubmit
        # 2 possibilities for these jobs:
        # - job failed and aCT decided to resubmit: clean output files
        # - job was manually set toresubmit by aCT admin: set arc job tocancel,
        #   attempt to get heartbeat info and clean but don't fail if not possible.
        #   In this case don't wait for cancellation to finish as A-REX may be
        #   broken. There is always the possibility of a race condition where
        #   output files are produced and uploaded between checking for them and
        #   cancelling the job.
        select = "actpandastatus='toresubmit' and arcjobs.id=pandajobs.arcjobid limit 100"
        columns = ["pandajobs.arcjobid", "pandajobs.pandaid", "arcjobs.JobID", "arcjobs.arcstate", "arcjobs.restartstate"]
        jobstoupdate=self.dbarc.getArcJobsInfo(select, columns=columns, tables='arcjobs, pandajobs')

        if len(jobstoupdate)==0:
            # nothing to do
            return

        killedbymanual = [j for j in jobstoupdate if j['arcstate'] != 'donefailed' and j['arcstate'] != 'done' and j['arcstate'] != 'lost' and j['arcstate'] != 'cancelled']

        # TODO: make data transfer separate from main validator thread
        self.downloadSmallFiles(killedbymanual)
        # Cancel the jobs manually set toresubmit (TODO: when the jobs eventually go
        # to cancelled the arc id will not be in pandajobs any more so they will
        # never be cleaned up)
        for job in killedbymanual:
            self.log.info('%s: manually asked to resubmit, cancelling arc job %s' %
                          (job['pandaid'], job['JobID']))
            desc = {'arcstate': 'tocancel', 'tarcstate': self.dbarc.getTimeStamp()}
            self.dbarc.updateArcJobLazy(job['arcjobid'], desc)
        self.dbarc.Commit()

        # pull out output file info from pilot heartbeat json into dict, order by SE
        surls = {}
        for job in jobstoupdate:
            jobsurls = self.extractOutputFilesFromMetadata(job["arcjobid"])
            if not jobsurls:
                if job in killedbymanual or (job['restartstate'] != 'Finishing' and job['arcstate'] != 'done'):
                    # If job failed before finishing there is probably no
                    # output files and so nothing to clean. Just let it be
                    # resubmitted and clean arc job
                    self.cleanDownloadedJob(job['arcjobid'])
                    select = "arcjobid="+str(job['arcjobid'])
                    desc = {"actpandastatus": "starting", "arcjobid": None}
                    self.dbpanda.updateJobs(select, desc)
                else:
                    # Can't clean outputs so mark as failed (see more detail below)
                    self.log.error("%s: Cannot remove output of arc job %s" % (job['pandaid'], job["arcjobid"]))
                    self.cleanDownloadedJob(job['arcjobid'])
                    select = "arcjobid='"+str(job["arcjobid"])+"'"
                    desc = {"actpandastatus": "toclean", "pandastatus": "transferring"}
                    self.dbpanda.updateJobs(select, desc)
            else:
                surls.update(jobsurls)

        if not surls:
            # nothing to clean
            return

        for se in surls:
            removedsurls = self.removeOutputFiles(surls[se])
            for id, result in removedsurls.items():
                # if manually killed the cleaning is allowed to fail
                if id in [j['arcjobid'] for j in killedbymanual]:
                    select = "arcjobid='"+str(id)+"'"
                    # Setting arcjobid to NULL lets Panda2Arc pick up the job for resubmission
                    desc = {"actpandastatus": "starting", "arcjobid": None}
                    self.dbpanda.updateJobsLazy(select, desc)
                    continue

                if result == self.ok:
                    self.cleanDownloadedJob(id)
                    select = "arcjobid='"+str(id)+"'"
                    # Setting arcjobid to NULL lets Panda2Arc pick up the job for resubmission
                    desc = {"actpandastatus": "starting", "arcjobid": None}
                    self.dbpanda.updateJobsLazy(select, desc)
                elif result == self.failed:
                    # If we couldn't clean outputs the next try of the job will
                    # also fail. Better to return to panda for an increased
                    # attempt no. Setting to toclean and pandastatus=transferring
                    # means it will be processed by cleanFailedJobs() so don't
                    # clean the arc job here
                    select = "arcjobid='"+str(id)+"'"
                    desc = {"actpandastatus": "toclean", "pandastatus": "transferring"}
                    self.dbpanda.updateJobsLazy(select, desc)
                else:
                    # Retry next time
                    pass

        self.dbpanda.Commit()
        self.dbarc.Commit()


    def process(self):
        self.logger.arclogfile.setReopen(True)
        self.logger.arclogfile.setReopen(False)
        self.setSites()
        self.validateFinishedJobs()
        self.cleanFailedJobs()
        self.cleanResubmittingJobs()

        # Validator suffers from memory leaks in arc bindings, so exit once per day
        if time.time() - self.starttime > 60*60*24:
            self.log.info("%s exited for periodic restart", self.name)
            self.finish()


if __name__ == '__main__':

    am=aCTValidator()
    am.run()
    am.finish()
