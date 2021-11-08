# aCTFetcher.py
#
# Fetches output from finished jobs
#

import time
import os
import errno
import arc
import shutil
import fnmatch, re
from threading import Thread

from act.common.aCTProcess import aCTProcess
from act.common import aCTUtils

class fetchSomeThr(Thread):
    """
    Helper function for threading
    """
    def __init__ (self, func, jobs, downloadfiles):
        Thread.__init__(self)
        self.func = func
        self.jobs = jobs
        self.downloadfiles = downloadfiles
        self.result = (None, None, None)
    def run(self):
        self.result = self.func(self.jobs, self.downloadfiles)

class aCTFetcher(aCTProcess):
    '''
    Downloads output data for finished ARC jobs.
    '''

    def fetchAll(self, jobs):

        # Get all outputs using Job Supervisor
        job_supervisor = arc.JobSupervisor(self.uc, list(jobs.values()))
        job_supervisor.Update()
        dirs = arc.StringList()
        job_supervisor.Retrieve(self.tmpdir, False, False, dirs)

        return (list(job_supervisor.GetIDsProcessed()), list(job_supervisor.GetIDsNotProcessed()))

    def listUrlRecursive(self, url, fname='', filelist=[]):
        dp = aCTUtils.DataPoint(url+'/'+fname, self.uc)
        files = dp.h.List(arc.DataPoint.INFO_TYPE_NAME | arc.DataPoint.INFO_TYPE_TYPE)
        if not files[1]:
            self.log.warning("Failed listing %s/%s" % (url, fname))
            return filelist
        for f in files[0]:
            if f.GetType()==f.file_type_file:
                filelist.append((fname+'/'+f.GetName()).strip('/'))
            elif f.GetType()==f.file_type_dir:
                filelist = self.listUrlRecursive(url, (fname+'/'+str(f.GetName())).strip('/'), filelist)
        return filelist


    def fetchSome(self, jobs, downloadfiles):

        # Get specified files for the jobs in downloadfiles
        # jobs: id: Job object
        # downloadfiles: id: list of files relative to session dir, with wildcards
        if not jobs or not downloadfiles:
            return ([], [], [])

        # construct datapoint object, initialising connection. Use the same
        # object until base URL changes. TODO group by base URL.
        datapoint = aCTUtils.DataPoint(next(iter(jobs.values())).JobID, self.uc)
        dp = datapoint.h
        dm = arc.DataMover()
        dm.retry(False)
        dm.passive(True)
        dm.secure(False)
        fetched = []
        notfetched = []
        notfetchedretry = []

        for (id, job) in jobs.items():
            if id not in downloadfiles:
                continue
            jobid = job.JobID

            # If connection URL is different reconnect
            if arc.URL(jobid).ConnectionURL() != dp:
                datapoint = aCTUtils.DataPoint(jobid, self.uc)
                dp = datapoint.h
            localdir = self.tmpdir + jobid[jobid.rfind('/'):] + '/'
            sessiondir = jobid
            # Check for REST
            if job.JobManagementInterfaceName == 'org.nordugrid.arcrest':
                sessiondir += '/session'

            files = downloadfiles[id].split(';')
            if re.search('[\*\[\]\?]', downloadfiles[id]):
                # found wildcard, need to get sessiondir list
                remotefiles = self.listUrlRecursive(sessiondir)
                expandedfiles = []
                for wcf in files:
                    if re.search('[\*\[\]\?]', wcf):
                        # only match wildcards in matching dirs
                        expandedfiles += [rf for rf in remotefiles if fnmatch.fnmatch(rf, wcf) and os.path.dirname(rf)==os.path.dirname(wcf)]
                    else:
                        expandedfiles.append(wcf)
                # remove duplicates from wildcard matching through set
                files = list(set(expandedfiles))

            for f in files:
                localfile = str(localdir + f)
                localfiledir = localfile[:localfile.rfind('/')]
                # create required local dirs
                try:
                    os.makedirs(localfiledir, 0o755)
                except OSError as e:
                    if e.errno != errno.EEXIST or not os.path.isdir(localfiledir):
                        self.log.warning('Failed to create directory %s: %s', localfiledir, os.strerror(e.errno))
                        notfetched.append(jobid)
                        break
                # Check for diagnostic files in REST
                if job.JobManagementInterfaceName == 'org.nordugrid.arcrest' and f.startswith('gmlog/'):
                    remotefile = arc.URL(str(jobid + '/diagnose/' + f[6:]))
                else:
                    remotefile = arc.URL(str(sessiondir + '/' + f))
                if not dp.SetURL(remotefile):
                    datapoint = aCTUtils.DataPoint(remotefile.str(), self.uc)
                    dp = datapoint.h
                localdp = aCTUtils.DataPoint(localfile, self.uc)
                # do the copy
                status = dm.Transfer(dp, localdp.h, arc.FileCache(), arc.URLMap())
                if not status and str(status).find('File unavailable') == -1: # tmp fix for globus error which is always retried
                    if status.Retryable():
                        self.log.warning('Failed to download but will retry %s: %s', dp.GetURL().str(), str(status))
                        notfetchedretry.append(jobid)
                    else:
                        self.log.error('Failed to download with permanent failure %s: %s', dp.GetURL().str(), str(status))
                        notfetched.append(jobid)
                    break
                self.log.info('Downloaded %s', dp.GetURL().str())
            if jobid not in notfetched and jobid not in notfetchedretry:
                fetched.append(jobid)
        return (fetched, notfetched, notfetchedretry)


    def fetchJobs(self, arcstate, nextarcstate):

        # Get list of jobs in the right state
        jobstofetch = self.db.getArcJobs("arcstate='"+arcstate+"' and cluster='"+self.cluster+"'" + " limit 100")

        if not jobstofetch:
            return
        self.log.info("Fetching %i jobs" % sum(len(v) for v in jobstofetch.values()))

        fetched = []; notfetched = []; notfetchedretry = []
        for proxyid, jobs in jobstofetch.items():
            self.uc.CredentialString(str(self.db.getProxy(proxyid)))

            # Clean the download dir just in case something was left from previous attempt
            for job in jobs:
                shutil.rmtree(self.tmpdir + job[2].JobID[job[2].JobID.rfind('/'):], True)

            # Get list of downloadable files for these jobs
            filestodl = self.db.getArcJobsInfo("arcstate='"+arcstate+"' and cluster='"+self.cluster+"' and proxyid='"+str(proxyid)+"'", ['id', 'downloadfiles'])
            # id: downloadfiles
            downloadfiles = dict((row['id'], row['downloadfiles']) for row in filestodl)
            # jobs to download all files
            jobs_downloadall = dict((j[0], j[2]) for j in jobs if j[0] in downloadfiles and not downloadfiles[j[0]])
            # jobs to download specific files
            jobs_downloadsome = dict((j[0], j[2]) for j in jobs if j[0] in downloadfiles and downloadfiles[j[0]])

            # We don't know if a failure from JobSupervisor is retryable or not
            # so always retry
            (f, r) = self.fetchAll(jobs_downloadall)
            fetched.extend(f)
            notfetchedretry.extend(r)

            nthreads=10
            jobkeys=list(jobs_downloadsome.keys())
            # split job list in nthreads sublists
            jl=[jobkeys[i:i + nthreads] for i in range(0, len(jobkeys), nthreads)]

            for l in jl:
                tlist = []
                # loop over sublist
                for j in l:
                    # one job per thread
                    onejob=dict((k,jobs_downloadsome[k]) for k in [j])
                    t = fetchSomeThr(self.fetchSome, onejob ,downloadfiles)
                    tlist.append(t)
                    t.start()
                for t in tlist:
                    t.join()
                    (f,n,r) = t.result
                    fetched.extend(f)
                    notfetched.extend(n)
                    notfetchedretry.extend(r)

        # Check for massive failure, and back off before trying again
        # TODO: downtime awareness
        if len(notfetched) > 10 and len(notfetched) == len(jobstofetch) or \
           len(notfetchedretry) > 10 and len(notfetchedretry) == len(jobstofetch):
            self.log.error("Failed to get any jobs from %s, sleeping for 5 mins" % self.cluster)
            time.sleep(300)
            return

        for proxyid, jobs in jobstofetch.items():
            for (id, appjobid, job, created) in jobs:
                if job.JobID in notfetchedretry:
                    self.log.warning("%s: Could not get output from job %s" % (appjobid, job.JobID))
                    # Remove download directory to allow retry
                    shutil.rmtree(self.tmpdir + job.JobID[job.JobID.rfind('/'):], True)
                    # Check if job still exists
                    fileinfo = arc.FileInfo()
                    self.uc.CredentialString(str(self.db.getProxy(proxyid)))
                    dp = aCTUtils.DataPoint(job.JobID, self.uc)
                    status = dp.h.Stat(fileinfo)
                    # TODO Check other permanent errors
                    if not status and status.GetErrno() == errno.ENOENT:
                        self.log.warning("%s: Job %s no longer exists" % (appjobid, job.JobID))
                        self.db.updateArcJob(id, {"arcstate": "donefailed",
                                                  "tarcstate": self.db.getTimeStamp()})
                    # Otherwise try again next time
                elif job.JobID in notfetched:
                    self.log.error("%s: Failed to download job %s" % (appjobid, job.JobID))
                    self.db.updateArcJob(id, {"arcstate": "donefailed",
                                              "tarcstate": self.db.getTimeStamp()})
                else:
                    self.log.info("%s: Downloaded job %s" % (appjobid, job.JobID))
                    self.db.updateArcJob(id, {"arcstate": nextarcstate,
                                              "tarcstate": self.db.getTimeStamp()})


    def process(self):

        # download failed job outputs that should be fetched
        self.fetchJobs('tofetch', 'donefailed')
        # download finished job outputs
        self.fetchJobs('finished', 'done')


if __name__ == '__main__':
    st=aCTFetcher()
    st.run()
    st.finish()
