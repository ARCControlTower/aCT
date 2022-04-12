import datetime
import http.client
import os
import ssl
from random import shuffle
from urllib.parse import urlparse

from act.arc.rest import (cleanJobs, getJobsDelegations, killJobs,
                          renewDelegation, restartJobs, submitJobs)
from act.common.aCTProcess import aCTProcess
from act.common.exceptions import ACTError, SubmitError


class aCTSubmitter(aCTProcess):

    def submit(self):
        """
        Main function to submit jobs.
        """

        # check for stopsubmission flag
        if self.conf.get(['downtime','stopsubmission']) == "true":
            self.log.info('Submission suspended due to downtime')
            return

        # check for any site-specific limits or status
        clusterstatus = self.conf.getCond(["sites", "site"], f"endpoint={self.cluster}", ["status"]) or 'online'
        if clusterstatus == 'offline':
            self.log.info('Site status is offline')
            return

        clustermaxjobs = int(self.conf.getCond(["sites", "site"], f"endpoint={self.cluster}", ["maxjobs"]) or 999999)
        nsubmitted = self.db.getNArcJobs(f"cluster='{self.cluster}'")
        if nsubmitted >= clustermaxjobs:
            self.log.info(f'{nsubmitted} submitted jobs is greater than or equal to max jobs {clustermaxjobs}')
            return

        # Apply fair-share
        if self.cluster:
            fairshares = self.db.getArcJobsInfo("arcstate='tosubmit' and clusterlist like '%"+self.cluster+"%'", ['fairshare', 'proxyid'])
        else:
            fairshares = self.db.getArcJobsInfo("arcstate='tosubmit' and clusterlist=''", ['fairshare', 'proxyid'])

        if not fairshares:
            self.log.info('Nothing to submit')
            return

        # split by proxy for GU queues
        fairshares = list(set([(p['fairshare'], p['proxyid']) for p in fairshares]))
        # For proxy bug - see below
        shuffle(fairshares)

        # apply maxjobs limit per submitter (check above should make sure greater than zero)
        nsubmitters = int(self.conf.getCond(["sites", "site"], f"endpoint={self.cluster}", ["submitters"]) or 1)
        limit = min(clustermaxjobs - nsubmitted, 100) // nsubmitters
        if limit == 0:
            self.log.info(f'{clustermaxjobs} maxjobs - {nsubmitted} submitted is smaller than {nsubmitters} submitters')
            return

        # Divide limit among fairshares, unless exiting after first loop due to
        # proxy bug, but make sure at least one job is submitted
        if len(self.db.getProxiesInfo('TRUE', ['id'])) == 1:
            limit = max(limit // len(fairshares), 1)

        for fairshare, proxyid in fairshares:

            # Exit loop if above limit
            if nsubmitted >= clustermaxjobs:
                self.log.info("CE is at limit of %s submitted jobs, exiting" % clustermaxjobs)
                break

            try:
                # catch any exceptions here to avoid leaving lock
                if self.cluster:
                    # Lock row for update in case multiple clusters are specified
                    #jobs=self.db.getArcJobsInfo("arcstate='tosubmit' and ( clusterlist like '%{0}' or clusterlist like '%{0},%' ) and fairshare='{1}' order by priority desc limit 10".format(self.cluster, fairshare),
                    jobs=self.db.getArcJobsInfo("arcstate='tosubmit' and ( clusterlist like '%{0}' or clusterlist like '%{0},%' ) and fairshare='{1}' and proxyid='{2}' limit {3}".format(self.cluster, fairshare, proxyid, limit),
                                                columns=["id", "jobdesc", "appjobid", "priority", "proxyid", "clusterlist"], lock=True)
                    if jobs:
                        self.log.debug("started lock for writing %d jobs"%len(jobs))
                else:
                    jobs=self.db.getArcJobsInfo("arcstate='tosubmit' and clusterlist='' and fairshare='{0} and proxyid={1}' limit {2}".format(fairshare, proxyid, limit),
                                                columns=["id", "jobdesc", "appjobid", "priority", "proxyid", "clusterlist"])
                # mark submitting in db
                jobs_taken=[]
                for j in jobs:
                    jd={'cluster': self.cluster, 'arcstate': 'submitting', 'tarcstate': self.db.getTimeStamp()}
                    self.db.updateArcJobLazy(j['id'],jd)
                    jobs_taken.append(j)
                jobs=jobs_taken

            finally:
                if self.cluster:
                    try:
                        self.db.Commit(lock=True)
                        self.log.debug("ended lock")
                    except:
                        self.log.warning("Failed to release DB lock")
                else:
                    self.db.Commit()

            if len(jobs) == 0:
                #self.log.debug("No jobs to submit")
                continue
            self.log.info("Submitting %d jobs for fairshare %s and proxyid %d" % (len(jobs), fairshare, proxyid))

            # Set UserConfig credential for querying infosys
            proxystring = str(self.db.getProxy(proxyid))
            self.uc.CredentialString(proxystring)
            global usercred
            usercred = self.uc

            # Filter only sites for this process
            qjobs=self.db.getArcJobsInfo("cluster='" +str(self.cluster)+ "' and  arcstate='submitted' and fairshare='%s'" % fairshare, ['id','priority'])
            rjobs=self.db.getArcJobsInfo("cluster='" +str(self.cluster)+ "' and  arcstate='running' and fairshare='%s'" % fairshare, ['id'])

            # max waiting priority
            try:
                maxpriowaiting = max(jobs,key = lambda x : x['priority'])['priority']
            except:
                maxpriowaiting = 0
            self.log.info("Maximum priority of waiting jobs: %d" % maxpriowaiting)


            # max queued priority
            try:
                maxprioqueued = max(qjobs,key = lambda x : x['priority'])['priority']
            except:
                maxprioqueued = 0
            self.log.info("Max priority queued: %d" % maxprioqueued)

            qfraction = float(self.conf.get(['jobs', 'queuefraction'])) if self.conf.get(['jobs', 'queuefraction']) else 0.15
            qoffset = int(self.conf.get(['jobs', 'queueoffset'])) if self.conf.get(['jobs', 'queueoffset']) else 100

            ##################################################################
            #
            # New REST submission code
            #
            ##################################################################

            self.log.info(f"Submitting {len(jobs)} jobs")

            # get proxy path
            proxypath = os.path.join(self.db.proxydir, f"proxiesid{proxyid}")
            if not os.path.isfile(proxypath):
                self.log.error(f"Proxy path {proxypath} is not a file")
                self.setJobsArcstate(jobs, "tosubmit", commit=True)
                continue

            # read job descriptions from DB
            for job in jobs:
                job["descstr"] = str(self.db.getArcJobDescription(str(job["jobdesc"])))

            # parse cluster URL
            url = urlparse(self.cluster)
            queue = url.path.split("/")[-1]

            try:
                # create proxy authenticated connection
                context = ssl.SSLContext(ssl.PROTOCOL_TLS)
                context.load_cert_chain(proxypath, keyfile=proxypath)
                conn = http.client.HTTPSConnection(url.netloc, context=context)

                # submit jobs to ARC
                jobs = submitJobs(conn, queue, proxypath, jobs, logger=self.log)

            except ssl.SSLError as e:
                self.log.error(f"Could not create SSL context for proxy {proxypath}: {e}")
                self.setJobsArcstate(jobs, "tosubmit", commit=True)
                continue
            except (http.client.HTTPException, ConnectionError) as e:
                self.log.error(f"Could not connect to cluster {url.netloc}: {e}")
                self.setJobsArcstate(jobs, "tosubmit", commit=True)
                continue
            except SubmitError as e:
                self.log.error(str(e))
                self.setJobsArcstate(jobs, e.arcstate, commit=True)
                continue

            # log submission results and set job state
            for job in jobs:
                if "msg" in job:
                    job["arcstate"] = "tocancel"
                    self.log.debug(f"Submission failed for job {job['appjobid']}: {job['msg']}")
                else:
                    job["arcstate"] = "submitted"
                    self.log.debug(f"Submission successfull for job {job['appjobid']}: {job['arcid']}")

            # update job records in DB
            for job in jobs:
                jobdict = {}
                jobdict["arcstate"] = job["arcstate"]
                jobdict["tarcstate"] = self.db.getTimeStamp()
                jobdict["tstate"] = self.db.getTimeStamp()
                jobdict["cluster"] = self.cluster

                if url.port is None:
                    port = 443
                else:
                    port = url.port
                interface = f"https://{url.hostname}:{port}/arex"

                if "delegation" in job:
                    jobdict["DelegationID"] = job["delegation"]
                if "arcid" in job:
                    jobdict["IDFromEndpoint"] = job["arcid"]
                    jobdict["JobID"] = f"{interface}/rest/1.0/jobs/{job['arcid']}"
                if "state" in job:
                    jobdict["State"] = job["state"]

                jobdict["JobManagementInterfaceName"] = "org.nordugrid.arcrest"
                jobdict["JobManagementURL"] = interface
                jobdict["JobStatusInterfaceName"] = "org.nordugrid.arcrest"
                jobdict["JobStatusURL"] = interface
                jobdict["ServiceInformationInterfaceName"] = "org.nordugrid.arcrest"
                jobdict["ServiceInformationURL"] = interface

                self.db.updateArcJobLazy(job["id"], jobdict)

            self.db.Commit()

        self.log.info("Done")

    def setJobsArcstate(self, jobs, arcstate, commit=False):
        for job in jobs:
            updateDict = {"arcstate": arcstate, "tarcstate": self.db.getTimeStamp()}
            self.db.updateArcJobLazy(job["id"], updateDict)
        if commit:
            self.db.Commit()

    # jobs that have been in submitting state for more than an hour
    # should be canceled
    def checkFailedSubmissions(self):

        jobs = self.db.getArcJobsInfo(f"arcstate='tosubmit' and cluster='{self.cluster}'", ["id", "appjobid", "jobdesc", "created"])

        for job in jobs:
            if job["created"] + datetime.timedelta(hours=1) < datetime.datetime.now(): # TODO: hardcoded
                self.db.updateArcJobLazy(job["id"], {"arcstate": "tocancel", "tarcstate": self.db.getTimeStamp()})

        self.db.Commit()

    def processToCancel(self):
        COLUMNS = ["id", "appjobid", "proxyid", "IDFromEndpoint"]

        # fetch jobs from DB for this cluster and also jobs that
        # don't have cluster assigned
        jobstocancel = self.db.getArcJobsInfo(
            "arcstate='tocancel' and (cluster='{0}' or clusterlist like '%{0}' or clusterlist like '%{0},%')".format(self.cluster),
            COLUMNS
        )
        dbtocancel = self.db.getArcJobsInfo(
            "arcstate='tocancel' and cluster=''",
            COLUMNS
        )
        if dbtocancel:
            jobstocancel.extend(dbtocancel)
        if not jobstocancel:
            return

        self.log.info(f"Cancelling {len(jobstocancel)} jobs")

        # aggregate jobs by proxyid
        jobsdict = {}
        for row in jobstocancel:
            if not row["proxyid"] in jobsdict:
                jobsdict[row["proxyid"]] = []
            jobsdict[row["proxyid"]].append(row)

        for proxyid, jobs in jobsdict.items():

            # create jobdicts for kill operation
            tokill = []  # all jobs to be killed
            toARCKill = []  # jobs to be killed in ARC
            for job in jobs:
                jobdict = {
                    "arcid": job["IDFromEndpoint"],
                    "id": job["id"],
                    "appjobid": job["appjobid"],
                    "arcstate": "cancelled"
                }
                tokill.append(jobdict)
                if job["IDFromEndpoint"]:
                    toARCKill.append(jobdict)

            proxypath = os.path.join(self.db.proxydir, f"proxiesid{proxyid}")
            conn = None
            try:
                # create proxy authenticated connection
                context = ssl.SSLContext(ssl.PROTOCOL_TLS)
                context.load_cert_chain(proxypath, keyfile=proxypath)
                url = urlparse(self.cluster)
                conn = http.client.HTTPSConnection(url.netloc, context=context)
                self.log.debug(f"Connected to cluster {url.netloc}")

                # clean jobs and log results
                toARCKill = killJobs(conn, toARCKill)
                for job in toARCKill:
                    if "msg" in job:
                        self.log.error(f"Error canceling job {job['appjobid']}: {job['msg']}")
                    else:
                        job["arcstate"] = "cancelling"
                        self.log.debug(f"ARC will cancel job {job['appjobid']}")

                # update DB
                for job in tokill:
                    jobdict = {"arcstate": job["arcstate"], "tarcstate": self.db.getTimeStamp()}
                    if job["arcstate"] == "cancelling":
                        jobdict["tstate"] = self.db.getTimeStamp()
                    self.db.updateArcJobLazy(job["id"], jobdict)
                self.db.Commit()

            except ssl.SSLError as e:
                self.log.error(f"Could not create SSL context for proxy {proxypath}: {e}")
            except (http.client.HTTPException, ConnectionError) as e:
                self.log.error(f"Could not connect to cluster {url.netloc}: {e}")
            except ACTError as e:
                self.log.error(f"Error canceling ARC jobs: {e}")
            finally:
                if conn:
                    conn.close()

    # This does not handle jobs with empty clusterlist. What about that?
    #
    # This does not kill jobs (before cleaning them)!!!
    def processToResubmit(self):
        COLUMNS = ["id", "appjobid", "proxyid", "IDFromEndpoint", "cluster"]

        # fetch jobs from DB
        jobstoresubmit = self.db.getArcJobsInfo(
            f"arcstate='toresubmit' and cluster='{self.cluster}'",
            COLUMNS
        )
        if not jobstoresubmit:
            return

        self.log.info(f"Resubmitting {len(jobstoresubmit)} jobs")

        # aggregate jobs by proxyid
        jobsdict = {}
        for row in jobstoresubmit:
            if not row["proxyid"] in jobsdict:
                jobsdict[row["proxyid"]] = []
            jobsdict[row["proxyid"]].append(row)

        for proxyid, jobs in jobsdict.items():

            # create job dicts for clean operation
            toclean = []
            toARCClean = []
            for job in jobs:
                jobdict = {
                    "arcid": job["IDFromEndpoint"],
                    "id": job["id"],
                    "appjobid": job["appjobid"]
                }
                toclean.append(jobdict)
                if "cluster" in job:
                    toARCClean.append(jobdict)

            proxypath = os.path.join(self.db.proxydir, f"proxiesid{proxyid}")
            conn = None
            try:
                # create proxy authenticated connection
                context = ssl.SSLContext(ssl.PROTOCOL_TLS)
                context.load_cert_chain(proxypath, keyfile=proxypath)
                url = urlparse(self.cluster)
                conn = http.client.HTTPSConnection(url.netloc, context=context)
                self.log.debug(f"Connected to cluster {url.netloc}")

                # clean jobs and log results
                toARCClean = cleanJobs(conn, toARCClean)
                for job in toARCClean:
                    if "msg" in job:
                        self.log.error(f"Error cleaning job {job['appjobid']}: {job['msg']}")
                    else:
                        self.log.debug(f"Successfully cleaned job {job['appjobid']}")

                # update DB
                for job in toclean:
                    tstamp = self.db.getTimeStamp()
                    # "created" needs to be reset so that it doesn't get understood
                    # as failing to submit since first insertion.
                    jobdict = {"arcstate": "tosubmit", "tarcstate": tstamp, "created": tstamp}
                    self.db.updateArcJobLazy(job["id"], jobdict)
                self.db.Commit()

            except ssl.SSLError as e:
                self.log.error(f"Could not create SSL context for proxy {proxypath}: {e}")
            except http.client.HTTPException as e:
                self.log.error(f"Could not connect to cluster {url.netloc}: {e}")
            except ACTError as e:
                self.log.error(f"Error cleaning ARC jobs: {e}")
            finally:
                if conn:
                    conn.close()

    def processToRerun(self):
        COLUMNS = ["id", "appjobid", "proxyid", "IDFromEndpoint"]

        # fetch jobs from DB
        jobstorerun = self.db.getArcJobsInfo(
            f"arcstate='torerun' and cluster='{self.cluster}'",
            COLUMNS
        )
        if not jobstorerun:
            return

        self.log.info("Resuming {len(jobstorerun)} jobs")

        # aggregate jobs by proxyid
        jobsdict = {}
        for row in jobstorerun:
            if not row["proxyid"] in jobsdict:
                jobsdict[row["proxyid"]] = []
            jobsdict[row["proxyid"]].append(row)

        for proxyid, jobs in jobsdict.items():

            # create a list of jobdicts to be rerun
            torerun = []
            for job in jobs:
                jobdict = {
                    "arcid": job["IDFromEndpoint"],
                    "id": job["id"],
                    "appjobid": job["appjobid"]
                }
                torerun.append(jobdict)

            proxypath = os.path.join(self.db.proxydir, f"proxiesid{proxyid}")
            conn = None
            try:
                # create proxy authenticated connection
                context = ssl.SSLContext(ssl.PROTOCOL_TLS)
                context.load_cert_chain(proxypath, keyfile=proxypath)
                url = urlparse(self.cluster)
                conn = http.client.HTTPSConnection(url.netloc, context=context)
                self.log.debug(f"Connected to cluster {url.netloc}")

                # get delegations for jobs
                # TODO: one delegation for each job is assumed
                try:
                    delegations = getJobsDelegations(conn, "/arex/rest/1.0", torerun)
                except ACTError as e:  # TODO: what to do with such jobs?
                    self.log.error(f"Unable to retreive delegations: {e}")
                    continue

                # renew delegations
                torestart = []
                for delegation, job in zip(delegations, torerun):
                    if int(delegation["status-code"]) != 200:
                        job["msg"] = f"{delegation['status-code']} {delegation['reason']}"
                        continue
                    try:
                        renewDelegation(conn, "/arex/rest/1.0", delegation["delegation_id"], proxypath)
                    except ACTError as e:
                        job["msg"] = str(e)
                    else:
                        torestart.append(job)

                # restart in ARC
                try:
                    restartJobs(conn, torestart)
                except ACTError as e:
                    self.log.error(f"Unable to restart jobs: {e}")
                    continue

                # log results and update DB
                for job in torerun:
                    tstamp = self.db.getTimeStamp()
                    if "msg" in job:
                        self.log.error(f"Error rerunning job {job['appjobid']}: {job['msg']}")
                        self.db.updateArcJobLazy(job["id"], {"arcstate": "failed", "tarcstate": tstamp})
                    else:
                        self.log.info("Successfully rerun job {job['appjobid']}")
                        self.db.updateArcJobLazy(job["id"], {"arcstate": "submitted", "tarcstate": tstamp})
                self.db.Commit()

            except ssl.SSLError as e:
                self.log.error(f"Could not create SSL context for proxy {proxypath}: {e}")
            except http.client.HTTPException as e:
                self.log.error(f"Could not connect to cluster {url.netloc}: {e}")
            except ACTError as e:
                self.log.error(f"Error cleaning ARC jobs: {e}")
            finally:
                if conn:
                    conn.close()

    def process(self):

        # process jobs which have to be cancelled
        self.processToCancel()
        # process jobs which have to be resubmitted
        self.processToResubmit()
        # process jobs which have to be rerun
        self.processToRerun()
        # submit new jobs
        self.submit()
        # check jobs which failed to submit
        self.checkFailedSubmissions()


# Main
if __name__ == '__main__':
    asb=aCTSubmitter()
    asb.run()
    asb.finish()
