import datetime
import os
import shutil
from http.client import HTTPException
from json import JSONDecodeError
from ssl import SSLError

from act.arc.aCTARCProcess import aCTARCProcess
from act.common.aCTJob import ACTJob
from pyarcrest.arc import ARCRest
from pyarcrest.errors import ARCError, ARCHTTPError, MissingDiagnoseFile


# TODO: HARDCODED
HTTP_BUFFER_SIZE = 2 ** 23  # 8MB


class aCTFetcher(aCTARCProcess):

    def setup(self):
        super().setup()

        self.loadConf()
        self.tmpdir = self.conf.tmp.dir

    # TODO: refactor to some library aCT job operation
    def fetchJobs(self, arcstate, nextarcstate):
        """
        Download results of jobs to configured directory.

        Signal handling strategy:
        - method checks termination before job batch for every proxyid
        """
        COLUMNS = ["id", "appjobid", "proxyid", "IDFromEndpoint", "downloadfiles", "tarcstate"]

        # TODO: HARDCODED
        jobstofetch = self.db.getArcJobsInfo(f"arcstate='{arcstate}' and cluster='{self.cluster}' limit 100", COLUMNS)

        if not jobstofetch:
            return
        self.log.info(f"Fetching {len(jobstofetch)} jobs")

        # aggregate jobs by proxyid
        jobsdict = {}
        for row in jobstofetch:
            if not row["proxyid"] in jobsdict:
                jobsdict[row["proxyid"]] = []
            jobsdict[row["proxyid"]].append(row)

        for proxyid, dbjobs in jobsdict.items():

            if self.mustExit:
                self.log.info(f"Exiting early due to requested shutdown")
                self.stopWithException()

            jobs = []
            tofetch = []
            for dbjob in dbjobs:
                job = ACTJob()
                job.loadARCDBJob(dbjob)
                jobs.append(job)
                tofetch.append(job.arcjob)
                resdir = os.path.join(self.tmpdir, job.arcjob.id)
                shutil.rmtree(resdir, True)
                os.makedirs(resdir, exist_ok=True)

            proxypath = os.path.join(self.db.proxydir, f"proxiesid{proxyid}")

            # fetch job results from REST
            arcrest = None
            try:
                arcrest = ARCRest.getClient(self.cluster, proxypath=proxypath, logger=self.log)

                # fetch jobs
                # TODO: HARDCODED
                arcrest.downloadJobFiles(self.tmpdir, tofetch, workers=10, blocksize=HTTP_BUFFER_SIZE, timeout=60)

            except JSONDecodeError as exc:
                self.log.error(f"Invalid JSON response from ARC: {exc}")
            except (HTTPException, ConnectionError, SSLError, ARCError, ARCHTTPError, TimeoutError, OSError, ValueError) as exc:
                self.log.error(f"Error fetching jobs in ARC: {exc}")
            finally:
                if arcrest:
                    arcrest.close()

            for job in jobs:
                isError = False
                for error in job.arcjob.errors:
                    # don't treat missing diagnose file as fail
                    if isinstance(error, MissingDiagnoseFile):
                        self.log.info(str(error))
                    else:
                        isError = True
                        self.log.error(f"Error fetching job {job.appid} {job.arcid}: {error}")
                if isError:
                    # TODO: HARDCODED
                    if job.tarcstate + datetime.timedelta(hours=24) < datetime.datetime.utcnow():
                        self.log.info(f"Fetch timeout for job {job.appid} {job.arcid}, marking job \"donefailed\"")
                        jobdict = {"arcstate": "donefailed", "tarcstate": self.db.getTimeStamp()}
                        self.db.updateArcJob(job.arcid, jobdict)
                    else:
                        self.log.info(f"Fetch timeout for job {job.appid} {job.arcid} not reached, will retry")
                else:
                    self.log.debug(f"Successfully fetched job {job.appid} {job.arcid}")
                    jobdict = {"arcstate": nextarcstate, "tarcstate": self.db.getTimeStamp()}
                    self.db.updateArcJob(job.arcid, jobdict)

        self.log.debug("Done")

    def process(self):
        # download failed job outputs that should be fetched
        self.fetchJobs('tofetch', 'donefailed')
        # download finished job outputs
        self.fetchJobs('finished', 'done')
