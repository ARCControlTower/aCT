import datetime
import os
import shutil
from json import JSONDecodeError

from act.arc.aCTARCProcess import aCTARCProcess
from pyarcrest.arc import ARCRest
from pyarcrest.errors import MissingDiagnoseFile, MissingResultFile

# TODO: document downloadfiles syntax
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
                self.log.info("Exiting early due to requested shutdown")
                self.stopWithException()

            # create parameters for download from ARC
            arcids = []
            outputFilters = {}
            diagnoseFiles = {}
            diagnoseDirs = {}
            for dbjob in dbjobs:
                arcid = dbjob["IDFromEndpoint"]
                arcids.append(arcid)
                downloadfiles = dbjob.get("downloadfiles", None)
                if downloadfiles:
                    # If there are multiple conflicting diagnose= entries,
                    # weird things can happen. Since this is internal to
                    # aCT, we don't bother to improve this for now.
                    for pattern in downloadfiles.split(";"):
                        if pattern.startswith("diagnose="):
                            path = pattern[len("diagnose="):]
                            parts = path.rsplit("/", 1)

                            # no subdir for diagnose file
                            # e. g. diagnose=errors
                            if len(parts) == 1:
                                diagnoseDirs[arcid] = ""
                            # all diagnose files in subdir
                            # e. g. diagnose=gmlog/
                            elif parts[1] == "":
                                diagnoseDirs[arcid] = parts[0]
                                diagnoseFiles.pop(arcid, None)
                                continue
                            # a diagnose file in subdir
                            # e. g. diagnose=gmlog/errors
                            else:
                                diagnoseDirs[arcid] = parts[0]

                            diagnoseFiles.setdefault(arcid, []).append(parts[-1])
                        else:
                            outputFilters.setdefault(arcid, []).append(pattern)

                # remove existing downloads (if previous failed)
                resdir = os.path.join(self.tmpdir, arcid)
                shutil.rmtree(resdir, True)
                os.makedirs(resdir, exist_ok=True)

            # get REST client
            proxypath = os.path.join(self.db.proxydir, f"proxiesid{proxyid}")
            try:
                arcrest = ARCRest.getClient(url=self.cluster, proxypath=proxypath, logger=self.log)
            except Exception as exc:
                self.log.error(f"Error creating REST client for proxy ID {proxyid} stored in {proxypath}: {exc}")
                continue

            # fetch job results from REST
            try:
                # TODO: HARDCODED
                results = arcrest.downloadJobFiles(self.tmpdir, arcids, outputFilters, diagnoseFiles, diagnoseDirs, workers=10, blocksize=HTTP_BUFFER_SIZE, timeout=60)
            except JSONDecodeError as exc:
                self.log.error(f"Invalid JSON response from ARC: {exc}")
                continue
            except Exception as exc:
                self.log.error(f"Error fetching jobs in ARC: {exc}")
                continue
            finally:
                arcrest.close()

            for job, errors in zip(dbjobs, results):
                isError = False
                for error in errors:
                    # don't treat missing diagnose file as fail
                    if isinstance(error, MissingDiagnoseFile):
                        self.log.info(f"Skipping the missing diagnose file \"{error.filename}\" for job {job['appjobid']}")

                    # missing result file -> error
                    elif isinstance(error, MissingResultFile):
                        isError = True
                        self.log.error(f"Error fetching job {job['appjobid']}: missing file {error.filename}")

                    # all other errors are fails as well
                    else:
                        isError = True
                        self.log.error(f"Error fetching job {job['appjobid']}: {error}")

                if isError:
                    # TODO: HARDCODED
                    if job['tarcstate'] + datetime.timedelta(hours=24) < datetime.datetime.utcnow():
                        jobdict = {"arcstate": "donefailed", "tarcstate": self.db.getTimeStamp()}
                        self.db.updateArcJob(job["id"], jobdict)
                        self.log.info(f"Fetch timeout for job {job['appjobid']}, marking job \"donefailed\"")
                    else:
                        self.log.info(f"Fetch timeout for job {job['appjobid']} not reached, will retry")
                else:
                    jobdict = {"arcstate": nextarcstate, "tarcstate": self.db.getTimeStamp()}
                    self.db.updateArcJob(job["id"], jobdict)
                    self.log.debug(f"Successfully fetched job {job['appjobid']}")

        self.log.debug("Done")

    def process(self):
        # download failed job outputs that should be fetched
        self.fetchJobs('tofetch', 'donefailed')
        # download finished job outputs
        self.fetchJobs('finished', 'done')
