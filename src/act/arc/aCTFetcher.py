import datetime
import os
import shutil
from json import JSONDecodeError
from collections import defaultdict

from act.arc.aCTARCProcess import aCTARCProcess
from act.arc.dbModels import ArcJob
from pyarcrest.errors import MissingDiagnoseFile, MissingResultFile
from sqlalchemy import select, update

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
        # fail jobs that are taking too long
        with self.db.Session.begin() as session:
            tstamp = self.db.getTimeStamp()
            if arcstate=='tofetch':
                limit = tstamp - datetime.timedelta(hours=1)
            elif arcstate=='finished':
                limit = tstamp - datetime.timedelta(hours=24)
            jobstofetch = session.execute(select(ArcJob.id, ArcJob.appjobid) \
                                             .where(ArcJob.arcstate==arcstate, ArcJob.cluster==self.cluster, ArcJob.tarcstate<limit)).all()
            if jobstofetch:
                session.execute(self.db.setJobsArcstate([job.id for job in jobstofetch], 'donefailed'))
                for job in jobstofetch:
                    self.log.warning(f"Could not fetch appjob({job.appjobid}) in time, setting to donefailed")

        # fetch remaining jobs
        with self.db.Session() as session:
            tofetch = session.execute(select(ArcJob.id, ArcJob.appjobid, ArcJob.proxyid, ArcJob.IDFromEndpoint, ArcJob.downloadfiles) \
                                      .where(ArcJob.arcstate==arcstate, ArcJob.cluster==self.cluster).limit(100)).all()

        if not tofetch:
            self.log.info(f"Nothing to fetch")
            return
        self.log.info(f"Fetching {len(tofetch)} jobs")
        
        # aggregate jobs by proxyid
        jobsdict = defaultdict(list)
        for job in tofetch:
            jobsdict[job.proxyid].append(job)

        for proxyid, dbjobs in jobsdict.items():
            self.stopOnFlag()

            # create parameters for download from ARC
            arcids = []
            outputFilters = {}
            diagnoseFiles = {}
            diagnoseDirs = {}
            for dbjob in dbjobs:
                arcid = dbjob.IDFromEndpoint
                arcids.append(arcid)
                downloadfiles = dbjob.downloadfiles
                refilter = ""
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
                            if pattern == "/":
                                refilter = ".*"
                            else:
                                refilter += f"|{pattern}"
                                if pattern.endswith("/"):
                                    refilter += ".*"

                    refilter = refilter.lstrip("|")
                    if refilter:
                        outputFilters[arcid] = refilter

                # remove existing downloads (if previous failed)
                resdir = os.path.join(self.tmpdir, arcid)
                shutil.rmtree(resdir, True)
                os.makedirs(resdir, exist_ok=True)

            # get REST client
            arcrest = self.getARCClient(proxyid)
            if not arcrest:
                continue

            # fetch job results from REST
            try:
                # TODO: HARDCODED
                results = arcrest.downloadJobFiles(
                    self.tmpdir,
                    arcids,
                    outputFilters,
                    diagnoseFiles,
                    diagnoseDirs,
                    workers=self.conf.rest.download_workers or 10,
                    recvsize=self.conf.rest.download_size or 8388608,  # 8MB
                    timeout=self.conf.rest.timeout or 60,
                )
            except JSONDecodeError as exc:
                self.log.error(f"Invalid JSON response from ARC: {exc}")
                continue
            except Exception as exc:
                self.log.error(f"Error fetching jobs in ARC: {exc}")
                continue
            finally:
                arcrest.close()

            # process results
            with self.db.Session.begin() as session:
                tstamp = self.db.getTimeStamp()
                for job, errors in zip(dbjobs, results):
                    isError = False
                    for error in errors:
                        # don't treat missing diagnose file as fail
                        if isinstance(error, MissingDiagnoseFile):
                            self.log.info(f"Skipping the missing diagnose file \"{error.filename}\" for appjob({job.appjobid})")

                        # missing result file -> error
                        elif isinstance(error, MissingResultFile):
                            isError = True
                            self.log.error(f"Error fetching appjob({job.appjobid}): missing file {error.filename}")

                        # all other errors are fails as well
                        else:
                            isError = True
                            self.log.error(f"Error fetching appjob({job.appjobid}): {error}")

                    if not isError:
                        session.execute(update(ArcJob).where(ArcJob.id==job.id).values(arcstate=nextarcstate, tarcstate=tstamp))
                        self.log.info(f"Successfully fetched appjob({job.appjobid})")

        self.log.info("Done")

    def process(self):
        # download failed job outputs that should be fetched
        self.fetchJobs('tofetch', 'donefailed')
        # download finished job outputs
        self.fetchJobs('finished', 'done')
