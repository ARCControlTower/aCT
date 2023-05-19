import os
from http.client import HTTPException
from json import JSONDecodeError
from ssl import SSLError

from act.arc.aCTARCProcess import aCTARCProcess
from pyarcrest.arc import ARCRest
from pyarcrest.errors import ARCError, ARCHTTPError


class aCTCleaner(aCTARCProcess):

    # TODO: refactor to some library aCT job operation
    def processToClean(self):
        """
        Clean designated jobs from ARC cluster and DB.

        Signal handling strategy:
        - method checks termination before job batch for every proxyid
        """
        COLUMNS = ["id", "appjobid", "proxyid", "IDFromEndpoint"]

        # Fetch all jobs that can be cleaned from database.
        # TODO: HARDCODED
        jobstoclean = self.db.getArcJobsInfo(
            f"arcstate='toclean' and cluster='{self.cluster}' limit 100",
            COLUMNS
        )
        if not jobstoclean:
            return

        self.log.info(f"Cleaning {len(jobstoclean)} jobs")

        # aggregate jobs by proxyid
        jobsdict = {}
        for row in jobstoclean:
            if not row["proxyid"] in jobsdict:
                jobsdict[row["proxyid"]] = []
            jobsdict[row["proxyid"]].append(row)

        for proxyid, dbjobs in jobsdict.items():

            if self.mustExit:
                self.log.info("Exiting early due to requested shutdown")
                self.stopWithException()

            arcjobs = []
            arcids = []
            for job in dbjobs:
                if job.get("IDFromEndpoint", None):
                    arcjobs.append(job)
                    arcids.append(job["IDFromEndpoint"])

            # clean jobs in ARC
            proxypath = os.path.join(self.db.proxydir, f"proxiesid{proxyid}")
            arcrest = None
            try:
                arcrest = ARCRest.getClient(self.cluster, proxypath=proxypath, logger=self.log)
                results = arcrest.cleanJobs(arcids)
            except JSONDecodeError as exc:
                self.log.error(f"Invalid JSON response from ARC: {exc}")
            except (HTTPException, ConnectionError, SSLError, ARCError, ARCHTTPError, TimeoutError, OSError, ValueError) as exc:
                self.log.error(f"Error killing jobs in ARC: {exc}")
            finally:
                if arcrest:
                    arcrest.close()

            # log results
            for job, result in zip(arcjobs, results):
                if isinstance(result, ARCHTTPError):
                    self.log.error(f"Error cleaning job {job['appjobid']} from ARC: {result}")
                else:
                    self.log.debug(f"Successfully cleaned job {job['appjobid']} from ARC")

            # update DB
            for job in dbjobs:
                self.log.debug(f"Successfully cleaned job {job['appjobid']} in arc DB")
                self.db.deleteArcJob(job["id"])

        self.log.debug("Done")

    def process(self):
        self.processToClean()
