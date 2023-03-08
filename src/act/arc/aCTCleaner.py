# aCTCleaner.py
#
# Cleans jobs from CE and ARC DB
#
import os
from http.client import HTTPException
from json import JSONDecodeError
from ssl import SSLError
from urllib.parse import urlparse

from act.arc.rest import ARCError, ARCHTTPError, ARCRest
from act.common.aCTProcess import aCTProcess
from act.common.aCTJob import ACTJob


class aCTCleaner(aCTProcess):

    def processToClean(self):
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

            jobs = []
            toARCClean = []
            for dbjob in dbjobs:
                job = ACTJob()
                job.loadARCDBJob(dbjob)
                jobs.append(job)
                if job.arcjob.id:
                    toARCClean.append(job.arcjob)

            proxypath = os.path.join(self.db.proxydir, f"proxiesid{proxyid}")

            arcrest = None
            try:
                arcrest = ARCRest(self.cluster, proxypath=proxypath)
                arcrest.cleanJobs(toARCClean)
            except JSONDecodeError as exc:
                self.log.error(f"Invalid JSON response from ARC: {exc}")
            except (HTTPException, ConnectionError, SSLError, ARCError, ARCHTTPError, TimeoutError, OSError, ValueError) as exc:
                self.log.error(f"Error killing jobs in ARC: {exc}")
            finally:
                arcrest.close()

            # log results and update DB
            for job in jobs:
                if job.arcjob.errors:
                    for error in job.arcjob.errors:
                        self.log.error(f"Error cleaning job {job.appid} {job.arcid}: {error}")
                else:
                    self.log.debug(f"Successfully cleaned job {job.appid} {job.arcid}")
                self.db.deleteArcJob(job.arcid)
            self.db.Commit()

        self.log.debug("Done")

    def process(self):
        self.processToClean()


if __name__ == '__main__':
    st = aCTCleaner()
    st.run()
    st.finish()
