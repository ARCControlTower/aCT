# aCTCleaner.py
#
# Cleans jobs from CE and ARC DB
#
import os
from http.client import HTTPException
from ssl import SSLError
from urllib.parse import urlparse
from json import JSONDecodeError

from act.arc.rest import RESTClient
from act.common.aCTProcess import aCTProcess
from act.common.exceptions import ACTError, ARCHTTPError


class aCTCleaner(aCTProcess):

    def processToClean(self):
        COLUMNS = ["id", "appjobid", "proxyid", "IDFromEndpoint"]

        # parse cluster URL
        try:
            url = urlparse(self.cluster)
        except ValueError as exc:
            self.log.error(f"Error parsing cluster URL {url}: {exc}")
            return

        # TODO: hardcoded limits
        #
        # Fetch all jobs that can be cleaned from database.
        jobstoclean = self.db.getArcJobsInfo(
            f"arcstate='toclean' and cluster='{self.cluster}' limit 100",
            COLUMNS
        )
        dbtoclean = self.db.getArcJobsInfo(
            "arcstate='toclean' and cluster='' limit 100",
            COLUMNS
        )
        if dbtoclean:
            jobstoclean.extend(dbtoclean)
        if not jobstoclean:
            return

        self.log.info(f"Cleaning {len(jobstoclean)} jobs")

        # aggregate jobs by proxyid
        jobsdict = {}
        for row in jobstoclean:
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
                    "appjobid": job["appjobid"],
                    "errors": []
                }
                toclean.append(jobdict)
                if job["IDFromEndpoint"]:
                    toARCClean.append(jobdict)

            proxypath = os.path.join(self.db.proxydir, f"proxiesid{proxyid}")

            try:
                restClient = RESTClient(url.hostname, port=url.port, proxypath=proxypath)
                toARCClean = restClient.cleanJobs(toARCClean)
            except (HTTPException, ConnectionError, SSLError, ACTError, ARCHTTPError, TimeoutError) as exc:
                self.log.error(f"Error killing jobs in ARC: {exc}")
            except JSONDecodeError as exc:
                self.log.error(f"Invalid JSON response from ARC: {exc}")
            finally:
                restClient.close()

            # log results and update DB
            for job in toARCClean:
                if job["errors"]:
                    for error in job["errors"]:
                        self.log.error(f"Error cleaning job {job['appjobid']} {job['id']}: {error}")
                else:
                    self.log.debug(f"Successfully cleaned job {job['appjobid']} {job['id']}")
            for job in toclean:
                self.db.deleteArcJob(job["id"])
            self.db.Commit()

        self.log.debug("Done")

    def process(self):
        # clean jobs
        self.processToClean()


if __name__ == '__main__':
    st = aCTCleaner()
    st.run()
    st.finish()
