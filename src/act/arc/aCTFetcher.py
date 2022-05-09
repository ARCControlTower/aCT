# aCTFetcher.py
#
# Fetches output from finished jobs
#

import os
import shutil
import datetime
from http.client import HTTPException
from json import JSONDecodeError
from ssl import SSLError
from urllib.parse import urlparse

from act.arc.rest import RESTClient
from act.common.aCTProcess import aCTProcess
from act.common.exceptions import ACTError, ARCHTTPError


class aCTFetcher(aCTProcess):
    '''
    Downloads output data for finished ARC jobs.
    '''

    def fetchJobs(self, arcstate, nextarcstate):
        COLUMNS = ["id", "appjobid", "proxyid", "IDFromEndpoint", "downloadfiles", "jobdesc", "tarcstate"]

        # parse cluster URL
        try:
            url = urlparse(self.cluster)
        except ValueError as exc:
            self.log.error(f"Error parsing cluster URL {url}: {exc}")
            return

        # TODO: hardcoded
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

        for proxyid, jobs in jobsdict.items():
            # remove existing results, create result dir
            for job in jobs:
                resdir = os.path.join(self.tmpdir, job["IDFromEndpoint"])
                shutil.rmtree(resdir, True)
                os.makedirs(resdir)

                # add key for fetchJobs
                job["arcid"] = job["IDFromEndpoint"]
                job["errors"] = []

            proxypath = os.path.join(self.db.proxydir, f"proxiesid{proxyid}")

            try:
                restClient = RESTClient(url.hostname, port=url.port, proxypath=proxypath)

                # fetch jobs
                # TODO: hardcoded workers
                results = restClient.fetchJobs(self.tmpdir, jobs, workers=10, logger=self.log)

            except (HTTPException, ConnectionError, SSLError, ACTError, ARCHTTPError, TimeoutError) as exc:
                self.log.error(f"Error killing jobs in ARC: {exc}")
            except JSONDecodeError as exc:
                self.log.error(f"Invalid JSON response from ARC: {exc}")
            finally:
                restClient.close()

            for job in results:
                # TODO: retry based on error condition
                if job["errors"]:
                    for error in job["errors"]:
                        self.log.error(f"Error fetching  job {job['appjobid']} {job['id']}: {error}")
                    if job["tarcstate"] + datetime.timedelta(hours=24) < datetime.datetime.utcnow():
                        jobdict = {"arcstate": "donefailed", "tarcstate": self.db.getTimeStamp()}
                        self.db.updateArcJobLazy(job["id"], jobdict)
                    else:
                        self.log.info(f"Fetch timeout for job {job['appjobid']} {job['id']} not reached, will retry")
                else:
                    self.log.debug(f"Successfully fetched job {job['appjobid']} {job['id']}")
                    jobdict = {"arcstate": nextarcstate, "tarcstate": self.db.getTimeStamp()}
                    self.db.updateArcJobLazy(job["id"], jobdict)

            self.db.Commit()

        self.log.debug("Done")

    def process(self):
        # download failed job outputs that should be fetched
        self.fetchJobs('tofetch', 'donefailed')
        # download finished job outputs
        self.fetchJobs('finished', 'done')


if __name__ == '__main__':
    st=aCTFetcher()
    st.run()
    st.finish()
