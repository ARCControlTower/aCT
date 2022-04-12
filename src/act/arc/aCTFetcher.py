# aCTFetcher.py
#
# Fetches output from finished jobs
#

import http.client
import os
import shutil
import ssl
from urllib.parse import urlparse

from act.arc.rest import fetchJobs
from act.common.aCTProcess import aCTProcess
from act.common.exceptions import ACTError


class aCTFetcher(aCTProcess):
    '''
    Downloads output data for finished ARC jobs.
    '''

    def fetchJobs(self, arcstate, nextarcstate):
        COLUMNS = ["id", "appjobid", "proxyid", "IDFromEndpoint", "downloadfiles", "jobdesc"]

        # TODO: hardcoded
        jobstofetch = self.db.getArcJobsInfo(f"arcstate='{arcstate}' and cluster='{self.cluster}' limit 100", COLUMNS)

        if not jobstofetch:
            return
        self.log.info(f"Fetching {len(jobstofetch)} jobs")

        url = urlparse(self.cluster)

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

            proxypath = os.path.join(self.db.proxydir, f"proxiesid{proxyid}")
            conn = None
            try:
                # create proxy authenticated connection
                context = ssl.SSLContext(ssl.PROTOCOL_TLS)
                context.load_cert_chain(proxypath, keyfile=proxypath)
                conn = http.client.HTTPSConnection(url.netloc, context=context)
                self.log.debug(f"Connected to cluster {url.netloc}")

                # fetch jobs
                # TODO: hardcoded workers
                results = fetchJobs(conn, "/arex/rest/1.0", self.tmpdir, proxypath, jobs, 10, logger=self.log)

                for job in results:
                    # TODO: retry based on error condition
                    if "msg" in job:
                        self.log.error(f"Error fetching  job {job['appjobid']}: {job['msg']}")
                        jobdict = {"arcstate": "donefailed", "tarcstate": self.db.getTimeStamp()}
                        self.db.updateArcJobLazy(job["id"], jobdict)
                    else:
                        self.log.debug(f"Successfully fetched job {job['appjobid']}")
                        jobdict = {"arcstate": nextarcstate, "tarcstate": self.db.getTimeStamp()}
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
