# aCTCleaner.py
#
# Cleans jobs from CE and ARC DB
#
import http.client
import os
from act.arc.rest import cleanJobs
import ssl
from urllib.parse import urlparse

from act.common.aCTProcess import aCTProcess
from act.common.exceptions import ACTError


class aCTCleaner(aCTProcess):

    def processToClean(self):
        COLUMNS = ["id", "appjobid", "proxyid", "IDFromEndpoint", "cluster"]

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

        url = urlparse(self.cluster)

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
                    "appjobid": job["appjobid"]
                }
                toclean.append(jobdict)
                if job["cluster"]:
                    toARCClean.append(jobdict)

            proxypath = os.path.join(self.db.proxydir, f"proxiesid{proxyid}")
            conn = None
            try:
                # create proxy authenticated connection
                context = ssl.SSLContext(ssl.PROTOCOL_TLS)
                context.load_cert_chain(proxypath, keyfile=proxypath)
                conn = http.client.HTTPSConnection(url.netloc, context=context)
                self.log.debug(f"Connected to cluster {url.netloc}")

                # clean jobs and log results
                toARCClean = cleanJobs(conn, toARCClean)
            except ssl.SSLError as e:
                self.log.error(f"Could not create SSL context for proxy {proxypath}: {e}")
            except http.client.HTTPException as e:
                self.log.error(f"Could not connect to cluster {url.netloc}: {e}")
            except ACTError as e:
                self.log.error(f"Error cleaning ARC jobs: {e}")
            else:
                for job in toARCClean:
                    if "msg" in job:
                        self.log.error(f"Error cleaning appjobid({job['appjobid']}), id({job['id']}): {job['msg']}")
                    else:
                        self.log.debug(f"Successfully cleaned appjobid({job['appjobid']}), id({job['id']})")
            finally:
                if conn:
                    conn.close()

            for job in toclean:
                self.db.deleteArcJob(job["id"])

        self.log.debug("Finished cleaning jobs")

    def process(self):

        # clean jobs
        self.processToClean()


if __name__ == '__main__':
    st = aCTCleaner()
    st.run()
    st.finish()
