# aCTCleaner.py
#
# Cleans jobs from CE and ARC DB
#
import http.client
import json
import ssl
from urllib.parse import urlparse

from act.common.aCTProcess import aCTProcess


class aCTCleaner(aCTProcess):

    def processToClean(self):
        jobstoclean = self.db.getArcJobs("arcstate='toclean' and cluster='"+self.cluster+"' limit 100")

        if not jobstoclean:
            return

        url = urlparse(self.cluster)

        self.log.info(f"Cleaning {sum(len(v) for v in jobstoclean.values())} jobs")
        for proxyid, jobs in jobstoclean.items():
            conn = None
            try:
                context = self.getProxySSLContext(proxyid)
                conn = http.client.HTTPSConnection(url.netloc, context=context)  # TODO: timeout
                self.log.debug(f"Connected to cluster {url.netloc}")
                self.cleanJobs(conn, jobs)
            except ssl.SSLError as e:
                self.log.error(f"Could not create SSL for proxyid {proxyid}: {e}")
                continue
            except http.client.HTTPException as e:
                self.log.error(f"Could not connect to cluster {url.netloc}: {e}")
                continue
            finally:
                if conn:
                    conn.close()

        self.log.debug("Finished cleaning jobs")

    def process(self):

        # clean jobs
        self.processToClean()

    def cleanJobs(self, conn, jobs):
        toclean = []
        for (_, _, job, _) in jobs:
            jobid = job.JobID.split('/')[-1]
            toclean.append({"id": jobid})

        jsonData = {}
        if len(toclean) == 1:
            jsonData["job"] = toclean[0]
        else:
            jsonData["job"] = toclean

        import pprint
        pp = pprint.PrettyPrinter()
        self.log.debug(f"Prepared JSON data for job clean operation:\n{pp.pformat(jsonData)}")

        try:
            conn.request(
                "POST",
                "/arex/rest/1.0/jobs?action=clean",
                body=json.dumps(jsonData),
                headers={"Accept": "application/json", "Content-type": "application/json"}
            )
            resp = conn.getresponse()
            self.log.debug(f"Response status: {resp.status}")
            jsonData = json.loads(resp.read().decode())
        except json.JSONDecodeError as e:
            self.log.error(f"Could not parse JSON response: {e}")
            return
        except http.client.HTTPException as e:
            self.log.error(f"POST /jobs?action=clean error: {e}")
            return

        self.log.debug(f"JSON response:\n{pp.pformat(jsonData)}")

        if isinstance(jsonData["job"], dict):
            results = [jsonData["job"]]
        else:
            results = jsonData["job"]

        for job, (id, appjobid, _, _) in zip(results, jobs):
            if job["status-code"] != "202":
                self.log.error(f"{appjobid}: Could not clean job {job['id']}: {job['reason']}")
            self.db.deleteArcJob(id)
            self.log.debug(f"Deleted arcjob {id}")


if __name__ == '__main__':
    st=aCTCleaner()
    st.run()
    st.finish()
