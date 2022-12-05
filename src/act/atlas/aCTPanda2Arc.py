import http.client
import json
import os
import traceback

from act.atlas.aCTATLASProcess import aCTATLASProcess
from act.atlas.aCTPanda2Xrsl import aCTPanda2Xrsl


class aCTPanda2Arc(aCTATLASProcess):
    '''
    Take new jobs in Panda table and insert then into the arcjobs table.
    '''

    def createArcJobs(self):
        """
        Insert new jobs from pandajobs to arcjobs.

        Signal handling strategy:
        - signals are deferred per job because arcjob insertion is not lazy
        - TODO: review: Assume that arcjobs insertion, pandajobs update and
                APFMon desc dump need to be atomic because desc dump cannot be
                rolled back and it might not be cleaned if job is not properly
                inserted to arcjobs and updated in dbpanda.
        """
        jobs = self.dbpanda.getJobs("arcjobid is NULL and actpandastatus in ('sent', 'starting') and siteName in %s limit 10000" % self.sitesselect)
        proxies_map = {}

        for job in jobs:

            if job['proxyid'] not in proxies_map:
                proxies_map[job['proxyid']] = self.dbarc.getProxyPath(job['proxyid'])

            parser = aCTPanda2Xrsl(job, self.sites[job['siteName']], self.tmpdir, self.conf, self.log)

            self.log.info("site %s maxwalltime %s", job['siteName'],self.sites[job['siteName']]['maxwalltime'] )

            try:
                parser.parse()
            except Exception as e:
                # try again later
                self.log.error('%s: Cant handle job description: %s' % (job['pandaid'], str(e)))
                self.log.error(traceback.format_exc())
                continue
            self.sendTraces(parser.traces, proxies_map[job['proxyid']])
            try:
                xrsl = parser.getXrsl()
            except:
                pass
            if xrsl is not None:
                endpoints = self.sites[job['siteName']]['endpoints']
                if not endpoints: # No CEs, try later
                    self.log.warning("%d: Cannot submit to %s because no CEs available" % (job['pandaid'], job['siteName']))
                    continue
                cl = []
                for e in endpoints:
                    if e.find('://') == -1:
                        # gsiftp is default if not specified
                        e = 'gsiftp://' + e
                    cl.append(e)
                cls = ",".join(cl)
                self.log.info("Inserting job %i with clusterlist %s" % (job['pandaid'], cls))
                maxattempts = 5
                if self.sites[job['siteName']]['truepilot']:
                    # truepilot jobs should never be resubmitted
                    maxattempts = 0

                # Set the list of files to download at the end of the job
                # new syntax for rest
                downloadfiles = 'diagnose=gmlog/errors'
                try:
                    downloadfiles += ';%s' % parser.jobdesc['logFile'][0].replace('.tgz', '')
                except:
                    pass
                if not self.sites[job['siteName']]['truepilot']:
                    downloadfiles += ';heartbeat.json'

                # exit handling context manager
                with self.sigdefer:

                    aid = self.dbarc.insertArcJobDescription(xrsl, maxattempts=maxattempts, clusterlist=cls,
                                                            proxyid=job['proxyid'], appjobid=str(job['pandaid']),
                                                            downloadfiles=downloadfiles, fairshare=job['siteName'])
                    if not aid:
                        self.log.error("%s: Failed to insert arc job description: %s" % (job['pandaid'], xrsl))
                        continue

                    jd = {}
                    jd['arcjobid'] = aid['LAST_INSERT_ID()']
                    jd['pandastatus'] = 'starting'
                    # make sure actpandastatus is really 'sent', in case of resubmitting
                    jd['actpandastatus'] = 'sent'
                    self.dbpanda.updateJob(job['pandaid'], jd)

                    # Dump description for APFMon
                    if self.conf.monitor.apfmon:
                        logdir = os.path.join(self.conf.joblog.dir,
                                            job['created'].strftime('%Y-%m-%d'),
                                            job['siteName'])
                        os.makedirs(logdir, 0o755, exist_ok=True)
                        jdlfile = os.path.join(logdir, '%s.jdl' % job['pandaid'])
                        with open(jdlfile, 'w') as f:
                            self.log.debug('Wrote description to %s' % jdlfile)
                            f.write(xrsl)

    def process(self):
        self.setSites()
        self.createArcJobs()

    def sendTraces(self, traces, proxypath):
        for trace in traces:
            try:
                conn = http.client.HTTPSConnection('rucio-lb-prod.cern.ch:443', key_file=proxypath, cert_file=proxypath, timeout=5)
                rdata = json.dumps(trace)
                headers = {"Content-type": "application/json"}
                conn.request("POST", "/traces/", rdata, headers)
                resp = conn.getresponse()
                status = resp.status
                if status != 201:
                    self.log.error("Error sending trace: %s : %s" % (resp.status, resp.reason))
                conn.close()
            except Exception as error:
                self.log.error("Error sending trace: %s" % error)
