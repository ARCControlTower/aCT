import os
from act.atlas.aCTATLASProcess import aCTATLASProcess
from act.atlas.aCTPanda2ClassAd import aCTPanda2ClassAd


class aCTPanda2Condor(aCTATLASProcess):
    '''
    Take new jobs in Panda table and insert then into the condorjobs table.
    '''

    def __init__(self):
        aCTATLASProcess.__init__(self, ceflavour=['HTCONDOR-CE', 'CREAM-CE'])

    def createCondorJobs(self):

        jobs = self.dbpanda.getJobs("condorjobid is NULL and siteName in %s limit 10000" % self.sitesselect)
        proxies_map = {}

        for job in jobs:

            if job['proxyid'] not in proxies_map:
                proxies_map[job['proxyid']] = self.dbarc.getProxyPath(job['proxyid'])

            parser = aCTPanda2ClassAd(job['pandajob'], job['id'], job['siteName'], self.sites[job['siteName']], proxies_map[job['proxyid']],
                                   self.tmpdir, self.conf, job['metadata'].decode(), self.log)

            self.log.info("site %s maxwalltime %s", job['siteName'],self.sites[job['siteName']]['maxwalltime'] )

            parser.parse()

            try:
                classad = parser.getClassAd()
            except:
                pass
            if classad is not None:
                endpoints = ','.join(self.sites[job['siteName']]['endpoints'])
                self.log.info("Inserting job %d with clusterlist %s" % (job['pandaid'], endpoints))
                self.log.debug("%d: classad: %s" % (job['pandaid'], classad))
                maxattempts = 0 # Never resubmit condor jobs

                aid = self.dbcondor.insertCondorJobDescription(classad, maxattempts=maxattempts, clusterlist=endpoints,
                                                               proxyid=job['proxyid'], appjobid=str(job['pandaid']),
                                                               fairshare=job['siteName'])
                if not aid:
                    self.log.error("%d: Failed to insert condor job description: %s" % (job['pandaid'], classad))
                    continue

                jd = {}
                jd['condorjobid'] = aid['LAST_INSERT_ID()']
                jd['pandastatus'] = 'starting'
                # make sure actpandastatus is really 'sent', in case of resubmitting
                jd['actpandastatus'] = 'sent'
                jd['corecount'] = int(classad['+xcount'])
                self.dbpanda.updateJob(job['pandaid'], jd)

                # Dump description for APFMon
                if self.conf.monitor.apfmon:
                    logdir = os.path.join(self.conf.joblog.dir,
                                          job['created'].strftime('%Y-%m-%d'),
                                          job['siteName'])
                    try: os.makedirs(logdir, 0o755)
                    except: pass
                    jdlfile = os.path.join(logdir, '%s.jdl' % job['pandaid'])
                    with open(jdlfile, 'w') as f:
                        f.write('\n'.join(['%s = %s' % (k,v) for (k,v) in classad.items()]))
                        self.log.debug('Wrote description to %s' % jdlfile)

    def process(self):
        self.setSites()
        self.createCondorJobs()

if __name__ == '__main__':

    am = aCTPanda2Condor()
    am.run()
    am.finish()
