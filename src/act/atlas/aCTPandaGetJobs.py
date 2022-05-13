from threading import Thread
import re
import time
import random
import arc
from act.atlas import aCTPanda
from act.common import aCTProxy
from act.atlas.aCTATLASProcess import aCTATLASProcess


class PandaGetThr(Thread):
    """
    Helper function for getting panda jobs
    """
    def __init__ (self, func, siteName, prodSourceLabel=None, getEventRanges=True, push=True):
        Thread.__init__(self)
        self.func = func
        self.siteName = siteName
        self.prodSourceLabel = prodSourceLabel
        self.getEventRanges = getEventRanges
        self.push = push
        self.result = (None, None, None, None)
    def run(self):
        if not self.push:
            self.result = (0, '', None, self.prodSourceLabel)
        else:
            self.result = self.func(self.siteName, self.prodSourceLabel, self.getEventRanges)


class aCTPandaGetJobs(aCTATLASProcess):

    """
    Main class for Panda interaction. Three major functions: init, run, finish
    """

    def __init__(self):
        aCTATLASProcess.__init__(self)

        # Get DN from configured proxy file
        uc = arc.UserConfig()
        uc.ProxyPath(self.arcconf.voms.proxypath)
        cred = arc.Credential(uc)
        dn = cred.GetIdentityName()
        self.log.info("Running under DN %s" % dn)
        # Keep a panda object per proxy. The site "type" maps to a specific
        # proxy role
        self.pandas = {}
        # Map the site type to a proxy id in proxies table
        # In future for analysis the id will change once the job is picked up
        self.proxymap = {}

        actp = aCTProxy.aCTProxy(self.log)
        for role in self.arcconf.voms.roles:
            attr = '/atlas/Role='+role
            proxyid = actp.getProxyId(dn, attr)
            if not proxyid:
                raise Exception("Proxy with DN "+dn+" and attribute "+attr+" was not found in proxies table")

            proxyfile = actp.path(dn, attribute=attr)
            # pilot role is mapped to analysis type
            psl = 'managed'
            if role == 'pilot':
                role = 'analysis'
                psl = 'user'
                self.proxymap['panda'] = proxyid
            self.pandas[role] = aCTPanda.aCTPanda(self.log, proxyfile)
            self.proxymap[psl] = proxyid

        # queue interval
        self.queuestamp=0

        # Register this aCT to APFMon
        self.apfmon.registerFactory()
        # CRIC queue info
        self.sites={}
        # Panda info on activated jobs: {queue: {'rc_test': 2, 'rest': 40}}
        self.activated = {}
        # Flag for calling getJob no matter what to have a constant stream
        self.getjob = False


    def setSites(self):
        self.sites = self.cricparser.getSites()

    def getActivated(self):
        """
        Get the number of activated jobs for each served queue
        """
        # If panda query fails safer to assume all queues have jobs
        self.activated.clear()
        # Assume any proxy is ok to query panda
        queueinfo = next(iter(self.pandas.values())).getQueueStatus()
        if queueinfo:
            for site in [k for k,v in self.sites.items() if v['enabled']]:
                if site not in queueinfo:
                    self.log.debug("%s: no jobs" % site)
                    self.activated[site] = {'rc_test': 0, 'rest': 0}
                    continue
                n_rc_test = 0
                n_rest = 0
                for label, jobs in queueinfo[site].items():
                    if 'activated' in jobs:
                        if label == 'rc_test':
                            n_rc_test += jobs['activated']
                        else:
                            n_rest += jobs['activated']

                self.activated[site] = {'rc_test': n_rc_test, 'rest': n_rest}
                self.log.debug('%s: activated rc_test %d, rest %d' % (site, n_rc_test, n_rest))

    def getPanda(self, sitename):
        return self.pandas.get(self.sites[sitename]['type'], self.pandas.get('production'))


    def getJobs(self, num):
        """
        Get at most num panda jobs from panda server. Store fetched jobs in database.
        """

        if num == 0:
            return 0

        if self.getjob:
            num = 1

        count=0

        for site, attrs in self.sites.items():
            if not attrs['enabled']:
                continue

            if attrs['status'] == 'offline':
                continue

            if attrs['maxjobs'] == 0:
                continue

            if (not self.getjob) and site in self.activated and sum([x for x in self.activated[site].values()]) == 0:
                self.log.info("Site %s: No activated jobs" % site)
                continue

            prodsourcelabel = None
            if attrs['status'] == 'test' and attrs['type'] in ['production', 'unified']:
                self.log.info("Site %s is in test, will set prodSourceLabel to prod_test" % site)
                prodsourcelabel = 'prod_test'
            elif attrs['type'] == 'unified':
                prodsourcelabel = 'unified'

            # Get number of jobs injected into ARC but not yet submitted
            nsubmitting = self.dbpanda.getNJobs("actpandastatus='sent' and siteName='%s'" % site )

            # Get total number of active jobs
            nall = self.dbpanda.getNJobs("siteName='%s' and actpandastatus!='done' \
                                          and actpandastatus!='donefailed' and actpandastatus!='donecancelled'" % site)
            self.log.info("Site %s: %i jobs in sent, %i total" % (site, nsubmitting, nall))

            # Limit number of jobs waiting submission to avoid getting too many
            # jobs from Panda
            if nsubmitting > self.conf.panda.minjobs:
                self.log.info("Site %s: at limit of sent jobs" % site)
                continue

            if nall >= self.sites[site]['maxjobs']:
                self.log.info("Site %s: at or above max job limit of %d" % (site, self.sites[site]['maxjobs']))
                continue

            nthreads = min(self.conf.panda.threads, self.sites[site]['maxjobs'] - nall)
            if self.getjob:
                nthreads = 1

            # if no jobs available
            stopflag=False

            #getEventRanges = not attrs['truepilot']
            getEventRanges = site in ['LRZ-LMU_MUC_MCORE1', 'BOINC-ES', 'IN2P3-CC_HPC_IDRIS_MCORE', 'praguelcg2_IT4I_MCORE']
            apfmonjobs = []

            for nc in range(0, max(num//nthreads, 1)):
                if stopflag:
                    continue

                tlist = []

                for i in range(0, nthreads):
                    r = random.Random()
                    if site in []:
                        t = PandaGetThr(self.getPanda(site).getJob, site, prodSourceLabel='ptest', getEventRanges=getEventRanges)
                    elif r.randint(0,100) <= 2:
                        if (not self.getjob) and site in self.activated and self.activated[site]['rc_test'] == 0:
                            self.log.debug('%s: No rc_test activated jobs' % site)
                            #t = PandaGetThr(self.getPanda(site).getJob, site, prodSourceLabel='ptest', getEventRanges=getEventRanges)
                            continue
                        else:
                            t = PandaGetThr(self.getPanda(site).getJob, site, prodSourceLabel='rc_test', getEventRanges=getEventRanges, push=attrs['push'])
                    else:
                        if (not self.getjob) and site in self.activated and self.activated[site]['rest'] == 0:
                            self.log.debug('%s: No activated jobs' % site)
                            continue
                        elif attrs['type'] == "analysis":
                            t = PandaGetThr(self.getPanda(site).getJob, site, prodSourceLabel='user', getEventRanges=getEventRanges, push=attrs['push'])
                        else:
                            t = PandaGetThr(self.getPanda(site).getJob, site, prodSourceLabel=prodsourcelabel, getEventRanges=getEventRanges, push=attrs['push'])
                    tlist.append(t)
                    t.start()
                    nall += 1
                    if nall >= self.sites[site]['maxjobs']:
                        self.log.info("Site %s: reached max job limit of %d" % (site, self.sites[site]['maxjobs']))
                        stopflag = True
                        break

                activatedjobs = False
                for t in tlist:
                    t.join()
                    (pandaid, pandajob, eventranges, prodsrclabel) = t.result
                    if pandaid == -1: # No jobs available
                        continue
                    activatedjobs = True
                    if pandaid == None: # connection error
                        stopflag = True
                        continue

                    n = {}
                    # Check eventranges is defined for ES jobs
                    if re.search('eventService=True', pandajob) and getEventRanges and (eventranges is None or eventranges == '[]'):
                        self.log.warning('%s: No event ranges given by panda' % pandaid)
                        n['pandastatus'] = 'closed'
                        n['actpandastatus'] = 'finished'
                        # Assumes only ARC sites (not condor) pre-fetch events
                        n['arcjobid'] = -1 # dummy id so job is not submitted
                    else:
                        n['pandastatus'] = 'sent'
                        n['actpandastatus'] = 'sent'
                    n['siteName'] = site
                    n['proxyid'] = self.proxymap.get(prodsrclabel, self.proxymap.get('managed'))
                    n['prodSourceLabel'] = prodsrclabel
                    n['eventranges'] = eventranges
                    if pandaid != 0:
                        try:
                            n['corecount'] = int(re.search(r'coreCount=(\d+)', pandajob).group(1))
                        except:
                            self.log.warning('%s: no corecount in job description' % pandaid)
                    n['sendhb'] = attrs['push']
                    if pandaid == 0:
                        # Pull mode: set dummy arcjobid and condorjobid to avoid
                        # job getting picked up before setting proper job desc after insertion
                        n['arcjobid'] = -1
                        n['condorjobid'] = -1
                    rowid = self.dbpanda.insertJob(pandaid, pandajob, n)['LAST_INSERT_ID()']
                    if pandaid == 0:
                        # Pull mode: use row id as job id for output files
                        pandaid = rowid
                        pandajob = 'PandaID=%d&prodSourceLabel=%s' % (pandaid, prodsrclabel)
                        self.dbpanda.updateJobs('id=%d' % pandaid, {'pandaid': pandaid, 'pandajob': pandajob, 'arcjobid': None, 'condorjobid': None})
                    apfmonjobs.append((rowid, pandaid))
                    count += 1

                if not activatedjobs:
                    if site in self.activated:
                        self.activated[site] = {'rest': 0, 'rc_test': 0}
                    stopflag = True

            self.apfmon.registerJobs(apfmonjobs, site)

        return count


    def process(self):
        """
        Method called from loop
        """
        self.setSites()
        if not self.activated or time.time() - self.starttime > 300:
            self.getActivated()
            self.starttime = time.time()
            self.getjob = True
            # Each 5 mins send the list of queues with maxjobs>0 to APFmon
            self.apfmon.registerLabels([k for (k,v) in self.sites.items() if v['maxjobs'] > 0])

        # request new jobs
        num = self.getJobs(int(self.conf.panda.getjobs))
        if num:
            self.log.info("Got %i jobs" % num)
        self.getjob = False

if __name__ == '__main__':
    am=aCTPandaGetJobs()
    am.run()
    #am.finish()
