import time
from threading import Thread

import arc
from act.atlas import aCTPanda
from act.atlas.aCTATLASProcess import aCTATLASProcess
from act.common import aCTProxy, aCTUtils


class PandaThr(Thread):
    """
    Helper function for threaded panda status update calls.
    func is generic, but it is only used for aCTPanda.updateStatus call.
    """

    def __init__ (self,func,id,status,args={}):
        Thread.__init__(self)
        self.func=func
        self.id = id
        self.status = status
        self.args = args
        self.result = None
    def run(self):
        self.result=self.func(self.id,self.status,self.args)


class aCTAutopilotSent(aCTATLASProcess):

    """
    Main class for Panda interaction. Three major functions: init, run, finish
    """

    def setup(self):
        super().setup()

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
            if role == 'pilot':
                role = 'analysis'
            self.pandas[role] = aCTPanda.aCTPanda(self.log, proxyfile)
            self.proxymap[role] = proxyid

        # queue interval
        self.queuestamp=0

        self.sites={}


    def setSites(self):
        self.sites = self.cricparser.getSites()


    def getPanda(self, sitename):
        return self.pandas.get(self.sites[sitename]['type'], self.pandas.get('production'))


    def updatePandaHeartbeat(self,pstatus):
        """
        Heartbeat status updates.

        Signal handling strategy:
        - no exit checking is performed because this is done automatically by
          parent aCTProcess and this is the only operation executed in the process loop
        """
        nthreads=self.conf.panda.threads
        columns = ['pandaid', 'siteName', 'startTime', 'computingElement', 'node', 'corecount']
        jobs=self.dbpanda.getJobs("pandastatus='"+pstatus+"' and sendhb=1 and ("+self.dbpanda.timeStampLessThan("theartbeat", self.conf.panda.heartbeattime)+" or modified > theartbeat) limit 1000", columns)
        if not jobs:
            return

        self.log.info("Update heartbeat for %d jobs in state %s (%s)" % (len(jobs), pstatus, ','.join([str(j['pandaid']) for j in jobs])))

        changed_pstatus = False
        if pstatus == 'sent':
            pstatus = 'starting'
            changed_pstatus = True

        tlist=[]
        for j in jobs:
            jd = {}
            if pstatus != 'starting':
                jd['startTime'] = j['startTime']
            if j['computingElement']:
                if j['computingElement'].find('://') != -1: # this if is only needed during transition period
                    jd['computingElement'] = arc.URL(str(j['computingElement'])).Host()
                else:
                    jd['computingElement'] = j['computingElement']
            jd['node'] = j['node']
            jd['siteName'] = j['siteName']
            # For starting truepilot jobs send pilotID with expected log
            # location so logs are available in case of lost heartbeat
            if pstatus == 'starting' and not changed_pstatus and self.sites[j['siteName']]['truepilot']:
                date = time.strftime('%Y-%m-%d', time.gmtime())
                logurl = '/'.join([self.conf.joblog.urlprefix, date, j['siteName'], '%s.out' % j['pandaid']])
                jd['pilotID'] = '%s|Unknown|Unknown|Unknown|Unknown' % logurl
            try:
                jd['jobMetrics']="coreCount=%s" % (j['corecount'] if j['corecount'] > 0 else self.sites[j['siteName']]['corecount'])
            except:
                pass
            t=PandaThr(self.getPanda(j['siteName']).updateStatus,j['pandaid'],pstatus,jd)
            tlist.append(t)

        aCTUtils.RunThreadsSplit(tlist,nthreads)

        for t in tlist:
            if t.result == None or 'StatusCode' not in t.result:
                # Strange response from panda, try later
                continue
            if t.result['StatusCode'] and t.result['StatusCode'][0] == '60':
                self.log.error('Failed to contact Panda, proxy may have expired')
                continue
            #self.log.debug('%s: %s' % (t.id, t.result))
            if 'command' in t.result  and t.result['command'][0] != "NULL":
                self.log.info("%s: response: %s" % (t.id,t.result) )
            jd={}
            if changed_pstatus:
                jd['pandastatus']=pstatus
            # Make sure heartbeat is ahead of modified time so it is not picked up again
            if self.sites[t.args['siteName']]['truepilot'] and pstatus == 'starting':
                # Set theartbeat 1h in the future to allow job to start
                # running and avoid race conditions with heartbeats
                # Now heartbeat timeout is 2h so we remove the offset
                #jd['theartbeat'] = self.dbpanda.getTimeStamp(time.time()+3600)
                jd['theartbeat'] = self.dbpanda.getTimeStamp(time.time()+1)
            else:
                jd['theartbeat'] = self.dbpanda.getTimeStamp(time.time()+1)
            # If panda tells us to kill the job, set actpandastatus to tobekilled
            # and remove from heartbeats
            if 'command' in t.result and ( ("tobekilled" in t.result['command'][0]) or ("badattemptnr" in t.result['command'][0]) ):
                self.log.info('%s: cancelled by panda' % t.id)
                jd['actpandastatus']="tobekilled"
                jd['pandastatus']=None
            self.dbpanda.updateJob(t.id,jd)

        self.log.info("Threads finished")


    def process(self):
        """
        Method called from loop
        """
        self.setSites()

        # Update all jobs currently in the system
        self.updatePandaHeartbeat('sent')
