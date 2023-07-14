import datetime
import os
import shutil
import time
from threading import Thread
from urllib.parse import parse_qs

import arc
from act.atlas import aCTPanda
from act.atlas.aCTATLASProcess import aCTATLASProcess
from act.atlas.aCTPandaJob import aCTPandaJob
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


class PandaBulkThr(Thread):
    """
    Bulk update pandaStatus
    """

    def __init__ (self,func,ids,args):
        Thread.__init__(self)
        self.func=func
        self.ids = ids
        self.args = args
        self.result = None
    def run(self):
        self.result=self.func(self.args)


class aCTAutopilot(aCTATLASProcess):
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
        self.log.info(f"Running under DN {dn}")
        # Keep a panda object per proxy. The site "type" maps to a specific
        # proxy role
        self.pandas = {}
        # Map the site type to a proxy id in proxies table
        # In future for analysis the id will change once the job is picked up
        self.proxymap = {}

        actp = aCTProxy.aCTProxy(self.log)
        for role in self.arcconf.voms.roles:
            attr = f'/atlas/Role={role}'
            proxyid = actp.getProxyId(dn, attr)
            if not proxyid:
                raise Exception(f"Proxy with DN {dn} and attribute {attr} was not found in proxies table")

            proxyfile = actp.path(dn, attribute=attr)
            # pilot role is mapped to analysis type
            if role == 'pilot':
                role = 'analysis'
            self.pandas[role] = aCTPanda.aCTPanda(self.log, proxyfile)
            self.proxymap[role] = proxyid

        # queue interval
        self.queuestamp=0
        self.nthreads=self.conf.panda.threads

        self.sites={}


    def setSites(self):
        self.sites = self.cricparser.getSites()


    def getPanda(self, sitename):
        return self.pandas.get(self.sites[sitename]['type'], self.pandas.get('production'))


    def updatePandaHeartbeat(self,pstatus):
        """
        Heartbeat status updates.

        Signal handling strategy:
        - exit is checked before doing anything
        - everything is executed without checking for exit to do heartbeats
          and their result processing uninterrupted
        """
        self.stopOnFlag()

        columns = ['pandaid', 'siteName', 'startTime', 'computingElement', 'node', 'corecount']
        # TODO: HARDCODED limit
        jobs = self.dbpanda.getJobs(f"pandastatus='{pstatus}' and sendhb=1 and ({self.dbpanda.timeStampLessThan('theartbeat', self.conf.panda.heartbeattime)} or modified > theartbeat) limit 1000", columns)
        if not jobs:
            return

        idstr = ",".join([str(job['pandaid']) for job in jobs])
        self.log.info(f"Update heartbeat for {len(jobs)} jobs in state {pstatus} ({idstr})")

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
                logurl = '/'.join([self.conf.joblog.urlprefix, date, j['siteName'], f"{j['pandaid']}.out"])
                jd['pilotID'] = f'{logurl}|Unknown|Unknown|Unknown|Unknown'
            try:
                corecount = int(j['corecount']) if j['corecount'] > 0 else self.sites[j['siteName']]['corecount']
                jd['jobMetrics'] = f"coreCount={corecount}"
                jd['coreCount'] = corecount
            except:
                self.log.warning(f"appjob({j['pandaid']}): no corecount available")
            t=PandaThr(self.getPanda(j['siteName']).updateStatus,j['pandaid'],pstatus,jd)
            tlist.append(t)

        aCTUtils.RunThreadsSplit(tlist, self.nthreads)

        for t in tlist:
            if t.result == None or 'StatusCode' not in t.result:
                # Strange response from panda, try later
                continue
            if t.result['StatusCode'] and t.result['StatusCode'][0] == '60':
                self.log.error('Failed to contact Panda, proxy may have expired')
                continue
            if 'command' in t.result  and t.result['command'][0] != "NULL":
                self.log.info(f"{t.id}: response: {t.result}")
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
                self.log.info(f'{t.id}: cancelled by panda')
                jd['actpandastatus']="tobekilled"
                jd['pandastatus']=None
            self.dbpanda.updateJob(t.id,jd)

        self.log.info("Threads finished")


    def updatePandaHeartbeatBulk(self,pstatus):
        """
        Heartbeat status updates in bulk.

        Signal handling strategy:
        - exit is checked before doing anything
        - everything is executed without checking for exit to do heartbeats
          and their result processing uninterrupted
        """
        self.stopOnFlag()

        columns = ['pandaid', 'siteName', 'startTime', 'computingElement', 'node', 'corecount']
        # TODO: HARDCODED limit
        jobs=self.dbpanda.getJobs(f"pandastatus='{pstatus}' and sendhb=1 and ({self.dbpanda.timeStampLessThan('theartbeat', self.conf.panda.heartbeattime)} or modified > theartbeat) limit 1000", columns)
        #jobs=self.dbpanda.getJobs("pandastatus='"+pstatus+"' and sendhb=1 and ("+self.dbpanda.timeStampLessThan("theartbeat", 60)+" or modified > theartbeat) limit 1000", columns)
        if not jobs:
            return

        idstr = ",".join([str(job["pandaid"]) for job in jobs])
        self.log.info(f"Update heartbeat for {len(jobs)} jobs in state {pstatus} ({idstr})")

        changed_pstatus = False
        if pstatus == 'sent':
            pstatus = 'starting'
            changed_pstatus = True

        tlist=[]
        jobsbyproxy = {}
        for j in jobs:
            jd = {'jobId': j['pandaid'], 'state': pstatus}
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
                logurl = '/'.join([self.conf.joblog.urlprefix, date, j['siteName'], f"{j['pandaid']}.out"])
                jd['pilotID'] = f'{logurl}|Unknown|Unknown|Unknown|Unknown'
            try:
                corecount = int(j['corecount']) if j['corecount'] > 0 else self.sites[j['siteName']]['corecount']
                jd['jobMetrics'] = f"coreCount={corecount}"
                jd['coreCount'] = corecount
            except:
                self.log.warning(f"appjob({j['pandaid']}): no corecount available")

            try:
                jobsbyproxy[self.sites[j['siteName']]['type']].append(jd)
            except:
                jobsbyproxy[self.sites[j['siteName']]['type']] = [jd]

        for sitetype, jobs in jobsbyproxy.items():
            t = PandaBulkThr(self.pandas.get(sitetype, self.pandas.get('production')).updateStatuses, [j['jobId'] for j in jobs], jobs)
            tlist.append(t)

        aCTUtils.RunThreadsSplit(tlist, self.nthreads)

        for t in tlist:
            if not t or not t.result or not t.result[0]:
                # Strange response from panda, try later
                continue

            for pandaid, response in zip(t.ids, t.result[1]):
                try:
                    result = parse_qs(response)
                except Exception:
                    self.log.error(f'Could not parse result from panda: {response}')
                    continue

                if not result.get('StatusCode'):
                    # Strange response from panda, try later
                    continue
                if result['StatusCode'][0] == '60':
                    self.log.error('Failed to contact Panda, proxy may have expired')
                    continue
                if result.get('command', [''])[0] not in ['', "NULL"]:
                    self.log.info(f"appjob({pandaid}): response: {result}")
                jd = {}
                if changed_pstatus:
                    jd['pandastatus'] = pstatus
                # Make sure heartbeat is ahead of modified time so it is not picked up again
                jd['theartbeat'] = self.dbpanda.getTimeStamp(time.time()+1)
                # If panda tells us to kill the job, set actpandastatus to tobekilled
                # and remove from heartbeats
                if result.get('command', [''])[0] in ["tobekilled", "badattemptnr", "alreadydone"]:
                    self.log.info(f'appjob({pandaid}): cancelled by panda')
                    jd['actpandastatus'] = "tobekilled"
                    jd['pandastatus'] = None
                self.dbpanda.updateJob(pandaid, jd)

        self.log.info("Threads finished")


    def updatePandaFinishedPilot(self):
        """
        Final status update for completed jobs (finished or failed in athena)
        and cancelled jobs

        Signal handling strategy:
        - exit is checked before doing anything
        - everything is executed without checking for exit to do heartbeats
          and their result processing uninterrupted
        """
        self.stopOnFlag()

        # TODO: HARDCODED limit
        jobs=self.dbpanda.getJobs("actpandastatus='finished' or actpandastatus='failed' or actpandastatus='cancelled' limit 1000")

        if not jobs:
            return

        idstr = ",".join([str(job['pandaid']) for job in jobs])
        self.log.info(f"Updating panda for {len(jobs)} finished jobs ({idstr})")

        tlist = []
        for j in jobs:
            # If true pilot skip heartbeat and just update DB
            if not j['sendhb']:
                jd={}
                jd['pandastatus']=None
                jd['actpandastatus']='done'
                if j['actpandastatus'] == 'failed':
                    jd['actpandastatus']='donefailed'
                if j['actpandastatus'] == 'cancelled':
                    jd['actpandastatus']='donecancelled'
                if not j['startTime']:
                    jd['startTime'] = datetime.datetime.utcnow()
                if not j['endTime']:
                    jd['endTime'] = datetime.datetime.utcnow()
                self.dbpanda.updateJob(j['pandaid'], jd)
                continue

            # Cancelled jobs have no heartbeat info
            if j['actpandastatus'] == 'cancelled':
                jobinfo = aCTPandaJob(jobinfo = {'jobId': j['pandaid'], 'state': 'failed'})
                jobinfo.pilotErrorCode = 1144
                jobinfo.pilotErrorDiag = "This job was killed by panda server"
                jobinfo.startTime = j['startTime'] if j['startTime'] else datetime.datetime.utcnow()
                jobinfo.endTime = j['endTime'] if j['endTime'] else datetime.datetime.utcnow()
            else:
                try:
                    # Load heartbeat information from pilot
                    fname = os.path.join(self.tmpdir, "heartbeats", f"{j['pandaid']}.json")
                    jobinfo = aCTPandaJob(filename=fname)
                except Exception as x:
                    self.log.error(f"appjob({j['pandaid']}): {x}")
                    # Send some basic info back to panda
                    info = {'jobId': j['pandaid'], 'state': j['pandastatus']}
                    jobinfo = aCTPandaJob(jobinfo=info)
                    jobinfo.errorCode = 9000
                    jobinfo.errorDiag = 'Job failed for unknown reason'
                else:
                    os.remove(fname)

            self.log.debug(f"appjob({j['pandaid']}): final heartbeat: {jobinfo.dictionary()}")
            t=PandaThr(self.getPanda(j['siteName']).updateStatus,j['pandaid'],j['pandastatus'],jobinfo.dictionary())
            tlist.append(t)

        aCTUtils.RunThreadsSplit(tlist, self.nthreads)

        tstamp = self.dbpanda.getTimeStamp()
        for t in tlist:
            if t.result == None:
                continue
            if 'StatusCode' in t.result and t.result['StatusCode'] and t.result['StatusCode'][0] != '0':
                self.log.error('Error updating panda')
                continue
            jd={}
            jd['pandastatus']=None
            jd['actpandastatus']='done'
            if t.status == 'failed':
                jd['actpandastatus']='donefailed'
            if 'pilotErrorCode' in t.args and t.args['pilotErrorCode'] == 1144:
                jd['actpandastatus']='donecancelled'
            jd['theartbeat'] = tstamp

            self.dbpanda.updateJob(t.id,jd)
            # Send done message to APFMon
            self.apfmon.updateJob(t.id, 'done' if jd['actpandastatus'] == 'done' else 'fault')

        self.dbpanda.Commit()
        self.log.info("Threads finished")

        # Clean inputfiles
        for j in jobs:
            pandainputdir = os.path.join(self.tmpdir, 'inputfiles', str(j['pandaid']))
            shutil.rmtree(pandainputdir, ignore_errors=True)

    def checkJobs(self):
        """
        Sanity checks when restarting aCT. Check for nonexistent jobs... TODO

        Signal handling strategy:
        - exit is checked before every job update
        """
        # Does it matter which proxy is used? Assume no
        panda = next(iter(self.pandas.values()))
        pjobs = panda.queryJobInfo()

        # panda error if [] possible
        if len(pjobs) == 0:
            self.log.info('No panda jobs found')
            return

        pjids=[]
        for j in pjobs:
            if j['jobStatus'] == 'sent' or j['jobStatus'] == 'running' or j['jobStatus'] == 'transferring' or j['jobStatus'] == 'starting' :
                pjids.append(j['PandaID'])
        self.log.info(f"{len(pjids)}")

        # try to recover lost jobs (jobs in aCT but not in Panda)

        jobs=self.dbpanda.getJobs("pandastatus like '%'")

        for j in jobs:
            self.stopOnFlag()
            self.log.info(f"appjob({j['pandaid']})")
            if j['pandaid'] in pjids:
                pass
            else:
                self.log.info(f"appjob({j['pandaid']}) not in panda, cancel and remove from aCT")
                jd={}
                jd['pandastatus'] = None
                jd['actpandastatus']='tobekilled'
                self.dbpanda.updateJob(j['pandaid'],jd)

        # check db for jobs in Panda but not in aCT
        count=0
        for j in pjobs:
            self.stopOnFlag()
            self.log.info(f"checking panda job pandaid({j['PandaID']})")
            job=self.dbpanda.getJob(j['PandaID'])
            if job is None and ( j['pandastatus'] == 'running' or j['pandastatus'] == 'transferring' or j['pandastatus'] == 'starting') :
                self.log.info(f"Missing: pandaid({j['PandaID']})")
                count+=1
                panda.updateStatus(j['PandaID'],'failed')
        self.log.info(f"missing jobs: {count} removed")


    def updateArchive(self):
        """
        Move old jobs older than 1 day to archive table.

        Signal handling strategy:
        - exit is checked before every job update
        """
        # modified column is reported in local time so may not be exactly one day
        select = self.dbpanda.timeStampLessThan('modified', 60*60*24)
        select += ' and (actpandastatus="done" or actpandastatus="donefailed" or actpandastatus="donecancelled")'
        columns = ['pandaid', 'sitename', 'actpandastatus', 'starttime', 'endtime', 'modified']
        jobs = self.dbpanda.getJobs(select, columns)
        if not jobs:
            return

        self.log.info(f'Archiving {len(jobs)} jobs')
        tstamp = self.dbpanda.getTimeStamp()
        for job in jobs:
            self.stopOnFlag()
            self.log.info(f"Archiving panda job appjob({job['pandaid']})")
            # Fill out empty start/end time
            if job['starttime']:
                if not job['endtime']:
                    job['endtime'] = job['modified']
            elif job['endtime']:
                job['starttime'] = job['endtime']
            else:
                job['starttime'] = tstamp
                job['endtime'] = tstamp

            # archive table doesn't have modified
            jobarchive = job.copy()
            del jobarchive['modified']
            self.dbpanda.insertJobArchiveLazy(jobarchive)
            self.dbpanda.deleteJob(job['pandaid']) # commit is called here


    def process(self):
        """
        Method called from loop
        """
        self.setSites()

        # Getting new jobs is now done in aCTPandaGetJobs

        # Update all jobs currently in the system
        self.updatePandaHeartbeatBulk('starting')
        self.updatePandaHeartbeat('running')
        self.updatePandaHeartbeat('transferring')

        # Update jobs which finished
        self.updatePandaFinishedPilot()

        # Move old jobs to archive - every hour
        if time.time()-self.starttime > 3600:
            self.log.info("Checking for jobs to archive")
            self.updateArchive()
            self.starttime = time.time()
