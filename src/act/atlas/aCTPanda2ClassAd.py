import json
import os
import time
from urllib.parse import parse_qs
class aCTPanda2ClassAd:

    def __init__(self, pandajob, pandajobid, sitename, siteinfo, proxypath, tmpdir, atlasconf, metadata, log):
        # To work with htcondor.Submit() a plain dict is used instead of a
        # ClassAd object. All values must be strings.
        self.classad = {'Universe': '9'} # Always use grid universe
        self.log = log
        self.pandajob = pandajob
        self.pandajobid = pandajobid
        self.jobdesc = parse_qs(pandajob)
        self.pandaid = self.jobdesc['PandaID'][0]
        self.prodsourcelabel = self.jobdesc.get('prodSourceLabel', ['None'])[0]
        self.resourcetype = self.jobdesc.get('resourceType', ['None'])[0]
        self.siteinfo = siteinfo
        self.ncores = siteinfo['corecount']
        self.proxy = proxypath

        self.defaults = {}
        self.defaults['memory'] = siteinfo['maxrss']
        self.defaults['cputime'] = siteinfo['maxtime']
        self.defaults['corecount'] = siteinfo['corecount']
        self.memory = 0
        self.sitename = sitename
        self.schedconfig = siteinfo['schedconfig']
        self.walltime = siteinfo['maxwalltime'] * 60
        if self.walltime == 0:
            self.walltime = 4*24*3600

        self.piloturl = siteinfo.get('params', {}).get('pilot_url')
        self.pilotversion = siteinfo.get('pilot_version', '2')

        self.tmpdir = tmpdir
        self.inputfiledir = os.path.join(self.tmpdir, 'inputfiles')
        self.inputjobdir = os.path.join(self.inputfiledir, self.pandaid)
        self.longjob = False
        self.traces = []
        if len(self.pandajob) > 50000:
            self.longjob = True

        try:
            self.schedulerid = json.loads(metadata)['schedulerid']
        except:
            self.schedulerid = atlasconf.panda.schedulerid

        self.wrapper = atlasconf.executable.wrapperurl
        # Condor out/err/log
        now = time.gmtime() # gmtime() is like localtime() but in UTC
        today = "%04d-%02d-%02d" % (now[0], now[1], now[2])
        self.logdir = os.path.join(atlasconf.joblog.dir, today, sitename)
        try: os.makedirs(self.logdir)
        except: pass

        self.logurl = f'{atlasconf.joblog.urlprefix}/{today}/{sitename}'

        # APFmon
        self.monitorurl = atlasconf.monitor.apfmon

    def getNCores(self):

        # Unified panda queues: always use coreCount from job description
        try:
            self.ncores = int(self.jobdesc.get('coreCount', [self.defaults['corecount']])[0])
        except: # corecount is NULL
            self.ncores = 1

        self.classad['+xcount'] = str(self.ncores)
        return self.ncores

    def setDisk(self):

        if 'maxDiskCount' in self.jobdesc:
            disk = int(self.jobdesc['maxDiskCount'][0])
        else:
            disk = 500

        # Add input file sizes
        if 'fsize' in self.jobdesc:
            disk += sum([int(f) for f in self.jobdesc['fsize'][0].split(',')]) / 1000000
        # Add safety factor
        disk += 2000
        self.log.debug('%s: disk space %d' % (self.pandaid, disk))
        self.classad['RequestDisk'] = str(disk * 1024) # number in KiB

    def setTime(self):

        if 'maxCpuCount' in self.jobdesc:
            cpucount = int(self.jobdesc['maxCpuCount'][0])
            cpucount = int(2 * cpucount)
            self.log.info('%s: job maxCpuCount %s' % (self.pandaid, cpucount))
        else:
            cpucount = self.defaults['cputime']
            self.log.info('%s: Using default maxCpuCount %s' % (self.pandaid, cpucount))

        if cpucount == 0:
            cpucount = self.defaults['cputime']

        walltime = cpucount

        # Give jobs at least 1 hour
        walltime = max(3600, walltime)
        # .. and at most 4 days
        walltime = min(self.walltime, walltime)

        self.walltime = walltime
        cputime = self.getNCores() * walltime
        self.log.info('%s: walltime: %ds, cputime: %ds' % (self.pandaid, walltime, cputime))

        # CERN uses MaxRunTime (seconds)
        self.classad['+MaxRuntime'] = str(walltime)
        # US sites use maxWallTime (in minutes)
        self.classad['+maxWallTime'] = str(walltime/60)

    def setMemory(self):

        # condor uses total memory, not per core
        if 'minRamCount' in self.jobdesc:
            memory = int(self.jobdesc['minRamCount'][0])
        else:
            memory = self.defaults['memory']

        if memory <= 0:
            memory = self.defaults['memory']

        # fix until maxrss in pandajob is better known
        if memory <= 500:
            memory = 500

        # fix memory to 500MB units
        memory = int(memory-1)/500*500 + 500
        self.memory = memory

        self.classad['+maxMemory'] = str(memory)

    def setExecutable(self):

        self.classad['Cmd'] = self.wrapper

    def getJobType(self):

        return 'user' if self.prodsourcelabel in ['user', 'panda'] else 'managed'

    def getResourceType(self):

        if self.resourcetype != 'None':
            return self.resourcetype

        resource = 'SCORE'
        if self.ncores > 1:
            resource = 'MCORE'
        if self.memory > self.defaults['memory']:
            resource += '_HIMEM'
        return resource

    def setArguments(self):

        pargs = '-q %s -r %s -s %s -d -j %s --pilot-user ATLAS -w generic --job-type %s --resource-type %s --url https://pandaserver.cern.ch -p 25443' \
                % (self.schedconfig, self.sitename, self.sitename, self.prodsourcelabel, self.getJobType(), self.getResourceType())
        if self.prodsourcelabel == 'rc_alrb':
            pargs += ' -i ALRB'
        elif self.prodsourcelabel.startswith('rc_test'):
            pargs += ' -i RC'

        if self.siteinfo['python_version'].startswith('3'):
            pargs += ' --pythonversion 3'

        if self.piloturl:
            pargs += ' --piloturl %s' % (self.piloturl)
        else:
            pargs += ' --pilotversion %s' % (self.pilotversion)

        self.classad['Arguments'] = str(pargs)

    def setInputs(self):

        # Write panda job desc to a file to upload with the job. Condor doesn't
        # allow different src and dest filenames so make a dir per panda job id
        if not self.siteinfo['push']:
            return
        try:
            os.makedirs(self.inputjobdir)
        except:
            pass
        tmpfile = os.path.join(self.inputjobdir, "pandaJobData.out")
        with open(tmpfile, "w") as f:
            f.write(self.pandajob)
        self.classad['TransferInputFiles'] = str(tmpfile)

    def setLog(self):

        self.classad['UserLog'] = str(os.path.join(self.logdir, '%s.log' % self.pandaid))

    def setOutputs(self):

        self.classad['Output'] = str(os.path.join(self.logdir, '%s.out' % self.pandaid))
        self.classad['Error'] = str(os.path.join(self.logdir, '%s.err' % self.pandaid))

    def setPriority(self):

        # Scale panda priority to 1-100. TODO: Maybe can pass panda priority directly
        if 'currentPriority' in self.jobdesc:
            prio = 50
            try:
                prio = int(self.jobdesc['currentPriority'][0])
                if prio < 1:
                    prio = 1
                if prio > 0 and prio < 1001:
                    prio = prio * 90 // 1000
                if prio > 1000 and prio < 10001:
                    prio = 90 + (prio - 1000) // 900
                if prio > 10000:
                    prio = 100
            except:
                pass
            self.classad['JobPrio'] = str(prio)

    def setEnvironment(self):
        environment = []
        # Set schedulerID and job log URL
        environment.append('PANDA_JSID=%s' % self.schedulerid)
        environment.append('GTAG=%s/%s.out' % (self.logurl, self.pandaid))

        # Vars for APFMon
        environment.append('APFCID=%s' % self.pandajobid)
        # harvester prepends "harvester-" to the schedulerid but APFMon uses the original one
        environment.append('APFFID=%s' % self.schedulerid.replace("harvester-",""))
        if self.monitorurl:
            environment.append('APFMON=%s' % self.monitorurl)
        environment.append('FACTORYQUEUE=%s' % self.sitename)

        self.classad['Environment2'] = '"%s"' % ' '.join(environment)


    def setProxy(self):
        self.classad['X509UserProxy'] = self.proxy

    def setAttrs(self):

        # Set CREAM attributes. Times are in minutes.
        if len([e for e in self.siteinfo['endpoints'] if e.startswith('cream')]) > 0:
            creamattrs = 'CpuNumber=%d;WholeNodes=false;SMPGranularity=%d;' % (self.ncores, self.ncores)
            creamattrs += 'CERequirements = "other.GlueCEPolicyMaxCPUTime == %d' % ((self.walltime/60) * self.ncores)
            creamattrs += ' && other.GlueCEPolicyMaxWallClockTime == %d' % (self.walltime/60)
            creamattrs += ' && other.GlueHostMainMemoryRAMSize == %d' % (self.memory)
            creamattrs += ' && other.GlueHostMainMemoryVirtualSize == %d";' % (self.memory * 3)
            self.classad['CreamAttributes'] = creamattrs

    def parse(self):
        self.setTime()
        #self.setDisk()
        self.setMemory()
        self.setExecutable()
        self.setArguments()
        self.setInputs()
        self.setLog()
        self.setOutputs()
        self.setPriority()
        self.setEnvironment()
        self.setProxy()
        self.setAttrs()

    def getClassAd(self):
        return self.classad


if __name__ == '__main__':
    from act.common.aCTLogger import aCTLogger
    from act.common.aCTConfig import aCTConfigAPP
    logger=aCTLogger('test')
    l=logger()
    pjob = "jobsetID=799&logGUID=5ba37307-e4d7-4224-82f9-ff0503622677&cmtConfig=x86_64-slc6-gcc48-opt&prodDBlocks=user.rwatari%3Auser.rwatari.1k_10mu.xm005_yp106.RDO.20161003_2_EXT0_RDO2RDOFTK_v01_all1E5ev_EXT2.99328897%2Cpanda.1110091801.467362.lib._9845189&dispatchDBlockTokenForOut=NULL%2CNULL%2CNULL&destinationDBlockToken=NULL%2CNULL%2CNULL&destinationSE=NULL&realDatasets=user.rwatari.1k_10mu.xm005_yp106.RDO.20161003_2_EXT0_PseduoTracking_v14_all1E5ev_EXT0%2F%2Cuser.rwatari.1k_10mu.xm005_yp106.RDO.20161003_2_EXT0_PseduoTracking_v14_all1E5ev_EXT1%2F%2Cuser.rwatari.1k_10mu.xm005_yp106.RDO.20161003_2_EXT0_PseduoTracking_v14_all1E5ev.log%2F&prodUserID=%2FDC%3Dch%2FDC%3Dcern%2FOU%3DOrganic+Units%2FOU%3DUsers%2FCN%3Drwatari%2FCN%3D764796%2FCN%3DRyutaro+Watari%2FCN%3Dproxy&GUID=51997D0A-850A-9044-A264-83A8986FE1C6%2C1de48e07-f37c-43e6-a343-3947342858b1&realDatasetsIn=user.rwatari.1k_10mu.xm005_yp106.RDO.20161003_2_EXT0_RDO2RDOFTK_v01_all1E5ev_EXT2%2Cpanda.1110091801.467362.lib._9845189&nSent=0&cloud=ND&StatusCode=0&homepackage=AnalysisTransforms-AtlasProduction_20.7.3.7&inFiles=user.rwatari.9557718.EXT2._000016.RDO_FTK.pool.root%2Cpanda.1110091801.467362.lib._9845189.7456421499.lib.tgz&processingType=panda-client-0.5.69-jedi-athena-trf&currentPriority=814&fsize=1140292964%2C727003478&fileDestinationSE=ANALY_SiGNET_DIRECT%2CANALY_SiGNET_DIRECT%2CANALY_SiGNET_DIRECT&scopeOut=user.rwatari%2Cuser.rwatari&minRamCount=4772&jobDefinitionID=836&scopeLog=user.rwatari&transformation=http%3A%2F%2Fpandaserver.cern.ch%3A25085%2Ftrf%2Fuser%2FrunAthena-00-00-12&maxDiskCount=3167&coreCount=1&prodDBlockToken=NULL%2CNULL&transferType=NULL&destinationDblock=user.rwatari.1k_10mu.xm005_yp106.RDO.20161003_2_EXT0_PseduoTracking_v14_all1E5ev_EXT0.104826316_sub0341667607%2Cuser.rwatari.1k_10mu.xm005_yp106.RDO.20161003_2_EXT0_PseduoTracking_v14_all1E5ev_EXT1.104826317_sub0341667608%2Cuser.rwatari.1k_10mu.xm005_yp106.RDO.20161003_2_EXT0_PseduoTracking_v14_all1E5ev.log.104826315_sub0341667610&dispatchDBlockToken=NULL%2CNULL&jobPars=-l+panda.1110091801.467362.lib._9845189.7456421499.lib.tgz+--sourceURL+https%3A%2F%2Faipanda078.cern.ch%3A25443+-r+WorkArea%2Frun%2Ffast%2F+--trf+--useLocalIO++-i+%22%5B%27user.rwatari.9557718.EXT2._000016.RDO_FTK.pool.root%27%5D%22+-o+%22%7B%27IROOT%27%3A+%5B%28%27InDetDxAOD.pool.root%27%2C+%27user.rwatari.9845189.EXT0._002324.InDetDxAOD.pool.root%27%29%2C+%28%27esd.pool.root%27%2C+%27user.rwatari.9845189.EXT1._002324.esd.pool.root%27%29%5D%7D%22++-j+%22Reco_tf.py%2520--inputRDOFile%253Duser.rwatari.9557718.EXT2._000016.RDO_FTK.pool.root%2520--outputESDFile%253Desd.pool.root%2520%2520--doAllNoise%2520False%2520--autoConfiguration%253Deverything%2520--numberOfCavernBkg%253D0%2520--postInclude%253DFTKFastSim%2FInDetDxAOD.py%2520--preExec%2520%2527rec.UserAlgs%253D%255B%2522FTKFastSim%2FFTKFastSimulation_jobOptions.py%2522%255D%253Brec.doCalo.set_Value_and_Lock%2528False%2529%253Brec.doMuon.set_Value_and_Lock%2528False%2529%253Brec.doJetMissingETTag.set_Value_and_Lock%2528False%2529%253Brec.doEgamma.set_Value_and_Lock%2528False%2529%253Brec.doMuonCombined.set_Value_and_Lock%2528False%2529%253Brec.doTau.set_Value_and_Lock%2528False%2529%253Brec.doTrigger.set_Value_and_Lock%2528False%2529%253Brec.doFTK.set_Value_and_Lock%2528True%2529%253Bfrom%2520AthenaCommon.DetFlags%2520import%2520DetFlags%253BDetFlags.all_setOn%2528%2529%253BDetFlags.FTK_setOn%2528%2529%2527%2520--maxEvents%253D-1%2520--postExec%2520r2e%253A%2520%2527ServiceMgr%252B%253DService%2528%2522BeamCondSvc%2522%2529%253BbeamCondSvc%253DServiceMgr.BeamCondSvc%253BbeamCondSvc.useDB%253DFalse%253BbeamCondSvc.posX%253D-0.0497705%253BbeamCondSvc.posY%253D1.06299%253BbeamCondSvc.posZ%253D0.0%253BbeamCondSvc.sigmaX%253D0.0251281%253BbeamCondSvc.sigmaY%253D0.0231978%253BbeamCondSvc.sigmaZ%253D0.1%253BbeamCondSvc.sigmaXY%253D-2.7745e-06%253BbeamCondSvc.tiltX%253D-1.51489e-05%253BbeamCondSvc.tiltY%253D-4.83891e-05%253B%2527%22&attemptNr=2&swRelease=Atlas-20.7.3&nucleus=NULL&maxCpuCount=0&outFiles=user.rwatari.9845189.EXT0._002324.InDetDxAOD.pool.root%2Cuser.rwatari.9845189.EXT1._002324.esd.pool.root%2Cuser.rwatari.1k_10mu.xm005_yp106.RDO.20161003_2_EXT0_PseduoTracking_v14_all1E5ev.log.9845189.002324.log.tgz&ddmEndPointOut=NDGF-T1_SCRATCHDISK%2CNDGF-T1_SCRATCHDISK%2CNDGF-T1_SCRATCHDISK&scopeIn=user.rwatari%2Cpanda&PandaID=3072596651&sourceSite=NULL&dispatchDblock=NULL%2Cpanda.1110091801.467362.lib._9845189&prodSourceLabel=user&checksum=ad%3Afd1c3aac%2Cad%3A516b31b3&jobName=user.rwatari.1k_10mu.xm005_yp106.RDO.20161003_2_EXT0_PseduoTracking_v14_all1E5ev%2F.3071213044&ddmEndPointIn=NDGF-T1_SCRATCHDISK%2CNDGF-T1_SCRATCHDISK&taskID=9845189&logFile=user.rwatari.1k_10mu.xm005_yp106.RDO.20161003_2_EXT0_PseduoTracking_v14_all1E5ev.log.9845189.002324.log.tgz"
    pjobid = 1234
    info = {'schedconfig': 'ANALY_SiGNET_DIRECT', 'corecount': 1, 'truepilot': True, 'maxwalltime': 10800, 'direct_access_lan': True, 'type': 'analysis', 'endpoints': ['condor ce509.cern.ch ce509.cern.ch:9619'], 'maxrss': 2000, 'maxtime': 4*24*3600, 'push': True}
    proxy = '/tmp/x509up_u%d' % os.getuid()
    conf = aCTConfigAPP()
    a = aCTPanda2ClassAd(pjob, pjobid, 'ANALY_SiGNET_DIRECT', info, proxy, '/tmp', conf, '{}', l)
    a.parse()
    print(a.getClassAd())
