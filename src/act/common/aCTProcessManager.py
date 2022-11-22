import datetime
import importlib
import multiprocessing
import time

from act.arc.aCTDBArc import aCTDBArc
from act.common.aCTConfig import aCTConfigAPP, aCTConfigARC
from act.condor.aCTDBCondor import aCTDBCondor


class aCTProcessManager:
    '''Manager of aCT processes, starting and stopping as necessary'''

    def __init__(self, log):
        self.conf = aCTConfigARC()
        self.appconf = aCTConfigAPP()

        self.log = log
        self.actlocation = self.conf.actlocation.dir
        self.logdir = self.conf.logger.logdir

        # DB connection
        if 'arc' in self.appconf.modules:
            self.dbarc = aCTDBArc(self.log)
        else:
            self.dbarc = None
        if 'condor' in self.appconf.modules:
            self.dbcondor = aCTDBCondor(self.log)
        else:
            self.dbcondor = None

        # dictionary of all running processes
        self.processes = {}
        # list of tuples of process and datetime of termination
        self.terminating = []
        # list of tuples of process and datetime of kill
        self.killing = []

    def update(self):
        self.killProcs(timeout=5)
        self.closeProcs(timeout=5)

        if 'arc' in self.appconf.modules:
            self.updateClusterProcs('arc')

        if 'condor' in self.appconf.modules:
            self.updateClusterProcs('condor')

        for module in self.appconf.modules:
            self.updateSingleProcs(module)

    # TODO: check ergonomics and correctnes of config interface (e. g. does
    # get() return None or empty DictObj since empty DictObj is False (is it?))
    def startClusterProcs(self, module, group, cluster):
        moduleProcs = self.processes.setdefault(module, {})
        groupProcs = moduleProcs.setdefault(group, {})
        clusterProcs = groupProcs.setdefault(cluster, {})
        mod = importlib.import_module(f'.{module}', 'act')
        groupList = mod.processes.get(group, {})
        for procClass in groupList:
            procName = procClass.__name__
            # do not start if process disabled in config
            # TODO: .get(...) should return empty DictObj for better ergonomics
            if self.appconf.get(module, {}).get('disable', {}).get('processes', {}).get(group, {}).get(procName, False):
                self.log.debug(f'Not running disabled process {procName}')
                continue
            # create process if it doesn't exist
            elif procName not in clusterProcs:
                self.log.debug(f'Starting process {procName} for cluster {cluster}')
                clusterProcs[procName] = multiprocessing.Process(target=procClass())
                clusterProcs[procName].start()
            # close not alive process and create new one
            elif not clusterProcs[procName].is_alive():
                self.log.debug(f'Process {procName} for cluster {cluster} not alive, restarting')
                clusterProcs[procName].close()
                clusterProcs[procName] = multiprocessing.Process(target=procClass())
                clusterProcs[procName].start()
            else:
                self.log.debug(f'Process {procName} running for cluster {cluster}')

    def startSingleProcs(self, module):
        moduleProcs = self.processes.setdefault(module, {})
        singleProcs = moduleProcs.setdefault('single', {})
        mod = importlib.import_module(f'.{module}', 'act')
        singleList = mod.processes.get('single', {})
        for procClass in singleList:
            procName = procClass.__name__
            # do not start if process disabled in config
            # TODO: .get(...) should return empty DictObj for better ergonomics
            if self.appconf.get(module, {}).get('disable', {}).get('processes', {}).get('single', {}).get(procName, False):
                self.log.debug(f'Not running disabled process {procName}')
                continue
            # create process if it doesn't exist
            if procName not in singleProcs:
                self.log.debug(f'Starting process {procName}')
                singleProcs[procName] = multiprocessing.Process(target=procClass())
                singleProcs[procName].start()
            # close not alive process and create new one
            elif not singleProcs[procName].is_alive():
                self.log.debug(f'Process {procName} not alive, restarting')
                singleProcs[procName].close()
                singleProcs[procName] = multiprocessing.Process(target=procClass())
                singleProcs[procName].start()
            else:
                self.log.debug(f'Process {procName} running')

    def stopProcs(self, procs):
        now = datetime.datetime.utcnow()
        for proc in procs:
            proc.terminate()
            self.terminating.append((proc, now))

    def killProcs(self, timeout=5):
        now = datetime.datetime.utcnow()
        for i in range(len(self.terminating - 1), -1, -1):
            proc, termtime = self.terminating[i]
            # close if terminated
            if not proc.is_alive():
                proc.close()
                self.terminating.pop(i)
            # kill if not terminated after timeout
            elif (now - termtime).second > timeout:
                proc.kill()
                self.killing.append((proc, datetime))
                self.terminating.pop(i)

    def closeProcs(self, timeout=5):
        now = datetime.datetime.utcnow()
        for i in range(len(self.killing - 1), -1, -1):
            proc, killtime = self.killing[i]
            # close process if terminated or timeout
            if not proc.is_alive() or (now - killtime).second > timeout:
                proc.close()
                self.killing.pop(i)

    def updateClusterProcs(self, module):
        if module == 'arc':
            activeClusters = self.dbarc.getActiveClusters()
            requestedClusters = self.dbarc.getClusterLists()
        elif module == 'condor':
            activeClusters = self.dbcondor.getActiveClusters()
            requestedClusters = self.dbcondor.getClusterLists()
        else:
            return
        moduleProcs = self.processes.get(module, {})

        # terminate submitters for clusters that are not in any clusterlist
        submitProcs = moduleProcs.get('submitter', {})
        for cluster, submitters in submitProcs.items():
            if cluster not in requestedClusters:
                self.log.debug(f'Cluster {cluster} not requested anymore, stopping {", ".join(submitters.keys())}')
                self.stopProcs(submitters.values())
                submitProcs.clear()

        # terminate cluster processes for inactive clusters
        clusterProcs = moduleProcs.get('cluster', {})
        for cluster, managers in clusterProcs.items():
            if cluster not in activeClusters:
                self.log.debug(f'Cluster {cluster} not active anymore, stopping {", ".join(managers.keys())}')
                self.stopProcs(managers.values())
                clusterProcs.clear()

        # start submitter processes for requested clusters
        for cluster in requestedClusters:
            self.startClusterProcs(module, 'submitter', cluster)

        # start cluster processes for active clusters
        for cluster in activeClusters:
            self.startClusterProcs(module, 'cluster', cluster)

    def updateSingleProcs(self, module):
        self.startSingleProcs(module)

    def stopAllProcesses(self, timeout=2):
        for module in self.appconf.modules:
            moduleProcs = self.processes.get(module, {})

            self.stopProcs(moduleProcs.get('single', {}).values())

            submitProcs = moduleProcs.get('submitter', {})
            for cluster, submitters in submitProcs.items():
                self.stopProcs(submitters)

            clusterProcs = moduleProcs.get('cluster', {})
            for cluster, managers in clusterProcs.items():
                self.stopProcs(managers)

        while True:
            self.killProcs(timeout)
            self.closeProcs(timeout)
            if not self.killing and not self.terminating:
                break
            time.sleep(timeout)

    def reconnectDB(self):
        try:
            del self.dbarc
            del self.dbcondor
        except AttributeError:  # Already deleted
            pass
        self.dbarc = aCTDBArc(self.log)
        self.dbcondor = aCTDBCondor(self.log)
