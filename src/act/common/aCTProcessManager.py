import datetime
import itertools
import importlib
import multiprocessing
import time

from act.arc.aCTDBArc import aCTDBArc
from act.common.aCTConfig import aCTConfigAPP
from act.condor.aCTDBCondor import aCTDBCondor


class aCTProcessManager:
    """
    Manages aCT processes using the multiprocessing module.

    There are 3 types of processes to manage:
    - single: a single instance of the process is managed
    - cluster: one instance of the process is managed per cluster
    - submitter: a special kind of cluster process that should be managed under
      different conditions than regular cluster process

    single type processes are always required to run while cluster and
    submitter types are only necessary if the jobs require them. Process
    manager creates and supervises all required processes. It restarts crashed
    or terminated processes. It also stops processes that are not required
    anymore until they are required again. See the methods for more details
    on starting and stopping.

    Each module can define a dictionary structure of the processes to be run by
    the process manager, e. g. the arc module:

    processes = {
        'submitter': [aCTSubmitter],
        'cluster': [aCTCleaner, aCTFetcher, aCTStatus],
        'single': [aCTProxyHandler, aCTMonitor]
    }

    The values in the dictionary structure are classes from aCT process
    hierarchy that are used to instantiate the callable objects for
    multiprocessing.Process instances that run the OS processes. Created
    multiprocessing.Process instances are kept in the attributes
    processes, terminating and killing.

    The processes dictionary structure has all instances that are required to
    be running and is designed in a way that every individual instance can
    easily be accessed and manipulated. An example dictionary structure could
    be:

    self.processes = {
        "arc": {
            "submitter": {
                "arc01.net": {
                    "aCTSubmitter":     multiprocessing.Process,
                },
                "arc02.net": {
                    "aCTSubmitter":     multiprocessing.Process,
                },
            },
            "cluster": {
                "arc01.net": {
                    "aCTCleaner":       multiprocessing.Process,
                    "aCTFetcher":       multiprocessing.Process,
                    "aCTStatus":        multiprocessing.Process,
                },
            },
            "single": {
                "aCTProxyHandler":      multiprocessing.Process,
                "aCTMonitor":           multiprocessing.Process,
            },
        },
        "atlas": {
            "single": {
                "aCTCRICFetcher":       multiprocessing.Process,
                "aCTATLASStatus":       multiprocessing.Process,
                "aCTATLASStatusCondor": multiprocessing.Process,
                "aCTAutopilot":         multiprocessing.Process,
                "aCTAutopilotSent":     multiprocessing.Process,
                "aCTPanda2Arc":         multiprocessing.Process,
                "aCTPanda2Condor":      multiprocessing.Process,
                "aCTValidator":         multiprocessing.Process,
                "aCTValidatorCondor":   multiprocessing.Process,
            },
        },
    }

    terminating and killing are lists of instances that are being terminated.
    """

    def __init__(self, log):
        """Initialize required attributes."""
        self.appconf = aCTConfigAPP()

        self.log = log

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

    def update(self, termTimeout=5, killTimeout=5):
        """Update the state of managed processes."""
        self.killProcs(timeout=termTimeout)
        self.closeProcs(timeout=killTimeout)

        if 'arc' in self.appconf.modules:
            self.updateClusterProcs('arc')

        if 'condor' in self.appconf.modules:
            self.updateClusterProcs('condor')

        for module in self.appconf.modules:
            self.updateSingleProcs(module)

    # TODO: check ergonomics and correctnes of config interface (e. g. does
    # get() return None or empty DictObj since empty DictObj is False (is it?))
    def startClusterProcs(self, module, procType, cluster):
        """
        Start all eligible processes of a given type for a given cluster.

        Though submitter processes are handled separately from cluster
        processes, the way of handling them is still the same. That is why
        the type is given as a parameter.
        """
        moduleProcs = self.processes.setdefault(module, {})
        typeProcs = moduleProcs.setdefault(procType, {})
        clusterProcs = typeProcs.setdefault(cluster, {})
        mod = importlib.import_module(f'.{module}', 'act')
        typeList = mod.processes.get(procType, {})
        for procClass in typeList:
            procName = procClass.__name__
            # do not start if process disabled in config
            if procName in self.appconf.get('disableProcs', {}).get(module, []):
                self.log.debug(f'Not running disabled process {procName}')
                continue
            # create process if it doesn't exist
            elif procName not in clusterProcs:
                self.log.debug(f'Starting process {procName} for cluster {cluster}')
                clusterProcs[procName] = multiprocessing.Process(target=procClass(cluster))
                clusterProcs[procName].start()
            # close not alive process and create new one
            elif not clusterProcs[procName].is_alive():
                self.log.debug(f'Process {procName} for cluster {cluster} not alive, restarting')
                # only available from Python 3.7
                #clusterProcs[procName].close()
                clusterProcs[procName] = multiprocessing.Process(target=procClass(cluster))
                clusterProcs[procName].start()
            else:
                self.log.debug(f'Process {procName} running for cluster {cluster}')

    def startSingleProcs(self, module):
        """Start all eligible single processes for a given module."""
        moduleProcs = self.processes.setdefault(module, {})
        singleProcs = moduleProcs.setdefault('single', {})
        mod = importlib.import_module(f'.{module}', 'act')
        singleList = mod.processes.get('single', {})
        for procClass in singleList:
            procName = procClass.__name__
            # do not start if process disabled in config
            if procName in self.appconf.get('disableProcs', {}).get(module, []):
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
                # only available from Python 3.7
                #singleProcs[procName].close()
                singleProcs[procName] = multiprocessing.Process(target=procClass())
                singleProcs[procName].start()
            else:
                self.log.debug(f'Process {procName} running')

    def stopProcs(self, procs):
        """
        Terminate a given list of processes.

        The processes are appended to the list of terminating processes
        together with the datetime object of the time of the termination for
        the purpose of sending SIGKILL if it doesn't terminate in required
        time.
        """
        now = datetime.datetime.utcnow()
        for proc in procs:
            proc.terminate()
            self.terminating.append((proc, now))

    def killProcs(self, timeout=5):
        """Kill all processes that haven't terminated in a given timeout."""
        now = datetime.datetime.utcnow()
        for i in range(len(self.terminating) - 1, -1, -1):
            proc, termtime = self.terminating[i]
            # close if terminated
            if not proc.is_alive():
                # only available from Python 3.7
                #proc.close()
                self.terminating.pop(i)
            # kill if not terminated after timeout
            elif (now - termtime).seconds > timeout:
                # only available from Python 3.7
                #proc.kill()
                self.killing.append((proc, datetime))
                self.terminating.pop(i)

    def closeProcs(self, timeout=5):
        """Close all processes that haven't been killed in a given timeout."""
        now = datetime.datetime.utcnow()
        for i in range(len(self.killing) - 1, -1, -1):
            proc, killtime = self.killing[i]
            # close process if terminated or timeout
            if not proc.is_alive() or (now - killtime).seconds > timeout:
                # only available from Python 3.7
                #try:
                #    proc.close()
                #except ValueError:
                #    pass
                self.killing.pop(i)

    def updateClusterProcs(self, module):
        """Update state of (not) required processes for a given module."""
        if module == 'arc':
            activeClusters = [entry['cluster'] for entry in self.dbarc.getActiveClusters()]
            requestedClusters = itertools.chain(*[entry['clusterlist'].split(',') for entry in self.dbarc.getClusterLists()])
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
                submitProcs[cluster].clear()

        # terminate cluster processes for inactive clusters
        clusterProcs = moduleProcs.get('cluster', {})
        for cluster, managers in clusterProcs.items():
            if cluster not in activeClusters:
                self.log.debug(f'Cluster {cluster} not active anymore, stopping {", ".join(managers.keys())}')
                self.stopProcs(managers.values())
                clusterProcs[cluster].clear()

        # start submitter processes for requested clusters
        for cluster in requestedClusters:
            self.startClusterProcs(module, 'submitter', cluster)

        # start cluster processes for active clusters
        for cluster in activeClusters:
            self.startClusterProcs(module, 'cluster', cluster)

    def updateSingleProcs(self, module):
        """Update state of single processes for a given module."""
        self.startSingleProcs(module)

    def stopAllProcesses(self, timeout=2):
        """
        Stop all running processes with a given timeout.

        The timeout is first used for termination and then for killing.
        """
        for module in self.appconf.modules:
            moduleProcs = self.processes.get(module, {})

            self.stopProcs(moduleProcs.get('single', {}).values())

            submitProcs = moduleProcs.get('submitter', {})
            for cluster, submitters in submitProcs.items():
                self.stopProcs(submitters.values())

            clusterProcs = moduleProcs.get('cluster', {})
            for cluster, managers in clusterProcs.items():
                self.stopProcs(managers.values())

        while True:
            self.killProcs(timeout)
            self.closeProcs(timeout)
            if not self.killing and not self.terminating:
                break
            time.sleep(timeout)

    def reconnectDB(self):
        """Reconnect database connections."""
        try:
            del self.dbarc
            del self.dbcondor
        except AttributeError:  # Already deleted
            pass
        self.dbarc = aCTDBArc(self.log)
        self.dbcondor = aCTDBCondor(self.log)
