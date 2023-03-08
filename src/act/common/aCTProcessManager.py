# There are some try blocks that deal with process management and module
# importing that can be faulty. The blocks prevent the program to crash and
# the process management to break down which would leave the managed processes
# hanging around or hanging up the main program.
#
# TODO: there is probably a more elegant way of hardening


import datetime
import importlib
import itertools
import multiprocessing

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
    multiprocessing.Process instances are kept in the manager's
    attributes: processes, terminating and killing.

    The processes dictionary structure has all instances that are required to
    be running and is designed in a way that every individual instance can
    easily be accessed based on its module, type (and cluster for cluster
    processes). An example dictionary structure could be:

    self.processes = {
        "act.arc": {
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
        "act.atlas": {
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

    Process running conditions:
    - single processes are always required to run
    - submitter processes are required to run for every cluster that is
      specified in any one of the jobs in aCT
    - cluster processes are required to run for every cluster that jobs
      were submitted to and are required until the jobs are not on the
      cluster anymore
    """

    def __init__(self, log):
        """Initialize required attributes."""
        self.appconf = aCTConfigAPP()

        self.log = log

        # DB connection
        if 'act.arc' in self.appconf.modules:
            self.dbarc = aCTDBArc(self.log)
        else:
            self.dbarc = None
        if 'act.condor' in self.appconf.modules:
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
        self.appconf = aCTConfigAPP()

        self.killProcs(timeout=termTimeout)
        self.closeProcs(timeout=killTimeout)

        if 'act.arc' in self.appconf.modules:
            self.updateClusterProcs('act.arc')

        if 'act.condor' in self.appconf.modules:
            self.updateClusterProcs('act.condor')

        for module in self.appconf.modules:
            self.updateSingleProcs(module)

    def startClusterProcs(self, module, procType, cluster):
        """
        Start all enabled processes of a given type for a given cluster.

        Though submitter processes are handled separately from cluster
        processes, the way of handling them is still the same. That is why
        the type is given as a parameter and this function is used to manage
        both types.
        """
        moduleProcs = self.processes.setdefault(module, {})
        typeProcs = moduleProcs.setdefault(procType, {})
        clusterProcs = typeProcs.setdefault(cluster, {})

        # try for hardening
        try:
            mod = importlib.import_module(module)
        except Exception as exc:
            self.log.debug(f'Error loading module {module}: {exc}')
            return
        typeList = mod.processes.get(procType, {})

        for procClass in typeList:
            procName = procClass.__name__
            # do not start if process disabled in config
            if procName in self.appconf.get('disabledProcs', {}).get(module, {}).get(procType, {}).get(cluster, []):
                self.log.debug(f'Not running disabled process {procName} for cluster {cluster}')
                continue
            # create process if it doesn't exist
            elif procName not in clusterProcs:
                self.log.debug(f'Starting process {procName} for cluster {cluster}')

                # try for hardening
                try:
                    clusterProcs[procName] = multiprocessing.Process(target=procClass(cluster))
                    clusterProcs[procName].start()
                except Exception as exc:
                    self.log.debug(f'Error starting process {procName} for cluster {cluster}: {exc}')
                    continue

            # close not alive process and create new one
            elif not clusterProcs[procName].is_alive():
                self.log.debug(f'Process {procName} for cluster {cluster} not alive, restarting')
                # only available from Python 3.7
                #clusterProcs[procName].close()

                # try for hardening
                try:
                    clusterProcs[procName] = multiprocessing.Process(target=procClass(cluster))
                    clusterProcs[procName].start()
                except Exception as exc:
                    self.log.debug(f'Error starting process {procName} for cluster {cluster}: {exc}')
                    continue

            # try for hardening
            try:
                if clusterProcs[procName].is_alive():
                    self.log.debug(f'Process {procName} running for cluster {cluster}')
            except Exception as exc:
                self.log.debug(f'Error checking if {procName} is running for cluster {cluster}: {exc}')

    def startSingleProcs(self, module):
        """Start all enabled single processes for a given module."""
        moduleProcs = self.processes.setdefault(module, {})
        singleProcs = moduleProcs.setdefault('single', {})

        # try for hardening
        try:
            mod = importlib.import_module(module)
        except Exception as exc:
            self.log.debug(f'Error loading module {module}: {exc}')
            return
        singleList = mod.processes.get('single', {})

        for procClass in singleList:
            procName = procClass.__name__
            # do not start if process disabled in config
            if procName in self.appconf.get('disabledProcs', {}).get(module, {}).get('single', []):
                self.log.debug(f'Not running disabled process {procName}')
                continue
            # create process if it doesn't exist
            if procName not in singleProcs:
                self.log.debug(f'Starting process {procName}')

                # try for hardening
                try:
                    singleProcs[procName] = multiprocessing.Process(target=procClass())
                    singleProcs[procName].start()
                except Exception as exc:
                    self.log.debug(f'Error starting process {procName}: {exc}')
                    continue

            # close not alive process and create new one
            elif not singleProcs[procName].is_alive():
                self.log.debug(f'Process {procName} not alive, restarting')
                # only available from Python 3.7
                #singleProcs[procName].close()

                # try for hardening
                try:
                    singleProcs[procName] = multiprocessing.Process(target=procClass())
                    singleProcs[procName].start()
                except Exception as exc:
                    self.log.debug(f'Error starting process {procName}: {exc}')
                    continue

            # try for hardening
            try:
                if singleProcs[procName].is_alive():
                    self.log.debug(f'Process {procName} running')
            except Exception as exc:
                self.log.debug(f'Error checking if {procName} is running: {exc}')

    # TODO: would it be cleaner to have a method for stopping a single process?
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

            # try for hardening
            try:
                proc.terminate()
            except Exception as exc:
                self.log.debug(f'Failed to terminate process {proc.name}: {exc}')

            self.terminating.append((proc, now))

    def killProcs(self, timeout=5):
        """
        Kill all processes that haven't terminated in a given timeout.

        kill() is only available in Python >= 3.7 so for now the process
        objects are just passed on to killing list.
        """
        now = datetime.datetime.utcnow()
        for i in range(len(self.terminating) - 1, -1, -1):
            proc, termtime = self.terminating[i]

            # try for hardening
            try:
                # close if terminated
                if not proc.is_alive():
                    # only available from Python 3.7
                    #proc.close()
                    self.terminating.pop(i)
                # kill if not terminated after timeout
                elif (now - termtime).seconds > timeout:
                    # only available from Python 3.7
                    #proc.kill()
                    self.killing.append((proc, now))
                    self.terminating.pop(i)
            except Exception as exc:
                self.log.debug(f'Failed to check if process {proc.name} is running: {exc}')
                self.killing.append((proc, now))
                self.terminating.pop(i)

    def closeProcs(self, timeout=5):
        """
        Close all processes that haven't been killed in a given timeout.

        close() is only available in Python >= 3.7 so for now the process is
        kept around and join()ed in the end. If the process still persists, it
        becomes responsibility of the service manager/control terminal.
        """
        now = datetime.datetime.utcnow()
        for i in range(len(self.killing) - 1, -1, -1):
            proc, killtime = self.killing[i]

            # try for hardening
            try:
                # close process if terminated or timeout
                if not proc.is_alive():
                    self.killing.pop(i)
                elif (now - killtime).seconds > timeout:
                    ## only available from Python 3.7
                    ##try:
                    ##    proc.close()
                    ##except ValueError:
                    ##    pass
                    #self.killing.pop(i)
                    # Rather leave process in killing and join it in the end.
                    # Could potentially lead to accumulation of alive processes.
                    pass
            except Exception as exc:
                self.log.debug(f'Failed to check if process {proc.name} is running: {exc}')

    def updateClusterProcs(self, module):
        """
        Update state of cluster processes for a given module.

        Requested clusters is a list of unique clusters from clusterlists
        of all jobs. Active clusters is a list of unique clusters from
        cluster attribute of all jobs.
        """
        # get required and active clusters
        if module == 'act.arc':
            activeClusters = [entry['cluster'] for entry in self.dbarc.getActiveClusters()]
            requestedClusters = list(itertools.chain(*[entry['clusterlist'].split(',') for entry in self.dbarc.getClusterLists()]))
        elif module == 'act.condor':
            activeClusters = self.dbcondor.getActiveClusters()
            requestedClusters = self.dbcondor.getClusterLists()
        else:
            return
        moduleProcs = self.processes.get(module, {})

        # terminate submitter processes for non requested clusters
        submitProcs = moduleProcs.get('submitter', {})
        for cluster, submitters in submitProcs.items():
            if submitters and cluster not in requestedClusters:
                self.log.debug(f'Cluster {cluster} not requested anymore, stopping {", ".join(submitters.keys())}')
                self.stopProcs(submitters.values())
                submitProcs[cluster].clear()

        # terminate cluster processes for inactive clusters
        clusterProcs = moduleProcs.get('cluster', {})
        for cluster, managers in clusterProcs.items():
            if managers and cluster not in activeClusters:
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
        # stop disabled processes
        singleProcs = self.processes.get(module, {}).get('single', {})
        for procName in list(singleProcs):
            if procName in self.appconf.get('disabledProcs', {}).get(module, {}).get('single', []):
                self.log.debug(f'Single process {procName} disabled, stopping')
                self.stopProcs([singleProcs[procName]])
                del singleProcs[procName]

        # start single processes
        self.startSingleProcs(module)

    def stopAllProcesses(self):
        """
        Stop all running processes.

        The terminated processes are then joined which could lock up the
        process. In that case, the service manager or controlling terminal
        should handle the SIGKILL signal for the processes.
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

        for proc, _ in self.terminating:
            # try for hardening
            try:
                proc.join()
            except Exception as exc:
                self.log.debug(f'Failed to wait for stopped process {proc.name}')
        for proc, _ in self.killing:
            # try for hardening
            try:
                proc.join()
            except Exception as exc:
                self.log.debug(f'Failed to wait for killed process {proc.name}')

    def reconnectDB(self):
        """Reconnect database connections."""
        try:
            del self.dbarc
            del self.dbcondor
        except AttributeError:  # Already deleted
            pass
        self.dbarc = aCTDBArc(self.log)
        self.dbcondor = aCTDBCondor(self.log)
