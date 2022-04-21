import importlib
import itertools
import subprocess
import os

from . import aCTUtils
from act.arc import aCTDBArc
from act.condor import aCTDBCondor

class aCTProcessManager:
    '''
    Manager of aCT processes, starting and stopping as necessary
    '''

    def __init__(self, log, conf, appconf):

        self.conf = conf
        # logger
        self.log = log
        self.actlocation = self.conf.get(["actlocation","dir"])
        self.logdir = self.conf.get(["logger", "logdir"])
        # DB connection
        self.dbarc = aCTDBArc.aCTDBArc(self.log)
        self.dbcondor = aCTDBCondor.aCTDBCondor(self.log)
        # list of processes to run per cluster
        self.arcprocesses = ['act/arc/aCTStatus', 'act/arc/aCTFetcher', 'act/arc/aCTCleaner']
        self.condorprocesses = ['act/condor/aCTStatus', 'act/condor/aCTFetcher', 'act/condor/aCTCleaner']
        # submitter process
        self.arcsubmitter = 'act/arc/aCTSubmitter'
        self.condorsubmitter = 'act/condor/aCTSubmitter'
        # dictionary of processes:aCTProcessHandler of which to run a single instance
        self.processes_single = {'act/common/aCTProxyHandler': None,
                                 'act/common/aCTMonitor': None}
        apps = appconf.getList(["modules", "app"])
        for app in apps:
            try:
                ap = importlib.import_module(app).app_processes
                self.processes_single.update({f'{app.replace(".", "/")}/{p}': None for p in ap})
            except ModuleNotFoundError as e:
                self.log.critical(f'No such module {app}')
                raise e
            except AttributeError:
                self.log.info(f'No app-specific processes found in {app}')
            else:
                self.log.info(f'Loaded {", ".join(ap)} processes from {app}')

        # dictionary of cluster to list of aCTProcessHandlers
        self.running = {}
        # dictionary of cluster to list of Submitter processes handlers, there should
        # be one per unique cluster in clusterlist
        self.submitters = {}

        # Start single instance processes
        for process in self.processes_single:
            proc = self.aCTProcessHandler(process, self.logdir, actlocation=self.actlocation)
            proc.start()
            self.processes_single[process] = proc

    def __del__(self):

        self.log.info('Shutdown')
        for cluster, procs in self.running.items():
            for proc in procs:
                self.log.info('Terminating %s for %s' % (proc.name, cluster))
                proc.terminate()
        for cluster, procs in self.submitters.items():
            for proc in procs:
                self.log.info('Terminating aCTSubmitter for %s' % cluster)
                proc.terminate()
        for appproc, proc in self.processes_single.items():
            self.log.info('Terminating %s' % appproc)
            proc.terminate()

        # Sleep to allow processes to exit before checking them in aCTProcess
        # destructor
        aCTUtils.sleep(1)

    def reconnectDB(self):
        '''
        Reconnect DB
        '''
        try:
            del self.dbarc
            del self.dbcondor
        except AttributeError: # Already deleted
            pass
        self.dbarc = aCTDBArc.aCTDBArc(self.log)
        self.dbcondor = aCTDBCondor.aCTDBCondor(self.log)


    def checkARCClusters(self):
        '''
        Get the list of current clusters and (re)start necessary processes
        '''

        self.conf.parse()

        clusters = self.dbarc.getActiveClusters()
        activeclusters = dict((k, v) for (k, v) in zip([c['cluster'] for c in clusters],
                                                       [c['COUNT(*)'] for c in clusters]))
        clusters = self.dbarc.getClusterLists()
        clusterlists = dict((k, v) for (k, v) in zip([c['clusterlist'] for c in clusters],
                                                     [c['COUNT(*)'] for c in clusters]))

        # Check for processes that exited and if they should be restarted
        # All running per-cluster processes
        procs = [p for c in self.running for p in self.running[c]]
        # Submitter processes
        procs.extend(itertools.chain(*self.submitters.values()))
        # Single instance processes
        procs.extend(list(self.processes_single.values()))

        for proc in procs:
            rc = proc.check()
            if rc == None :
                self.log.debug("Process %s%s is running", proc.name, ' for %s' % proc.cluster if proc.cluster else '')
            elif proc.cluster and proc.cluster not in activeclusters.keys():
                self.log.info("Not restarting %s for %s as not needed", proc.name, proc.cluster)
                if proc.name == self.arcsubmitter:
                    if proc.cluster in self.submitters:
                        del self.submitters[proc.cluster]
                elif proc.cluster in self.running:
                    del self.running[proc.cluster]
            else:
                self.log.info("Restarting process %s %s", proc.name, 'for %s' % proc.cluster if proc.cluster else '')
                proc.restart()

        # Check for new processes to start
        for cluster in activeclusters:
            if cluster and cluster not in self.running.keys():
                self.running[cluster] = []
                for proc in self.arcprocesses:
                    self.log.info("Starting process %s for %s", proc, cluster)
                    ph = self.aCTProcessHandler(proc, self.logdir, cluster, actlocation=self.actlocation)
                    ph.start()
                    self.running[cluster].append(ph)

        # Get unique list of clusters from cluster lists
        clusterlist = []
        for cluster in clusterlists:
            if not cluster:
                cluster = ''
            clist = cluster.split(',')
            for c in clist:
                if c not in clusterlist:
                    clusterlist.append(c)

        # Start any new submitters required
        for cluster in clusterlist:
            if cluster not in self.submitters:
                procs = []
                for nprocs in range(int(self.conf.getCond(["sites", "site"], f"endpoint={cluster}", ["submitters"]) or 1)):
                    self.log.info("Starting process aCTSubmitter for %s", cluster)
                    ph = self.aCTProcessHandler(self.arcsubmitter, self.logdir, cluster, actlocation=self.actlocation)
                    ph.start()
                    procs.append(ph)
                self.submitters[cluster] = procs

    def checkCondorClusters(self):
        '''
        Get the list of current Condor clusters and (re)start necessary processes
        TODO: merge this with ARC version to avoid duplication
        '''

        clusters = self.dbcondor.getActiveClusters()
        activeclusters = dict((k, v) for (k, v) in zip([c['cluster'] for c in clusters],
                                                       [c['COUNT(*)'] for c in clusters]))
        clusters = self.dbcondor.getClusterLists()
        clusterlists = dict((k, v) for (k, v) in zip([c['clusterlist'] for c in clusters],
                                                     [c['COUNT(*)'] for c in clusters]))

        # Check for processes that exited and if they should be restarted
        # All running per-cluster processes
        procs = [p for c in self.running for p in self.running[c]]
        # Submitter processes
        procs.extend(itertools.chain(*self.submitters.values()))

        for proc in procs:
            rc = proc.check()
            if rc == None :
                self.log.debug("Process %s%s is running", proc.name, ' for %s' % proc.cluster if proc.cluster else '')
            elif proc.cluster and proc.cluster not in activeclusters.keys():
                self.log.info("Not restarting %s for %s as not needed", proc.name, proc.cluster)
                if proc.name == self.condorsubmitter:
                    if proc.cluster in self.submitters:
                        del self.submitters[proc.cluster]
                elif proc.cluster in self.running:
                    del self.running[proc.cluster]
            else:
                self.log.info("Restarting process %s %s", proc.name, 'for %s' % proc.cluster if proc.cluster else '')
                proc.restart()

        # Check for new processes to start
        for cluster in activeclusters:
            if cluster and cluster not in self.running.keys():
                self.running[cluster] = []
                for proc in self.condorprocesses:
                    self.log.info("Starting process %s for %s", proc, cluster)
                    ph = self.aCTProcessHandler(proc, self.logdir, cluster, actlocation=self.actlocation)
                    ph.start()
                    self.running[cluster].append(ph)

        # Get unique list of clusters from cluster lists
        clusterlist = []
        for cluster in clusterlists:
            if not cluster:
                cluster = ''
            clist = cluster.split(',')
            # Here we handle the different formats used by condor
            for c in clist:
                # use only hostname for condor clusters
                cinfo = c.split()
                if not cinfo or len(cinfo) == 1:
                    clusterlist.append(c)
                elif cinfo[0] == 'nordugrid':
                    clusterlist.append(cinfo[-1])
                elif cinfo[0] == 'condor':
                    clusterlist.append(cinfo[-2])
                elif cinfo[0] == 'cream':
                    clusterlist.append(cinfo[1][:cinfo[1].find('/')])
                else:
                    # unknown flavour, just use whole string
                    clusterlist.append(c)

        # Start any new submitters required
        for cluster in clusterlist:
            if cluster not in self.submitters:
                procs = []
                for nprocs in range(int(self.conf.getCond(["sites", "site"], f"endpoint={cluster}", ["submitters"]) or 1)):
                    self.log.info("Starting process aCTSubmitter for %s", cluster)
                    ph = self.aCTProcessHandler(self.condorsubmitter, self.logdir, cluster, actlocation=self.actlocation)
                    ph.start()
                    procs.append(ph)
                self.submitters[cluster] = procs


    class aCTProcessHandler:
        """
        Internal process control class wrapping Popen
        """
        def __init__(self, name, logdir, cluster='', actlocation=''):
            self.name = name
            self.cluster = cluster
            self.child = None
            self.actlocation = actlocation
            # Redirect stdout and stderr to process log
            self.fdout = open(os.path.join(logdir, name[name.rfind('/')+1:]+'.log'), 'a')
        def __del__(self):
            self.kill()
        def start(self):
            self.child = subprocess.Popen(['/usr/bin/env', 'python3', os.path.join(self.actlocation, self.name+".py"), self.cluster], stdout=self.fdout, stderr=subprocess.STDOUT)
        def check(self):
            return self.child.poll()
        def restart(self):
            if self.check() != None:
                self.start()
        def terminate(self):
            if self.child:
                self.child.terminate()
        def kill(self):
            # first kill nicely (SIGTERM)
            self.terminate()
            if self.child and self.check() == None:
                try:
                    print('checking pid', self.child.pid)
                    os.kill(self.child.pid, 0)
                except OSError: # process already exited
                    print('process gone')
                    return
                print('process still running, sleeping')
                aCTUtils.sleep(1)
                # make sure it is gone
                self.child.kill()

