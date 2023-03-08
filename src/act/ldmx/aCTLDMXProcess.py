import time
import os
import sys
import traceback
from rucio.client import Client

from act.common import aCTLogger
from act.common.aCTConfig import aCTConfigAPP, aCTConfigARC
from act.common import aCTUtils
from act.common.aCTSignal import aCTSignal
from act.arc import aCTDBArc
from act.ldmx import aCTDBLDMX

class aCTLDMXProcess:
    '''
    Base class for LDMX processes. Sub-classes should implement process()
    '''

    def __init__(self):
        # Get agent name from /path/to/aCTAgent.py
        self.name = os.path.basename(sys.argv[0])[:-3]

        # logger
        self.logger = aCTLogger.aCTLogger(self.name)
        self.log = self.logger()
        self.criticallogger = aCTLogger.aCTLogger('aCTCritical', arclog=False)
        self.criticallog = self.criticallogger()

        # set up signal handlers
        self.signal = aCTSignal(self.log)

        # config
        self.conf = aCTConfigAPP()
        self.arcconf = aCTConfigARC()
        self.tmpdir = self.arcconf.tmp.dir
        # database
        self.dbarc = aCTDBArc.aCTDBArc(self.log)
        self.dbldmx = aCTDBLDMX.aCTDBLDMX(self.log)
        # Rucio client
        self.rucio = Client()

        # start time for periodic restart
        self.starttime = time.time()
        self.log.info("Started %s", self.name)

    def setSites(self):
        '''
        Set map of sites, CEs and status
        '''
        self.sites = {}
        self.endpoints = {}  # Map of CE to site name
        self.rses = {}  # Map of RSE to CE endpoint
        for sitename, site in self.arcconf.sites:
            if not site.endpoint:
                self.log.error(f"\"endpoint\" not in config for site {sitename}")
            elif not site.rse:
                self.log.error(f"\"rse\" not in config for site {sitename}")
            else:
                siteinfo = {}
                siteinfo["endpoint"] = site.endpoint
                siteinfo["rse"] = site.rse
                self.sites[sitename] = siteinfo
                self.endpoints[site.endpoint] = sitename
                if "status" not in site or site.status != "offline":
                    self.rses[site.rse] = site.endpoint

    def process(self):
        '''
        Called every loop during the main loop. Subclasses must implement this
        method with their specific operations.
        '''
        pass

    def run(self):
        '''
        Main loop
        '''
        try:
            while 1:
                # parse config file
                self.conf = aCTConfigAPP()
                self.arcconf = aCTConfigARC()
                self.setSites()
                # do class-specific things
                self.process()
                # sleep
                aCTUtils.sleep(2)

                if self.signal.isInterrupted():
                    self.log.info("*** Exiting on exit interrupt ***")
                    break

        except:
            self.log.critical("*** Unexpected exception! ***")
            self.log.critical(traceback.format_exc())
            self.log.critical("*** Process exiting ***")
            self.criticallog.critical(traceback.format_exc())

        finally:
            self.finish()

    def finish(self):
        '''
        Clean up code when process exits
        '''
        self.log.info("Cleanup for %s", self.name)
