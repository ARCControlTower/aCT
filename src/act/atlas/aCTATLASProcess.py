import time
import os
import sys
import traceback

from act.common import aCTLogger
from act.common import aCTConfig
from act.common import aCTUtils
from act.common.aCTSignal import aCTSignal
from act.arc import aCTDBArc
from act.condor import aCTDBCondor
from act.atlas import aCTCRICParser
from act.atlas import aCTAPFMon
from act.atlas import aCTDBPanda


class aCTATLASProcess:
    '''
    Base class for all ATLAS-specific aCT processes. Sets up logging and configuration and
    provides basic start and stop functionality.
    '''

    def __init__(self, ceflavour=['ARC-CE']):
        # set up signal handlers
        self.signal = aCTSignal()

        # Get agent name from /path/to/aCTAgent.py
        self.name = os.path.basename(sys.argv[0])[:-3]

        # logger
        self.logger=aCTLogger.aCTLogger(self.name)
        self.log=self.logger()
        self.criticallogger = aCTLogger.aCTLogger('aCTCritical', arclog=False)
        self.criticallog = self.criticallogger()

        # config
        self.conf=aCTConfig.aCTConfigAPP()
        self.arcconf=aCTConfig.aCTConfigARC()
        self.tmpdir=str(self.arcconf.get(['tmp', 'dir']))
        # database
        self.dbarc=aCTDBArc.aCTDBArc(self.log)
        self.dbcondor=aCTDBCondor.aCTDBCondor(self.log)
        self.dbpanda=aCTDBPanda.aCTDBPanda(self.log)

        # APFMon
        self.apfmon = aCTAPFMon.aCTAPFMon(self.conf)

        # CRIC info
        self.flavour = ceflavour
        self.cricparser = aCTCRICParser.aCTCRICParser(self.log)
        self.sites = {}
        self.osmap = {}
        self.sitesselect = ''

        # start time for periodic restart
        self.starttime=time.time()
        self.log.info("Started %s", self.name)

    def setSites(self):
        self.sites = self.cricparser.getSites(flavour=self.flavour)
        self.osmap = self.cricparser.getOSMap()
        # For DB queries
        self.sitesselect =  "('%s')" % "','".join(self.sites.keys())

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
                self.conf.parse()
                self.arcconf.parse()
                # do class-specific things
                self.process()
                # sleep
                aCTUtils.sleep(2)
                # restart periodically in case of hangs
                #ip=int(self.conf.get(['periodicrestart', self.name.lower()]))
                #if time.time()-self.starttime > ip and ip != 0 :
                #    self.log.info("%s for %s exited for periodic restart", self.name, self.cluster)
                #    return

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
        os._exit(0)
