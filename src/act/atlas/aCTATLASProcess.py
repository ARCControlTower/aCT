import time

from act.atlas.aCTAPFMon import aCTAPFMon
from act.atlas.aCTCRICParser import aCTCRICParser
from act.atlas.aCTDBPanda import aCTDBPanda
from act.common.aCTConfig import aCTConfigAPP, aCTConfigARC
from act.common.aCTProcess import aCTProcess


class aCTATLASProcess(aCTProcess):
    '''
    Base class for all ATLAS-specific aCT processes. Sets up logging and configuration and
    provides basic start and stop functionality.
    '''

    def __init__(self, ceflavour=['ARC-CE']):
        super().__init__()
        self.ceflavour = ceflavour

    def loadConf(self):
        self.conf = aCTConfigAPP()
        self.arcconf = aCTConfigARC()

    def wait(self, limit=2):
        super().wait(limit)

    def setup(self):
        super().setup()

        # config
        self.loadConf()

        self.tmpdir = self.arcconf.tmp.dir

        # database
        self.db = aCTDBPanda(self.log)

        # APFMon
        self.apfmon = aCTAPFMon(self.conf)

        # CRIC info
        self.cricparser = aCTCRICParser(self.log)
        self.sites = {}
        self.sitesselect = []

        # start time for periodic restart
        self.starttime = time.time()

    def setSites(self):
        self.sites = self.cricparser.getSites(flavour=self.ceflavour)
        # For DB queries
        self.sitesselect = list(self.sites.keys())

    def finish(self):
        super().finish()
