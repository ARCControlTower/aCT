import time

from act.arc import aCTDBArc
from act.common.aCTConfig import aCTConfigAPP, aCTConfigARC
from act.common.aCTProcess import aCTProcess
from act.ldmx import aCTDBLDMX
from rucio.client import Client


class aCTLDMXProcess(aCTProcess):
    '''
    Base class for LDMX processes. Sub-classes should implement process()
    '''

    # overriding to prevent cluster argument
    def __init__(self):
        super().__init__()

    def loadConf(self):
        self.conf = aCTConfigAPP()
        self.arcconf = aCTConfigARC()

    def wait(self, limit=2):
        super().wait(limit)

    def setup(self):
        super().setup()

        self.loadConf()

        self.tmpdir = self.arcconf.tmp.dir

        # database
        self.dbarc = aCTDBArc.aCTDBArc(self.log)
        self.dbldmx = aCTDBLDMX.aCTDBLDMX(self.log)

        # Rucio client
        self.rucio = Client()

        # start time for periodic restart
        self.starttime = time.time()

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
