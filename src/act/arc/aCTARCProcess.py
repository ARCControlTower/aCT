import os
import random

from act.arc.aCTDBArc import aCTDBArc
from act.common.aCTConfig import aCTConfigARC
from act.common.aCTProcess import aCTProcess
from pyarcrest.arc import ARCRest


class aCTARCProcess(aCTProcess):

    # overriding to demand the cluster argument
    def __init__(self, cluster):
        super().__init__(cluster)

    def loadConf(self):
        self.conf = aCTConfigARC()

    def wait(self, limit=random.randint(5, 11)):
        super().wait(limit)

    def setup(self):
        super().setup()
        self.db = aCTDBArc(self.log)

    def finish(self):
        self.db.close()
        super().finish()

    def getARCClient(self, proxyid):
        proxypath = os.path.join(self.db.proxydir, f"proxiesid{proxyid}")
        try:
            return ARCRest.getClient(url=self.cluster, proxypath=proxypath, log=self.log)
        except Exception as exc:
            self.log.error(f"Error creating REST client for proxy ID {proxyid}: {exc}")
            return None
