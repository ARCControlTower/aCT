import random
import time

from act.arc.aCTDBArc import aCTDBArc
from act.common.aCTConfig import aCTConfigARC
from act.common.aCTProcess import aCTProcess


class aCTARCProcess(aCTProcess):

    # overriding to demand the cluster argument
    def __init__(self, cluster):
        super().__init__(cluster)

    def loadConf(self):
        self.conf = aCTConfigARC()

    def wait(self):
        time.sleep(random.randint(5, 11))

    def setup(self):
        super().setup()
        self.db = aCTDBArc(self.log)

    def finish(self):
        self.db.close()
        super().finish()
