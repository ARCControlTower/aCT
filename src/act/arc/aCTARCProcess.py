import os
import logging
import random

from act.arc.aCTDBArc import aCTDBArc
from act.common.aCTConfig import aCTConfigARC
from act.common.aCTProcess import aCTProcess
from act.common.aCTLogger import LEVELS
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

        # see docstring of aCTProcess.setup() for why loadConf is required
        #
        # the pyarcrest logs will be without cluster for now, some effort
        # is required to add cluster URL to logs
        self.loadConf()
        pyarcrestLogger = logging.getLogger("pyarcrest")
        level = LEVELS.get(self.conf.logger.level or logging.NOTSET)
        pyarcrestLogger.setLevel(level)

        # the object nesting of handlers var is this:
        # self.aCTLogger.LoggerAdapter.Logger.handlers
        for handler in self.logger.logger.logger.handlers:
            handler.addFilter(self.logFilter)
            pyarcrestLogger.addHandler(handler)

    def logFilter(self, record):
        """
        Add cluster attribute to log record.

        aCT logging infrastructure adds a custom value to log messages and
        format to include cluster URL in logs. LoggerAdapter is used for that.
        However, this adapter cannot be used for the pyarcrest logger.

        This method is a custom filter that is added to pyarcrest logger
        handlers to add cluster attribute to all log records.
        """
        record.cluster = self.cluster
        return True

    def finish(self):
        self.db.close()
        super().finish()

    def getARCClient(self, proxyid):
        proxypath = os.path.join(self.db.proxydir, f"proxiesid{proxyid}")
        try:
            return ARCRest.getClient(url=self.cluster, proxypath=proxypath, timeout=900)
        except Exception as exc:
            self.log.error(f"Error creating REST client for proxy ID {proxyid}: {exc}")
            return None
