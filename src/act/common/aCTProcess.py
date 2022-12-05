import signal
import traceback

from act.common.aCTLogger import aCTLogger
from act.common.aCTSignal import aCTSignalDeferrer


class aCTProcess:

    def __init__(self, cluster=None):
        self.name = self.__class__.__name__
        self.cluster = cluster

    # New process is started only when the process object is called, which is
    # after its construction. Don't know what exactly happens with resources
    # like logger, database connections and file descriptors if they exist
    # before the new process is created (duplication? leaking? unnecessary
    # existence?)
    #
    # That is why all resource init steps are in a new process.
    def setup(self):
        logname = f'{self.name}'
        if self.cluster:
            logname += f'-{self.cluster}'
        self.logger = aCTLogger(logname, cluster=self.cluster)
        self.criticallogger = aCTLogger('aCTCritical', cluster=self.cluster, arclog=False)
        self.log = self.logger()
        self.criticallog = self.criticallogger()

        signal.signal(signal.SIGINT, stopProcess)
        signal.signal(signal.SIGTERM, stopProcess)
        self.sigdefer = aCTSignalDeferrer(self.log, signal.SIGINT, signal.SIGTERM)

        msg = f'Starting process {self.name}'
        if self.cluster:
            msg += f' for cluster {self.cluster}'
        self.log.info(msg)

    def loadConf(self):
        pass

    def wait(self):
        pass

    def process(self):
        pass

    def finish(self):
        msg = f'Stopping process {self.name}'
        if self.cluster:
            msg += f' for cluster {self.cluster}'
        self.log.info(msg)

    def run(self):
        try:
            self.setup()
            while True:
                self.loadConf()
                self.process()
                self.wait()
        except ExitProcessException:
            self.log.info("*** Process exiting normally ***")
        except:
            self.log.critical("*** Unexpected exception! ***")
            self.log.critical(traceback.format_exc())
            self.log.critical("*** Process exiting ***")
            self.criticallog.critical(traceback.format_exc())
        finally:
            self.finish()

def exitHandler():
    stopProcess()


def stopProcess(signum, frame):
    """Throw exception for normal process exit."""
    raise ExitProcessException()


class ExitProcessException(Exception):
    pass
