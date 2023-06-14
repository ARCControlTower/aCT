import signal
import threading
import traceback
from urllib.parse import urlparse

from act.common.aCTLogger import aCTLogger
from setproctitle import setproctitle


class aCTProcess:
    """
    Base class defining the interface for the aCT process hierarchy.

    The basic idea of the aCT process interface is to consolidate as much of
    shared behaviour and provide ways to change or extend the parts that
    differ by overriding the methods.

    Every aCT process class should inherit from either this class or its
    descendants. The interface defines common functionality that all aCT
    processes should support (can be empty).

    Every instance of the aCT process hierarchy class is callable to be used in
    multiprocessing.Process as an implementation of the managed process.

    An important explanation has to be provided for the use of __init__()
    and setup() methods. __init__() is called on creation of the aCT process
    object in the parent OS process (main aCT process) and passed to the
    multiprocessing.Process as a parameter. multiprocessing.Process spawns a
    new OS process only when its run() method is called which in turn calls the
    provided aCT process object. Therefore __call__() and setup() methods of
    the aCT process object are executed in the new child OS process.

    The multiprocessing documentation is not very clear about what happens to
    certain types of resources, e. g. database connections (do they get closed,
    copied (is it even possible to copy a database connection?)). That is
    why the initialization of as many resources as possible should be pushed
    into the new process which requires another method, setup(). __init__()
    still has to be used for attributes which are required for proper setup.
    Another option for parametrizing setup() could also be to pass args and
    kwargs in the following chain:

    multiprocessing.Process(args=(...), kwargs={...}) ->
    __call__(*args, **kwargs) -> run(*args, *kwargs) -> setup(*args, **kwargs)

    The aCTProcess can exit in two ways:
    - by setting the self.terminate flag (setStopFlag method); the flag is
      checked at least once per main loop and in more involved operations it is
      used more granularly
    - by raising the ExitProcessException (stopWithException method); this one
      should not be used when transaction integrity is required (e. g. between
      modification of multiple tables and executing some operations with
      external systems)

    Process can also be terminated externally with SIGTERM. The signal handler
    uses one of the two methods to eventually stop the process.
    """

    def __init__(self, cluster=''):
        """Set up attributes needed for setup() in spawned OS process."""
        self.name = self.__class__.__name__
        self.cluster = cluster
        self.terminate = threading.Event()

    def __call__(self, *args, **kwargs):
        """Call the method implementing the process code."""
        self.run()

    def setup(self):
        """
        Set up the object in the new process before the main loop starts.

        If configuration loaded by loadConf() is required in overrided method,
        the method needs to call loadConf() first or perform enough setup for
        the loadConf() to be called, call loadConf() and then continue with the
        setup steps requiring the conf. The latter is also the reason why
        loadConf() is not called in the base implementation, it might require
        some setup steps.

        setproctitle() is used to change the name of the process (from actmain)
        so that the process can be identified properly by the aCTReport.py.
        """
        if self.cluster:
            setproctitle(f'{self.name} {self.cluster}')
            url = urlparse(self.cluster)
            logname = f'{self.name}-{url.hostname}'
        else:
            setproctitle(self.name)
            logname = self.name
        self.logger = aCTLogger(logname, cluster=self.cluster)
        self.criticallogger = aCTLogger('aCTCritical', cluster=self.cluster, arclog=False)
        self.log = self.logger()
        self.criticallog = self.criticallogger()

        msg = f'Starting process {self.name}'
        if self.cluster:
            msg += f' for cluster {self.cluster}'
        self.log.info(msg)

        # ignore SIGINT for use case of running aCT from terminal
        signal.signal(signal.SIGINT, signal.SIG_IGN)

        signal.signal(signal.SIGTERM, self.exitHandler)

    def loadConf(self):
        """
        Load process configuration.

        Gets called on every main loop iteration. Can also be used in other
        methods, e. g. overrided setup() method.
        """
        pass

    def wait(self, limit=10):
        """Wait before next iteration of the main loop."""
        self.terminate.wait(limit)

    def process(self):
        """Execute the process' main loop functionality."""
        pass

    def finish(self):
        """Execute cleanup after the main loop exits."""
        msg = f'Stopping process {self.name}'
        if self.cluster:
            msg += f' for cluster {self.cluster}'
        self.log.info(msg)

    def setStopFlag(self):
        """Set flag for process termination."""
        self.terminate.set()

    def stopWithException(self):
        """Raise exception for process termination."""
        self.setStopFlag()
        raise ExitProcessException()

    def isStopFlagSet(self):
        """Return the state of termination flag."""
        return self.terminate.is_set()

    def stopOnFlag(self):
        """Exit with exception on termination flag."""
        if self.isStopFlagSet():
            raise ExitProcessException()

    def run(self):
        """Run the process code."""
        try:
            self.setup()
            while True:
                self.loadConf()
                self.process()
                self.wait()
                self.stopOnFlag()
        except ExitProcessException:
            self.log.info("*** Process exiting normally ***")
        except:
            self.log.critical("*** Unexpected exception! ***")
            self.log.critical(traceback.format_exc())
            self.log.critical("*** Process exiting ***")
            self.criticallog.critical(traceback.format_exc())
        finally:
            self.finish()

    def exitHandler(self, signum, frame):
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        self.setStopFlag()


# The reason for inheriting from BaseException instead of recommended Exception
# is to avoid typical catch-all exception handlers disrupting the required
# behaviour.
class ExitProcessException(BaseException):
    """Exception that indicates normal process exit."""
    pass
