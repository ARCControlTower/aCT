import signal
import traceback
from urllib.parse import urlparse

from act.common.aCTLogger import aCTLogger
from act.common.aCTSignal import aCTSignalDeferrer


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

    multiprocessing.Process(args, kwargs) -> __call__(*args, **kwargs) ->
    run(*args, *kwargs) -> setup(*args, **kwargs)

    The aCTProcess can exit in two ways:
    - unhandled exception
    - ExitProcessException is raised either by the process itself when it wants
      to stop or by the signal handler when it is stopped externally by aCT
      process manager or OS. More information on signals and exit mechanism
      can be found in aCTSignal.py
    """

    def __init__(self, cluster=''):
        """Set up attributes needed for setup() in spawned OS process."""
        self.name = self.__class__.__name__
        self.cluster = cluster

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
        """
        logname = f'{self.name}'
        if self.cluster:
            url = urlparse(self.cluster)
            logname += f'-{url.hostname}'
        self.logger = aCTLogger(logname, cluster=self.cluster)
        self.criticallogger = aCTLogger('aCTCritical', cluster=self.cluster, arclog=False)
        self.log = self.logger()
        self.criticallog = self.criticallogger()

        # ignore SIGINT for use case of running aCT from terminal
        signal.signal(signal.SIGINT, signal.SIG_IGN)

        signal.signal(signal.SIGTERM, exitHandler)
        self.sigdefer = aCTSignalDeferrer(self.log, signal.SIGTERM)

        msg = f'Starting process {self.name}'
        if self.cluster:
            msg += f' for cluster {self.cluster}'
        self.log.info(msg)

    def loadConf(self):
        """
        Load process configuration.

        Gets called on every main loop iteration. Can also be used in other
        methods, e. g. overrided setup() method.
        """
        pass

    def wait(self):
        """Wait before next iteration of the main loop."""
        pass

    def process(self):
        """Execute the process' main loop functionality."""
        pass

    def finish(self):
        """Execute cleanup after the main loop exits."""
        msg = f'Stopping process {self.name}'
        if self.cluster:
            msg += f' for cluster {self.cluster}'
        self.log.info(msg)

    def stop(self):
        """Call function for normal process exit."""
        stopProcess()

    def run(self):
        """Run the process code."""
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

    def transaction(self, dbobjects):
        """Return transaction with process' logger and signal deferrer."""
        return aCTTransaction(self.log, self.sigdefer, dbobjects)


def exitHandler(signum, frame):
    """Call function for normal process exit."""
    stopProcess()


def stopProcess():
    """Throw exception for normal process exit."""
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    raise ExitProcessException()


class ExitProcessException(Exception):
    """Exception that indicates normal process exit."""
    pass


class aCTTransaction:
    """
    Encapsulates transaction handling for multiple databases.

    There are two main reasons for this abstraction. Firstly, most of aCT
    operations are performed on the batches of jobs. For better efficiency, the
    table manipulations can be done in a single transaction for the entire
    batch or even multiple batches instead of per job.

    Secondly, the way DB operations are done in aCT is that there is a separate
    object doing separate operations for every database table. This prevents
    multiple tables being updated in a single DB transaction for consistency.
    Instead, there is (at least) one transaction per table. If there is an
    interruption between the commits of multiple transactions, the state as
    reflected by the tables might not be consistent anymore.

    Because most parts of aCT can be interrupted asynchronously with exception
    (see aCTSignal.py for longer explanation), there needs to be a proper
    handling of such exceptions to properly structure database transactions.
    This is provided by an instance of this class that acts as a context
    manager that commits transaction if no exception was raised or rolls it
    back otherwise. The commit must not be interrupted so the signals that
    could interrupt the process are deferred with a handler.
    """

    def __init__(self, log, sigdefer, dbobjects):
        """
        Initialize transaction.

        Every transaction is done on one or more aCTDB objects.
        """
        self.log = log
        self.sigdefer = sigdefer
        self.dbobjects = dbobjects
        if len(self.dbobjects) <= 0:
            raise Exception("No database objects for transaction")

    def __enter__(self):
        """Log the beginning of transaction."""
        self.log.debug("Starting transaction")

    def __exit__(self, exc_type, exc_value, exc_tb):
        """
        Commit or rollback transactions based on exception parameters.

        If no exceptions were raised in the context, the parameters will all be
        None. Falsy value must be returned (None if no explicit return) to pass
        exceptions on.
        """
        with self.sigdefer:
            if exc_type is None:
                for db in self.dbobjects:
                    db.Commit()
                self.log.debug("Committing transaction")
            else:
                if exc_type is ExitProcessException:
                    self.log.debug("Rolling back transaction on exit")
                else:
                    self.log.debug(f"Rolling back transaction on unhandled exception: {exc_value}")
                for db in self.dbobjects:
                    db.db.conn.rollback()
