import signal


class aCTSignalDeferrer:
    """
    Provides mechanism for deferring signals.

    In general, proper exit strategies have to be determined for every well
    behaved program. The term "exit strategy" is used here to refer to the
    steps that are required to fullfill the program's specification correctly
    for any condition in which the program needs to terminate. It is of course
    highly context dependent, different for every program and even for many
    different components of the same program.

    Since aCT runs as a swarm of processes and a system service, the processes
    need to do proper signal handling and implement exit strategies for
    signals.  In Python, the signal handler is a function that the interpreter
    executes when the signal is received. There are roughly two ways to end the
    process in a controlled way when the signal is received.

    First, the signal handler can set some global flag that is used across the
    codebase to implement exit strategies. "granularity" of flag handling can
    become a challenge. If the reaction to the signal has to be done in a tight
    time frame it might increase the amount of flag handling code
    significantly. It can also increase the complexity of the operations as
    they now require additional (and potentially very granular) handling of
    exit strategies.

    The second approach is to throw exception in the signal handler (e. g. the
    default SIGINT handler in Python throws KeyboardInterrupt exception).
    This mechanism can have advantages over the first one. It can interrupt a
    block of code at any point. Provided that the exit strategies can be
    handled elegant enough with the exception mechanism, this approach can be
    simpler, more flexible and less verbose than the first one. The downside
    can be the handling of the parts of code that should not be interrupted by
    an exception.

    aCT uses the second approach. Transactional DB operations can combine
    elegantly with exceptions (see aCTTransacton class from aCTProcess.py).
    Other operations which are not transactional, e. g. interaction with the
    filesystem and external systems like ARC and Panda, need to carefully
    maintain the proper state in those systems and not leak their resources.
    That means that certain parts need to execute without being interrupted by
    the exception raised by the signal that can be received at any time (as has
    to be an assumption for OS signals).

    That is the purpose of an instance of this class. An instance is a context
    manager that can be used in a with statement. When the context is entered,
    it will save the current handlers for managed signals and install its own
    handler that stores the received signal. At the context exit, the previous
    handlers are restored and called for saved signals.
    """

    def __init__(self, log, *args):
        self.log = log
        self.oldHandlers = {}  # storage for deferred handlers
        self.received = {}  # storage for received signals
        self.level = 0  # variable to handle nesting

        self.signals = []
        for sig in args:
            # assuming that the developer is using the right signals
            # TODO: signal.valid_signals() was added in 3.8
            # TODO: signal.strsignal() was added in 3.8
            #if sig not in signal.valid_signals():
            #    raise ValueError(f"Given value {sig} is not a valid signal")
            #else:
            #    self.signals.append(sig)
            #    self.log.debug(f"Will handle signal {sig} {signal.strsignal(sig)}")
            self.signals.append(sig)
            self.log.debug(f"Will handle signal {sig}")

    def defer(self):
        self.level += 1
        if self.level != 1:
            return
        for sig in self.signals:
            self.oldHandlers[sig] = signal.getsignal(sig)
            signal.signal(sig, self.deferredHandler)
            # TODO: signal.strsignal() was added in 3.8
            #self.log.debug(f"Deferring signal {sig} {signal.strsignal(sig)}")
            self.log.debug(f"Deferring signal {sig}")

    # if signal is received multiple times, only the last frame will be saved
    def deferredHandler(self, signum, frame):
        self.received[signum] = (signum, frame)

    def restore(self):
        self.level -= 1
        if self.level != 0:
            return
        for sig in self.signals:
            oldHandler = self.oldHandlers.pop(sig, None)
            sigargs = self.received.pop(sig, None)
            # SIG_DFL, SIG_IGN are not callable
            if sigargs is not None and oldHandler is not None and callable(oldHandler):
                oldHandler(*sigargs)
            signal.signal(sig, oldHandler)
            # TODO: signal.strsignal() was added in 3.8
            #self.log.debug(f"Restoring signal {sig} {signal.strsignal(sig)}")
            self.log.debug(f"Restoring signal {sig}")

    def __enter__(self):
        self.defer()

    def __exit__(self, exc_type, exc_value, exc_tb):
        self.restore()
