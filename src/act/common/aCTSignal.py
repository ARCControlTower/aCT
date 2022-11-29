import signal
import socket
from selectors import DefaultSelector, EVENT_READ


EXIT = b"\1"


class aCTSignal:

    def __init__(self, logger):
        self.log = logger
        self.interruptRead, self.interruptWrite = socket.socketpair()
        self.selector = DefaultSelector()
        self.selector.register(self.interruptRead, EVENT_READ)

        signal.signal(signal.SIGINT, self.signalHandler)
        signal.signal(signal.SIGTERM, self.signalHandler)

    def signalHandler(self, signum, frame):
        self.log.info(f"*** Handling signal {signum} ***")
        self.interruptWrite.send(EXIT)

    def isInterrupted(self):
        for key, _ in self.selector.select(timeout=-1):
            if key.fileobj == self.interruptRead:
                data = self.interruptRead.recv(1)
                if data == EXIT:
                    self.log.info("*** Got exit interrupt on socket ***")
                    return True
        return False


class aCTSignalDeferrer:

    def __init__(self, log, *args):
        self.log = log
        self.oldHandlers = {}
        self.received = {}
        self.deferred = False

        self.signals = []
        for sig in args:
            # signal.valid_signals() was added in 3.8
            # signal.strsignal() was added in 3.8
            # relying on us using the right signal for now
            #if sig not in signal.valid_signals():
            #    raise ValueError(f"Given value {sig} is not a valid signal")
            #else:
            #    self.signals.append(sig)
            #    self.log.debug(f"Will handle signal {sig} {signal.strsignal(sig)}")
            self.signals.append(sig)
            self.log.debug(f"Will handle signal {sig}")

    def defer(self):
        if self.deferred:
            return
        self.deferred = True
        for sig in self.signals:
            self.oldHandlers[sig] = signal.getsignal(sig)
            signal.signal(sig, self.deferredHandler)
            self.log.debug(f"Deferring signal {sig} {signal.strsignal(sig)}")

    # if signal is received multiple times, only the last frame will be saved
    def deferredHandler(self, signum, frame):
        self.received[signum] = (signum, frame)

    def restore(self):
        if not self.deferred:
            return
        self.deferred = False
        for sig in self.signals:
            oldHandler = self.oldHandlers.pop(sig, None)
            sigargs = self.received.pop(sig, None)
            if sigargs is not None and oldHandler is not None:
                oldHandler(*sigargs)
            signal.signal(sig, oldHandler)
            self.log.debug(f"Restoring signal {sig} {signal.strsignal(sig)}")

    def __enter__(self):
        self.defer()

    def __exit__(self, exc_type, exc_value, exc_tb):
        self.restore()
