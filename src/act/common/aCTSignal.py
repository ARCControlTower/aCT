import signal


# TODO: signal.valid_signals() was added in 3.8
# TODO: signal.strsignal() was added in 3.8
class aCTSignalDeferrer:

    def __init__(self, log, *args):
        self.log = log
        self.oldHandlers = {}
        self.received = {}
        self.deferred = False

        self.signals = []
        for sig in args:
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
            #self.log.debug(f"Deferring signal {sig} {signal.strsignal(sig)}")
            self.log.debug(f"Deferring signal {sig}")

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
            # SIG_DFL, SIG_IGN are not callable
            if sigargs is not None and oldHandler is not None and callable(oldHandler):
                oldHandler(*sigargs)
            signal.signal(sig, oldHandler)
            #self.log.debug(f"Restoring signal {sig} {signal.strsignal(sig)}")
            self.log.debug(f"Restoring signal {sig}")

    def __enter__(self):
        self.defer()

    def __exit__(self, exc_type, exc_value, exc_tb):
        self.restore()
