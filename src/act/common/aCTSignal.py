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
                    self.log.info(f"*** Got exit interrupt on socket ***")
                    return True
        return False
