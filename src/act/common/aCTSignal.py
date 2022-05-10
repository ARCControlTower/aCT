import signal
import socket
from selectors import DefaultSelector, EVENT_READ


EXIT = b"\1"


class aCTSignal:

    def __init__(self):
        self.interruptRead, self.interruptWrite = socket.socketpair()
        self.selector = DefaultSelector()
        self.selector.register(self.interruptRead, EVENT_READ)

        signal.signal(signal.SIGINT, self.signalHandler)
        signal.signal(signal.SIGTERM, self.signalHandler)

    def signalHandler(self, signum, frame):
        self.interruptWrite.send(EXIT)

    def isInterrupted(self):
        for key, _ in self.selector.select(timeout=-1):
            if key.fileobj == self.interruptRead:
                data = self.interruptRead.recv(1)
                if data == EXIT:
                    return True
        return False
