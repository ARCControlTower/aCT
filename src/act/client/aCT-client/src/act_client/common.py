import os
import signal


def getIDParam(args):
    if not args.all and not args.id:
        raise ACTClientError("No job ID given (use -a/--all) or --id")
    elif args.id:
        return getIDsFromStr(args.id)
    else:
        []


# modified from act.client.jobmgr.getIDsFromList
def getIDsFromStr(listStr):
    groups = listStr.split(',')
    ids = []
    for group in groups:
        try:
            group.index('-')
        except ValueError:
            isRange = False
        else:
            isRange = True

        if isRange:
            try:
                firstIx, lastIx = group.split('-')
            except ValueError:  # if there is more than one dash
                raise ACTClientError(f'Invalid ID range: {group}')
            try:
                firstIx = int(firstIx)
            except ValueError:
                raise ACTClientError(f'Invalid ID range start: {firstIx}')
            try:
                lastIx = int(lastIx)
            except ValueError:
                raise ACTClientError(f'Invalid ID range end: {lastIx}')
            ids.extend(range(int(firstIx), int(lastIx) + 1))
        else:
            try:
                ids.append(int(group))
            except ValueError:
                raise ACTClientError(f'Invalid ID: {group}')
    return ids


def deleteFile(filename):
    try:
        if os.path.isfile(filename):
            os.remove(filename)
    except Exception as e:
        raise ACTClientError(f'Could not delete results zip {filename}: {e}')


def getWebDAVBase(args, conf):
    webdavBase = conf.get('webdav', None)
    if args.webdav:
        if args.webdav == 'webdav':  # webdav just as a flag, without URL
            if not webdavBase:
                raise ACTClientError('WebDAV location not configured')
        else:
            webdavBase = args.webdav  # use webdav URL parameter
    return webdavBase


# This does not save the old handler which is necessary if you want to restore
# KeyboardInterrupt.
def disableSIGINT():
    signal.signal(signal.SIGINT, signal.SIG_IGN)


# Inspiration: https://stackoverflow.com/a/21919644
class Signal:

    def __init__(self, signum, callback=None):
        self.callback = callback
        self.oldHandler = None
        self.received = None
        self.defered = False
        self.signum = signum

    def ignore(self):
        self.oldHandler = signal.getsignal(self.signum)
        signal.signal(self.signum, signal.SIG_IGN)

    def deferedHandler(self, signum, frame):
        self.received = (signum, frame)
        if self.callback:
            self.callback()

    def defer(self):
        self.received = None
        self.defered = True
        self.oldHandler = signal.getsignal(self.signum)
        signal.signal(self.signum, self.deferedHandler)

    def restore(self):
        if self.oldHandler is not None:
            signal.signal(self.signum, self.oldHandler)
            if self.defered:
                self.defered = False
                if self.received:
                    self.oldHandler(*self.received)


class ACTClientError(Exception):
    """Base exception of aCT client that has msg string attribute."""

    def __init__(self, msg=''):
        self.msg = msg

    def __str__(self):
        return self.msg
