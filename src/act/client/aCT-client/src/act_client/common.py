import signal
import ssl

import httpx
import trio


def checkJobParams(args):
    if not args.all and not args.id:
        raise ACTClientError("No job ID given (use -a/--all) or --id")
    if args.id:
        checkIDString(args.id)


# modified from act.client.jobmgr.getIDsFromList
def checkIDString(listStr):
    groups = listStr.split(',')
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
                _ = int(firstIx)
            except ValueError:
                raise ACTClientError(f'Invalid ID range start: {firstIx}')
            try:
                _ = int(lastIx)
            except ValueError:
                raise ACTClientError(f'Invalid ID range end: {lastIx}')
        else:
            try:
                _ = int(group)
            except ValueError:
                raise ACTClientError(f'Invalid ID: {group}')


def readFile(filename):
    try:
        with open(filename, 'r') as f:
            return f.read()
    except Exception as e:
        raise ACTClientError(f'Error reading file {filename}: {e}')


def getWebDAVClient(proxypath):
    try:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS)
        context.load_cert_chain(proxypath, keyfile=proxypath)
        _DEFAULT_CIPHERS = (
            'ECDH+AESGCM:DH+AESGCM:ECDH+AES256:DH+AES256:ECDH+AES128:DH+AES:ECDH+HIGH:'
            'DH+HIGH:ECDH+3DES:DH+3DES:RSA+AESGCM:RSA+AES:RSA+HIGH:RSA+3DES:!aNULL:'
            '!eNULL:!MD5'
        )
        context.set_ciphers(_DEFAULT_CIPHERS)
        limits = httpx.Limits(max_keepalive_connections=1, max_connections=1)
        timeout = httpx.Timeout(5.0, pool=None)
        client = httpx.AsyncClient(verify=context, timeout=timeout, limits=limits)
    except Exception as e:
        raise ACTClientError(f'Error creating proxy SSL context: {e}')
    return client


def getRESTClient():
    limits = httpx.Limits(max_keepalive_connections=1, max_connections=1)
    timeout = httpx.Timeout(5.0, pool=None)
    client = httpx.AsyncClient(timeout=timeout, limits=limits)
    return client


def getWebDAVBase(args, conf):
    webdavBase = conf.get('webdav', None)
    if args.webdav:
        if args.webdav == 'webdav':  # webdav just as a flag, without URL
            if not webdavBase:
                raise ACTClientError('WebDAV location not configured')
        else:
            webdavBase = args.webdav  # use webdav URL parameter
    return webdavBase


async def runWithSIGINTHandler(program, *args):
    cancel_scope = trio.CancelScope()

    def sigint_handler(signum, sigframe):
        disableSIGINT()
        cancel_scope.cancel()

    signal.signal(signal.SIGINT, sigint_handler)

    with cancel_scope:
        await program(*args)


def disableSIGINT():
    signal.signal(signal.SIGINT, signal.SIG_IGN)


class ACTClientError(Exception):
    """Base exception of aCT client that has msg string attribute."""

    def __init__(self, msg=''):
        self.msg = msg

    def __str__(self):
        return self.msg


class JobCleanup(ACTClientError):
    """
    Raised with a list of jobs and exception to raise.

    Jobs are dicts, those with 'cleanup' set to True should be cleaned up.
    """

    def __init__(self, cleanup, exception):
        super().__init__(str(exception))
        self.cleanup = cleanup
        self.exception = exception

    def __str__(self):
        return f'Jobs for cleanup {self.cleanup} after error: {self.msg}'


class ExitProgram(ACTClientError):
    """Exception raised to exit program with message and return code."""

    def __init__(self, msg='', code=0):
        super().__init__(msg)
        self.code = code
