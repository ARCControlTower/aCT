import http.client
import os
import signal
import ssl

from urllib.parse import urlparse

# TODO: hardcoded
HTTP_BUFFER_SIZE = 2**23


def getJobParams(args):
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


def readFile(filename):
    try:
        with open(filename, 'r') as f:
            return f.read()
    except Exception as e:
        raise ACTClientError(f'Error reading file {filename}: {e}')


def deleteFile(filename):
    try:
        if os.path.isfile(filename):
            os.remove(filename)
    except Exception as e:
        raise ACTClientError(f'Could not delete results zip {filename}: {e}')


def getHTTPConn(url, sslctx=None, blocksize=HTTP_BUFFER_SIZE):
    try:
        parts = urlparse(url)
        if parts.scheme == 'https':
            conn = PipeFixer(parts.hostname, parts.port, blocksize, True, sslctx)
        elif parts.scheme == 'http':
            conn = PipeFixer(parts.hostname, parts.port, blocksize, False)
        else:
            raise ACTClientError(f'Unsupported URL scheme "{parts.scheme}"')
    except http.client.HTTPException as e:
        raise ACTClientError(f'Error connecting to {parts.hostname}:{parts.port}: {e}')
    return conn


def getWebDAVConn(proxypath, url, blocksize=HTTP_BUFFER_SIZE):
    try:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS)
        context.load_cert_chain(proxypath, keyfile=proxypath)
        _DEFAULT_CIPHERS = (
            'ECDH+AESGCM:DH+AESGCM:ECDH+AES256:DH+AES256:ECDH+AES128:DH+AES:ECDH+HIGH:'
            'DH+HIGH:ECDH+3DES:DH+3DES:RSA+AESGCM:RSA+AES:RSA+HIGH:RSA+3DES:!aNULL:'
            '!eNULL:!MD5'
        )
        context.set_ciphers(_DEFAULT_CIPHERS)
    except Exception as e:
        raise ACTClientError(f'Error creating proxy SSL context: {e}')

    return getHTTPConn(url, sslctx=context)


class PipeFixer(object):
    """
    Duck type around HTTP(S)Connection that reconnects on broken pipe.

    Implements a subset of methods, the ones that are used by aCT client.
    Alternative would be to use higher level library like requests that
    could handle this automatically?
    """

    def __init__(self, host, port, blocksize, ssl=True, context=None):
        self.host = host
        self.port = port
        self.blocksize = blocksize
        self.ssl = ssl
        self.context = context
        self.conn = None
        self._connect()

    def close(self):
        if self.conn:
            self.conn.close()

    def request(self, method, url, **kwargs):
        self._reconnectOnBrokenPipe(self.conn.request, method, url, **kwargs)

    def getresponse(self):
        return self.conn.getresponse()

    def set_debuglevel(self, level):
        self.conn.set_debuglevel(level)

    def _reconnectOnBrokenPipe(self, func, *args, **kwargs):
        try:
            func(*args, **kwargs)
        except BrokenPipeError as e:
            print('Connection lost, reconnecting ...')
            try:
                self._connect()
            except http.client.HTTPException as e:
                print('Failed to reconnect!')
                raise ACTClientError('Could not reconnect: {e}')
            print('Successfully reconnected!')

            try:
                func(*args, **kwargs)
            except http.client.HTTPException as e:
                print('Failed to resend request!')
                raise ACTClientError('Could not resend request: {e}')
            print('Successfully resent request!')

    def _connect(self):
        self.close()

        if self.ssl:
            self.conn = http.client.HTTPSConnection(
                self.host,
                port=self.port,
                blocksize=self.blocksize,
                context=self.context
            )
        else:
            self.conn = http.client.HTTPConnection(
                self.host,
                port=self.port,
                blocksize=self.blocksize
            )


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


# Automatically starts ignoring signal when created and restores signal when
# deleted. It can also explicitly be told to ignore or restore.
class SignalIgnorer(object):

    def __init__(self, signum):
        self.signum = signum
        self.ignore()

    def __del__(self):
        self.restore()

    def ignore(self):
        self.oldHandler = signal.getsignal(self.signum)
        signal.signal(self.signum, signal.SIG_IGN)

    def restore(self):
        signal.signal(self.signum, self.oldHandler)


class ACTClientError(Exception):
    """Base exception of aCT client that has msg string attribute."""

    def __init__(self, msg=''):
        self.msg = msg

    def __str__(self):
        return self.msg
