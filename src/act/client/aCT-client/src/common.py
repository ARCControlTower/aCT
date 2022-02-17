import signal
import ssl
import sys

import trio
import httpx
import threading


def addCommonArgs(parser):
    parser.add_argument('--server', default=None, type=str,
            help='URL to aCT server')
    parser.add_argument('--port', default=None, type=int,
            help='port on aCT server')
    parser.add_argument('--conf', default=None, type=str,
            help='path to configuration file')


def addCommonJobFilterArgs(parser):
    parser.add_argument('-a', '--all', action='store_true',
            help='all jobs that match other criteria')
    parser.add_argument('--id', default=None,
            help='a list of IDs of jobs that should be queried')
    parser.add_argument('--name', default=None,
            help='substring that jobs should have in name')


def checkJobParams(args):
    if not args.all and not args.id:
        print("error: no job IDs given (use -a/--all or --id)")
        sys.exit(1)
    elif args.id and not isCorrectIDString(args.id):
        sys.exit(1)


# duplicated from act.client.proxymgr
# Since ARC doesn't seem to complain about non certificate files, should we
# check if given file is actual certificate here?
def readProxyFile(filename):
    try:
        with open(filename, 'r') as f:
            return f.read()
    except Exception as e:
        print('error: read proxy: {}'.format(str(e)))
        sys.exit(1)


# duplicated from act.client.common
def showHelpOnCommandOnly(argparser):
    if len(sys.argv) == 1:
        argparser.print_help()
        sys.exit(0)


# modified from act.client.jobmgr.getIDsFromList
# return boolean that tells whether the id string is OK
def isCorrectIDString(listStr):
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
            except ValueError: # if there is more than one dash
                print('error: invalid ID range: {}'.format(group))
                return False
            try:
                _ = int(firstIx)
            except ValueError:
                print('error: ID range start: {}'.format(firstIx))
                return False
            try:
                _ = int(lastIx)
            except ValueError:
                print('error: ID range end: {}'.format(firstIx))
                return False
        else:
            try:
                _ = int(group)
            except ValueError:
                print('error: invalid ID: {}'.format(group))
                return False
    return True


def readTokenFile(tokenFile):
    try:
        with open(tokenFile, 'r') as f:
            return f.read()
    except Exception as e:
        print('error: read token file: {}'.format(str(e)))
        sys.exit(1)


# optionally accepts session to dcache as kwarg and closes it afterwards
#
# TODO: response from dcache when removing directory is 204. Should we
#       fix on this or on less than 300?
async def clean_webdav(conf, args, jobids, dcclient=None):
    if not jobids:
        return
    if args.dcache and args.dcache != 'dcache':
        dcacheBase = args.dcache
    else:
        dcacheBase = conf.get('dcache', None)
        if dcacheBase is None:
            return

    print('Cleaning WebDAV directories ...')

    if not dcclient:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS)
        context.load_cert_chain(conf['proxy'], keyfile=conf['proxy'])
        # TODO: check if default ssl context is good
        _DEFAULT_CIPHERS = (
            'ECDH+AESGCM:DH+AESGCM:ECDH+AES256:DH+AES256:ECDH+AES128:DH+AES:ECDH+HIGH:'
            'DH+HIGH:ECDH+3DES:DH+3DES:RSA+AESGCM:RSA+AES:RSA+HIGH:RSA+3DES:!aNULL:'
            '!eNULL:!MD5'
        )
        context.set_ciphers(_DEFAULT_CIPHERS)
        client = httpx.AsyncClient(verify=context)
    else:
        client = dcclient

    async with trio.open_nursery() as tasks:
        for jobid in jobids:
            url = dcacheBase + '/' + str(jobid)
            tasks.start_soon(webdav_rmdir, client, url)


async def webdav_rmdir(client, url):
    headers = {'Accept': '*/*', 'Connection': 'Keep-Alive'}
    try:
        resp = await client.delete(url, headers=headers)
    except httpx.RequestError as e:
        print('request error: {}'.format(e))
        return
    # TODO: should we rely on 204 and 404 being the only right answers?
    if resp.status_code == 404:  # ignore, because we are just trying to delete
        return
    if resp.status_code >= 300:
        print('error: cannot remove WebDAV directory {}: {} - {}'.format(url, resp.status_code, resp.text))


async def run_with_sigint_handler(program):
    cancel_scope = trio.CancelScope()

    def sigint_handler(signum, sigframe):
        disableSIGINT()
        cancel_scope.cancel()

    signal.signal(signal.SIGINT, sigint_handler)

    with cancel_scope:
        await program()


def disableSIGINT():
    signal.signal(signal.SIGINT, signal.SIG_IGN)
