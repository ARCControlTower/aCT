import sys
import ssl
import asyncio
import aiohttp


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
# can creation of connector cause errors outside of webdav_rmdir (if it is
# defered until first request)?
async def cleandCache(conf, args, jobids, **kwargs):
    if args.dcache and args.dcache != 'dcache':
        dcacheBase = args.dcache
    else:
        dcacheBase = conf.get('dcache', None)
        if dcacheBase is None:
            return

    session = kwargs.get('session')
    if not session:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS)
        context.load_cert_chain(conf['proxy'], keyfile=conf['proxy'])
        _DEFAULT_CIPHERS = (
            'ECDH+AESGCM:DH+AESGCM:ECDH+AES256:DH+AES256:ECDH+AES128:DH+AES:ECDH+HIGH:'
            'DH+HIGH:ECDH+3DES:DH+3DES:RSA+AESGCM:RSA+AES:RSA+HIGH:RSA+3DES:!aNULL:'
            '!eNULL:!MD5'
        )
        context.set_ciphers(_DEFAULT_CIPHERS)
        connector = aiohttp.TCPConnector(ssl=context)
        session =  aiohttp.ClientSession(connector=connector)

    async with session:
        tasks = []
        for jobid in jobids:
            #await webdav_rmdir(session, dcacheBase + '/' + str(jobid))
            tasks.append(asyncio.ensure_future(webdav_rmdir(session, dcacheBase + '/' + str(jobid))))
        await asyncio.gather(*tasks)


async def webdav_rmdir(session, url):
    headers = {'Accept': '*/*', 'Connection': 'Keep-Alive'}
    try:
        async with session.delete(url, headers=headers) as resp:
            text = await resp.text()
            # TODO: should we rely on 204 and 404 being the only right answers?
            if resp.status == 404: # ignore, because we are just trying to delete
                return
            if resp.status >= 300:
                print('error: cannot remove dCache directory {}: {} - {}'.format(url, resp.status, text))
    except aiohttp.ClientError as e:
        print('HTTP client error: deleting directory {}: {}'.format(url, e))
