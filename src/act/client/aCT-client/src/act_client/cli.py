import argparse
import sys

from act_client.common import (ACTClientError, disableSIGINT, getHTTPConn,
                               getJobParams, getWebDAVBase, getWebDAVConn,
                               readFile)
from act_client.config import checkConf, expandPaths, loadConf
from act_client.operations import (aCTJSONRequest, cleanJobs, cleanWebDAV,
                                   fetchJobs, filterJobsToDownload, getJob,
                                   getJobStats, killJobs, resubmitJobs,
                                   submitJobs, uploadProxy)


def addCommonArgs(parser):
    parser.add_argument(
        '--server',
        default=None,
        type=str,
        help='URL of aCT server'
    )
    parser.add_argument(
        '--port',
        default=None,
        type=int,
        help='port of aCT server'
    )
    parser.add_argument(
        '--conf',
        default=None,
        type=str,
        help='path to configuration file'
    )


def addCommonJobFilterArgs(parser):
    parser.add_argument(
        '-a',
        '--all',
        action='store_true',
        help='all jobs that match other criteria'
    )
    parser.add_argument(
        '--id',
        default=None,
        help='a list of IDs of jobs that should be queried'
    )
    parser.add_argument(
        '--name',
        default=None,
        help='substring that jobs should have in name'
    )


def addStateArg(parser):
    parser.add_argument(
        '--state',
        default=None,
        help='perform command only on jobs in given state'
    )


def addWebDAVArg(parser):
    parser.add_argument(
        '--webdav',
        nargs='?',
        const='webdav',
        default='',
        help='URL of user\'s WebDAV directory'
    )


def createParser():
    parser = argparse.ArgumentParser()
    addCommonArgs(parser)

    subparsers = parser.add_subparsers(dest='command')

    parserInfo = subparsers.add_parser(
        'info',
        help='show info about aCT server'
    )

    parserClean = subparsers.add_parser(
        'clean',
        help='clean failed, done and donefailed jobs'
    )
    addCommonJobFilterArgs(parserClean)
    addStateArg(parserClean)
    addWebDAVArg(parserClean)

    parserFetch = subparsers.add_parser(
        'fetch',
        help='fetch failed jobs'
    )
    addCommonJobFilterArgs(parserFetch)

    parserGet = subparsers.add_parser(
        'get',
        help='download results of done and donefailed jobs'
    )
    addCommonJobFilterArgs(parserGet)
    addStateArg(parserGet)
    addWebDAVArg(parserGet)

    parserKill = subparsers.add_parser(
        'kill',
        help='kill jobs'
    )
    addCommonJobFilterArgs(parserKill)
    addStateArg(parserKill)
    addWebDAVArg(parserKill)

    parserProxy = subparsers.add_parser(
        'proxy',
        help='submit proxy certificate'
    )

    parserResub = subparsers.add_parser(
        'resub',
        help='resubmit failed jobs'
    )
    addCommonJobFilterArgs(parserResub)

    parserStat = subparsers.add_parser(
        'stat',
        help='print status for jobs'
    )
    addCommonJobFilterArgs(parserStat)
    addStateArg(parserStat)
    parserStat.add_argument(
        '--arc',
        default='JobID,State,arcstate',
        help='a list of columns from ARC table'
    )
    parserStat.add_argument(
        '--client',
        default='id,jobname',
        help='a list of columns from client table'
    )
    parserStat.add_argument(
        '--get-cols',
        action='store_true',
        help='get a list of possible columns from server'
    )

    parserSub = subparsers.add_parser(
        'sub',
        help='submit job descriptions'
    )
    addWebDAVArg(parserSub)
    parserSub.add_argument(
        '--clusterlist',
        default='default',
        help='a name of a list of clusters specified in config under "clusters" option OR a comma separated list of cluster URLs'
    )
    parserSub.add_argument(
        'xRSL',
        nargs='+',
        help='path to job description file'
    )
    return parser


def runSubcommand(args):
    conf = loadConf(path=args.conf)

    # override values from configuration with command arguments if available
    if args.server:
        conf['server'] = args.server
    if args.port:
        conf['port'] = args.port

    expandPaths(conf)

    if args.command == 'info':
        asyncfun = subcommandInfo
    elif args.command == 'clean':
        asyncfun = subcommandClean
    elif args.command == 'fetch':
        asyncfun = subcommandFetch
    elif args.command == 'get':
        asyncfun = subcommandGet
    elif args.command == 'kill':
        asyncfun = subcommandKill
    elif args.command == 'proxy':
        asyncfun = subcommandProxy
    elif args.command == 'resub':
        asyncfun = subcommandResub
    elif args.command == 'stat':
        asyncfun = subcommandStat
    elif args.command == 'sub':
        asyncfun = subcommandSub

    asyncfun(args, conf)


def main():
    parser = createParser()
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    #runSubcommand(args)
    try:
        runSubcommand(args)
    except ACTClientError as e:
        print(e)
        sys.exit(1)


def subcommandInfo(args, conf):
    checkConf(conf, ['server', 'token'])

    token = readFile(conf['token'])

    disableSIGINT()

    conn = getHTTPConn(conf['server'])
    try:
        jsonDict = aCTJSONRequest(conn, 'GET', '/info', token=token)
    finally:
        conn.close()

    print(f'aCT server URL: {conf["server"]}')
    print('Clusters:')
    for cluster in jsonDict['clusters']:
        print(cluster)


def subcommandClean(args, conf):
    checkConf(conf, ['server', 'token', 'proxy'])

    token = readFile(conf['token'])

    params = {}
    ids = getJobParams(args)
    if ids:
        params['id'] = ids
    if args.state:
        params['state'] = args.state
    if args.name:
        params['name'] = args.name

    disableSIGINT()

    conn = getHTTPConn(conf['server'])
    try:
        jobids = cleanJobs(conn, token, params)
        print(f'Cleaned {len(jobids)} jobs')
        webdavCleanup(args, conf, jobids)
    finally:
        conn.close()


# also closes connections given as params
def webdavCleanup(args, conf, jobids, webdavConn=None, webdavUrl=None):
    if not webdavUrl:
        webdavUrl = getWebDAVBase(args, conf)
    if jobids and webdavUrl:
        print('Cleaning WebDAV directories ...')
        if not webdavConn:
            webdavConn = getWebDAVConn(conf['proxy'], conf['webdav'])
        try:
            errors = cleanWebDAV(webdavConn, webdavUrl, jobids)
            for error in errors:
                print(error)
        finally:
            webdavConn.close()


def subcommandFetch(args, conf):
    checkConf(conf, ['server', 'token'])

    token = readFile(conf['token'])

    params = {}
    ids = getJobParams(args)
    if ids:
        params['id'] = ids
    if args.name:
        params['name'] = args.name

    disableSIGINT()

    conn = getHTTPConn(conf['server'])
    try:
        jsonDict = fetchJobs(conn, token, params)
    finally:
        conn.close()

    print(f'Will fetch {len(jsonDict)} jobs')


def subcommandGet(args, conf):
    checkConf(conf, ['server', 'token'])

    token = readFile(conf['token'])

    kwargs = {}
    ids = getJobParams(args)
    if ids:
        kwargs['id'] = ids
    if args.name:
        kwargs['name'] = args.name
    if args.state:
        kwargs['state'] = args.state

    conn = getHTTPConn(conf['server'])
    toclean = []
    try:
        jobs = filterJobsToDownload(conn, token, **kwargs)
        for job in jobs:
            try:
                dirname = getJob(conn, token, job['c_id'])
            except ACTClientError as e:
                print('Error downloading job {job["c_jobname"]}: {e}')
                continue

            if not dirname:
                print(f'No results for job {job["c_jobname"]}')
            else:
                print(f'Results for job {job["c_jobname"]} stored in {dirname}')
            toclean.append(job["c_id"])

    except KeyboardInterrupt:
        print('Stopping job download ...')

    finally:
        disableSIGINT()

        # reconnect in case KeyboardInterrupt left connection in a weird state
        conn._connect()

        if toclean:
            # clean from aCT
            params = {'id': toclean}
            try:
                toclean = cleanJobs(conn, token, params)
            except ACTClientError as e:
                print(e)
                return
            finally:
                conn.close()

            webdavCleanup(args, conf, toclean)


def subcommandKill(args, conf):
    checkConf(conf, ['server', 'token'])

    token = readFile(conf['token'])

    params = {}
    ids = getJobParams(args)
    if ids:
        params['id'] = ids
    if args.state:
        params['state'] = args.state
    if args.name:
        params['name'] = args.name

    disableSIGINT()

    # kill in aCT
    conn = getHTTPConn(conf['server'])
    try:
        jsonDict = killJobs(conn, token, params)
    finally:
        conn.close()
    print(f'Will kill {len(jsonDict)} jobs')

    # clean in WebDAV
    tokill = [job['c_id'] for job in jsonDict if job['a_id'] is None or job['a_arcstate'] in ('tosubmit', 'submitting')]
    webdavCleanup(args, conf, tokill)


def subcommandProxy(args, conf):
    checkConf(conf, ['server', 'token', 'proxy'])

    proxyStr = readFile(conf['proxy'])

    disableSIGINT()

    conn = getHTTPConn(conf['server'])
    try:
        uploadProxy(conn, proxyStr, conf['token'])
    finally:
        conn.close()

    print(f'Successfully inserted proxy. Access token stored in {conf["token"]}')


def subcommandResub(args, conf):
    checkConf(conf, ['server', 'token'])

    token = readFile(conf['token'])

    params = {}
    ids = getJobParams(args)
    if ids:
        params['id'] = ids
    if args.name:
        params['name'] = args.name

    disableSIGINT()

    conn = getHTTPConn(conf['server'])
    try:
        json = resubmitJobs(conn, token, params)
    finally:
        conn.close()

    print(f'Will resubmit {len(json)} jobs')


def subcommandStat(args, conf):
    checkConf(conf, ['server', 'token'])

    token = readFile(conf['token'])

    disableSIGINT()

    conn = getHTTPConn(conf['server'])
    try:
        if args.get_cols:
            getCols(conn, token)
        else:
            getStats(args, conn, token)
    finally:
        conn.close()


def getCols(conn, token):
    jsonDict = aCTJSONRequest(conn, 'GET', '/info', token=token)
    print('arc columns:')
    print(f'{", ".join(jsonDict["arc"])}')
    print()
    print('client columns:')
    print(f'{", ".join(jsonDict["client"])}')


def getStats(args, conn, token):
    params = {}
    ids = getJobParams(args)
    if ids:
        params['id'] = ids
    if args.arc:
        params['arctab'] = args.arc.split(',')
    if args.client:
        params['clienttab'] = args.client.split(',')
    if args.state:
        params['state'] = args.state
    if args.name:
        params['name'] = args.name

    jsonDict = getJobStats(conn, token, **params)

    if not jsonDict:
        return

    if args.arc:
        arccols = args.arc.split(',')
    else:
        arccols = []
    if args.client:
        clicols = args.client.split(',')
    else:
        clicols = []

    # For each column, determine biggest sized value so that output can
    # be nicely formatted.
    colsizes = {}
    for job in jsonDict:
        for key, value in job.items():
            # All keys have a letter and underscore prepended, which is not
            # used when printing
            colsize = max(len(str(key[2:])), len(str(value)))
            try:
                if colsize > colsizes[key]:
                    colsizes[key] = colsize
            except KeyError:
                colsizes[key] = colsize

    # Print table header
    for col in clicols:
        print(f'{col: <{colsizes["c_" + col]}}', end=' ')
    for col in arccols:
        print(f'{col: <{colsizes["a_" + col]}}', end=' ')
    print()
    line = ''
    for value in colsizes.values():
        line += '-' * value
    line += '-' * (len(colsizes) - 1)
    print(line)

    # Print jobs
    for job in jsonDict:
        for col in clicols:
            fullKey = 'c_' + col
            txt = job.get(fullKey)
            if not txt or str(txt).strip() == '':
                txt = "''"
            print(f'{txt: <{colsizes[fullKey]}}', end=' ')
        for col in arccols:
            fullKey = 'a_' + col
            txt = job.get(fullKey)
            if not txt or str(txt).strip() == '':
                txt = "''"
            print(f'{txt: <{colsizes[fullKey]}}', end=' ')
        print()


def subcommandSub(args, conf):
    checkConf(conf, ['server', 'token'])

    token = readFile(conf['token'])

    if 'clusters' in conf:
        if args.clusterlist in conf['clusters']:
            clusterlist = conf['clusters'][args.clusterlist]
        else:
            clusterlist = args.clusterlist.split(',')
    else:
        clusterlist = args.clusterlist.split(',')

    conn = getHTTPConn(conf['server'])
    webdavConn = None
    webdavUrl = None
    jobs = []
    try:
        if args.webdav:
            webdavConn = getWebDAVConn(conf['proxy'], conf['webdav'])
            webdavUrl = getWebDAVBase(args, conf)
        jobs = submitJobs(conn, token, args.xRSL, clusterlist, webdavConn, webdavUrl)
    except ACTClientError as e:
        print(f'Error submitting jobs: {e}')
    finally:
        disableSIGINT()

        # reconnect in case KeyboardInterrupt left connection in a weird state
        conn._connect()
        if webdavConn:
            webdavConn._connect()

        # print results
        for job in jobs:
            if 'msg' in job:
                if 'name' in job:
                    print(f'Job {job["name"]} not submitted: {job["msg"]}')
                else:
                    print(f'Job description {job["descpath"]} not submitted: {job["msg"]}')
            elif not job['cleanup']:
                print(f'Inserted job {job["name"]} with ID {job["id"]}')

        try:
            # existing webdavConn is closed by webdavCleanup
            submitCleanup(args, conf, conn, token, jobs, webdavConn, webdavUrl)
        finally:
            conn.close()


def submitCleanup(args, conf, conn, token, jobs, webdavConn, webdavUrl):
    # clean jobs that could not be submitted
    tokill = [job['id'] for job in jobs if job['cleanup']]
    if tokill:
        print('Cleaning up failed or cancelled jobs ...')
        params = {'id': tokill}
        jobs = killJobs(conn, token, params)

        # TODO: change API to not return underscored prefix
        toclean = [job['c_id'] for job in jobs]
        webdavCleanup(args, conf, toclean, webdavConn, webdavUrl)
