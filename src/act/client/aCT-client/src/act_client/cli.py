import argparse
import sys

import trio

from act_client.common import (ACTClientError, checkJobParams, getRESTClient,
                               getWebDAVBase, getWebDAVClient, readFile,
                               runWithSIGINTHandler)
from act_client.config import checkConf, expandPaths, loadConf
from act_client.operations import (cleanJobs, cleanWebDAV, fetchJobs,
                                   filterJobsToDownload, getJob, getJobStats,
                                   killJobs, resubmitJobs, submitJobs,
                                   uploadProxy)


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


# potential code flow:
# - different subcommands could have specific conf override requiring switch
# - path parameters need to be expanded, common for all commands
# - the way to execute commands could be different, requiring another switch,
#   which means duplication
#
# PROBLEM!!!!
# checkConf() call is specific for every command but requires to be run after
# expand paths forcing us to use another switch:
# common -> switch (argument overrides) -> common (expandPaths) ->
# -> checkConf
# UNLESS we put checkConf as part of specific code execution
def runSubcommand(args):
    if args.command in ('clean', 'fetch', 'get', 'kill', 'resub', 'stat'):
        checkJobParams(args)

    conf = loadConf(path=args.conf)

    # override values from configuration with command arguments if available
    if args.server:
        conf['server'] = args.server
    if args.port:
        conf['port'] = args.port

    expandPaths(conf)

    asyncfun = None
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

    #trio.run(runWithSIGINTHandler, asyncfun, args, conf, instruments=[Tracer()])
    trio.run(runWithSIGINTHandler, asyncfun, args, conf)


def main():
    parser = createParser()
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    try:
        runSubcommand(args)
    except ACTClientError as e:
        print(e)
        sys.exit(1)


async def subcommandInfo(args, conf):
    checkConf(conf, ['server', 'port', 'token'])

    token = readFile(conf['token'])

    url = conf['server'] + ':' + str(conf['port'])
    headers = {'Authorization': 'Bearer ' + token}
    async with getRESTClient() as client:
        resp = await client.get(url + '/info', headers=headers)
        json = resp.json()
    if resp.status_code != 200:
        raise ACTClientError(json['msg'])

    print(f'aCT server URL: {url}')
    print('Clusters:')
    for cluster in json['clusters']:
        print(f'{cluster}')


async def subcommandClean(args, conf):
    checkConf(conf, ['server', 'port', 'token', 'proxy'])

    token = readFile(conf['token'])

    url = conf['server'] + ':' + str(conf['port'])
    params = {}
    if args.id:
        params['id'] = args.id
    if args.state:
        params['state'] = args.state
    if args.name:
        params['name'] = args.name

    webdavUrl = getWebDAVBase(args, conf)

    with trio.CancelScope(shield=True):
        client = getRESTClient()
        async with client:
            jobids = await cleanJobs(client, url, token, params)
            print(f'Cleaned {len(jobids)} jobs')
            if jobids and webdavUrl:
                print('Cleaning WebDAV directories ...')
                webdavClient = getWebDAVClient(conf['proxy'])
                async with webdavClient:
                    errors = await cleanWebDAV(webdavClient, webdavUrl, jobids)
                    for error in errors:
                        print(error)


async def subcommandFetch(args, conf):
    checkConf(conf, ['server', 'port', 'token'])

    token = readFile(conf['token'])

    url = conf['server'] + ':' + str(conf['port'])
    params = {}
    if args.id:
        params['id'] = args.id
    if args.name:
        params['name'] = args.name

    client = getRESTClient()
    async with client:
        json = await fetchJobs(client, url, token, params)
    print(f'Will fetch {len(json)} jobs')


async def subcommandGet(args, conf):
    checkConf(conf, ['server', 'port', 'token'])

    token = readFile(conf['token'])

    url = conf['server'] + ':' + str(conf['port'])

    kwargs = {}
    if args.id:
        kwargs['id'] = args.id
    if args.name:
        kwargs['name'] = args.name
    if args.state:
        if args.state not in ('done', 'donefailed'):
            raise ACTClientError('Wrong state parameter, should be "done" or "donefailed"')
        kwargs['state'] = args.state

    toclean = []
    try:
        client = getRESTClient()
        async with client:
            jobs = await filterJobsToDownload(client, url, token, **kwargs)
            async with trio.open_nursery() as tasks:
                for job in jobs:
                    tasks.start_soon(adapterGetJob, client, url, token, job['c_id'], job['c_jobname'], toclean)
    finally:
        with trio.CancelScope(shield=True):
            if toclean:
                # clean from aCT
                client = getRESTClient()
                async with client:
                    params = {'id': ','.join(map(str, toclean))}
                    toclean = await cleanJobs(client, url, token, params)

                # clean from WebDAV
                webdavUrl = getWebDAVBase(args, conf)
                if webdavUrl:
                    print('Cleaning WebDAV directories ...')
                    webdavClient = getWebDAVClient(conf['proxy'])
                    async with webdavClient:
                        errors = await cleanWebDAV(webdavClient, webdavUrl, toclean)
                        for error in errors:
                            print(error)


async def adapterGetJob(client, url, token, jobid, jobname, toclean):
    try:
        dirname = await getJob(client, url, token, jobid)
    except ACTClientError as e:
        print(e)
        return
    if not dirname:
        print(f'No results for job {jobname}')
    else:
        print(f'Results for job {jobname} stored in {dirname}')
    toclean.append(jobid)


async def subcommandKill(args, conf):
    checkConf(conf, ['server', 'port', 'token'])

    token = readFile(conf['token'])

    url = conf['server'] + ':' + str(conf['port'])

    params = {}
    if args.id:
        params['id'] = args.id
    if args.state:
        params['state'] = args.state
    if args.name:
        params['name'] = args.name

    with trio.CancelScope(shield=True):
        # kill in aCT
        client = getRESTClient()
        async with client:
            json = await killJobs(client, url, token, params)
        print(f'Will kill {len(json)} jobs')

        # clean in WebDAV
        tokill = [job['c_id'] for job in json if job['a_id'] is None or job['a_arcstate'] in ('tosubmit', 'submitting')]
        webdavUrl = getWebDAVBase(args, conf)
        if tokill and webdavUrl:
            print('Cleaning WebDAV directories ...')
            webdavClient = getWebDAVClient(conf['proxy'])
            async with webdavClient:
                errors = await cleanWebDAV(webdavClient, webdavUrl, tokill)
                for error in errors:
                    print(error)


async def subcommandProxy(args, conf):
    checkConf(conf, ['server', 'port', 'token', 'proxy'])

    proxyStr = readFile(conf['proxy'])

    url = conf['server'] + ':' + str(conf['port'])

    with trio.CancelScope(shield=True):
        client = getRESTClient()
        async with client:
            await uploadProxy(client, url, proxyStr, conf['token'])

    print(f'Successfully inserted proxy. Access token stored in {conf["token"]}')


async def subcommandResub(args, conf):
    checkConf(conf, ['server', 'port', 'token'])

    token = readFile(conf['token'])

    url = conf['server'] + ':' + str(conf['port'])

    params = {}
    if args.id:
        params['id'] = args.id
    if args.name:
        params['name'] = args.name

    with trio.CancelScope(shield=True):
        client = getRESTClient()
        async with client:
            json = await resubmitJobs(client, url, token, params)

    print(f'Will resubmit {len(json)} jobs')


async def subcommandStat(args, conf):
    checkConf(conf, ['server', 'port', 'token'])

    token = readFile(conf['token'])

    url = conf['server'] + ':' + str(conf['port'])

    params = {}
    if args.id:
        params['id'] = args.id
    if args.arc:
        params['arctab'] = args.arc
    if args.client:
        params['clienttab'] = args.client
    if args.state:
        params['state'] = args.state
    if args.name:
        params['name'] = args.name

    with trio.CancelScope(shield=True):
        client = getRESTClient()
        async with client:
            json = await getJobStats(client, url, token, **params)

    if not json:
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
    for job in json:
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
    for job in json:
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


async def subcommandSub(args, conf):
    checkConf(conf, ['server', 'port', 'token'])

    token = readFile(conf['token'])

    if 'clusters' in conf:
        if args.clusterlist in conf['clusters']:
            clusterlist = conf['clusters'][args.clusterlist]
        else:
            clusterlist = args.clusterlist.split(',')
    else:
        clusterlist = args.clusterlist.split(',')

    url = conf['server'] + ':' + str(conf['port'])

    jobs = []
    try:
        client = getRESTClient()
        webdavUrl = getWebDAVBase(args, conf)
        webdavClient = getWebDAVClient(conf['proxy'])
        async with client, webdavClient:
            jobs = await submitJobs(client, url, token, args.xRSL, clusterlist, webdavClient, webdavUrl)
    finally:
        with trio.CancelScope(shield=True):
            client = getRESTClient()
            webdavUrl = getWebDAVBase(args, conf)
            webdavClient = getWebDAVClient(conf['proxy'])
            async with client, webdavClient:
                await subCleanup(client, url, token, jobs, webdavClient, webdavUrl)


async def subCleanup(client, url, token, jobs, webdavClient, webdavUrl):
    # print results
    for job in jobs:
        if 'msg' in job:
            if 'name' in job:
                print(f'Job {job["name"]} not submitted: {job["msg"]}')
            else:
                print(f'Job description {job["descpath"]} not submitted: {job["msg"]}')
        elif not job['cleanup']:
            print(f'Inserted job {job["name"]} with ID {job["id"]}')
    #return

    # clean jobs that could not be submitted
    tokill = [job['id'] for job in jobs if job['cleanup']]
    if tokill:
        params = {'id': ','.join(map(str, tokill))}
        print('Cleaning up failed or cancelled jobs ...')
        jobs = await killJobs(client, url, token, params)

        jobids = [job['c_id'] for job in jobs]  # TODO: change API to not return underscored prefix
        if jobids and webdavUrl:
            print('Cleaning WebDAV directories ...')
            errors = await cleanWebDAV(webdavClient, webdavUrl, jobids)
            for error in errors:
                print(error)


class Tracer(trio.abc.Instrument):
    def before_run(self):
        print("!!! run started")

    def _print_with_task(self, msg, task):
        # repr(task) is perhaps more useful than task.name in general,
        # but in context of a tutorial the extra noise is unhelpful.
        print(f"{msg}: {task.name}")

    def task_spawned(self, task):
        self._print_with_task("### new task spawned", task)

    def task_scheduled(self, task):
        self._print_with_task("### task scheduled", task)

    def before_task_step(self, task):
        self._print_with_task(">>> about to run one step of task", task)

    def after_task_step(self, task):
        self._print_with_task("<<< task step finished", task)

    def task_exited(self, task):
        self._print_with_task("### task exited", task)

    def before_io_wait(self, timeout):
        if timeout:
            print(f"### waiting for I/O for up to {timeout} seconds")
        else:
            print("### doing a quick check for I/O")
        self._sleep_time = trio.current_time()

    def after_io_wait(self, timeout):
        duration = trio.current_time() - self._sleep_time
        print(f"### finished I/O check (took {duration} seconds)")

    def after_run(self):
        print("!!! run finished")
