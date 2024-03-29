import argparse
import sys

from act_client.common import (ACTClientError, disableSIGINT, getIDParam,
                               getWebDAVBase, readFile)
from act_client.config import checkConf, expandPaths, loadConf
from act_client.operations import getACTRestClient, getWebDAVClient


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
        default=[],
        help='a list of IDs of jobs that should be queried'
    )
    parser.add_argument(
        '--name',
        default='',
        help='substring that jobs should have in name'
    )


def addStateArg(parser):
    parser.add_argument(
        '--state',
        default='',
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
        help='a comma separated list of columns from ARC table'
    )
    parserStat.add_argument(
        '--client',
        default='id,jobname',
        help='a comma separated list of columns from client table'
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
        commandFun = subcommandInfo
    elif args.command == 'clean':
        commandFun = subcommandClean
    elif args.command == 'fetch':
        commandFun = subcommandFetch
    elif args.command == 'get':
        commandFun = subcommandGet
    elif args.command == 'kill':
        commandFun = subcommandKill
    elif args.command == 'proxy':
        commandFun = subcommandProxy
    elif args.command == 'resub':
        commandFun = subcommandResub
    elif args.command == 'stat':
        commandFun = subcommandStat
    elif args.command == 'sub':
        commandFun = subcommandSub

    commandFun(args, conf)


def main():
    parser = createParser()
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    try:
        runSubcommand(args)
    except KeyboardInterrupt:
        sys.exit(1)
    except ACTClientError as exc:
        print(exc)
        sys.exit(1)


def subcommandInfo(args, conf):
    checkConf(conf, ['server', 'token'])

    actrest = getACTRestClient(conf)
    try:
        jsonData, status = actrest.getInfo()
        if status != 200:
            raise ACTClientError(jsonData["msg"])
    except Exception as exc:
        raise ACTClientError(f"Error fetching info from aCT server: {exc}")
    else:
        print(f'aCT server URL: {conf["server"]}')
        print('Clusters:')
        for cluster in jsonData['clusters']:
            print(cluster)
    finally:
        actrest.close()


def subcommandClean(args, conf):
    checkConf(conf, ['server', 'token', 'proxy'])

    actrest = getACTRestClient(conf)
    ids = getIDParam(args)
    try:
        disableSIGINT()
        jobids = actrest.cleanJobs(jobids=ids, name=args.name, state=args.state)
        print(f'Cleaned {len(jobids)} jobs')
    except Exception as exc:
        raise ACTClientError(f'Error cleaning jobs: {exc}')
    finally:
        actrest.close()

    webdavCleanup(args, conf, jobids)


def webdavCleanup(args, conf, jobids, webdavClient=None, webdavBase=None):
    if not jobids:
        return
    if not webdavBase:
        webdavBase = getWebDAVBase(args, conf)
        if not webdavBase:
            return

    print('Cleaning WebDAV directories ...')
    if webdavClient:
        closeWebDAV = False
    else:
        webdavClient = getWebDAVClient(conf, webdavBase)
        closeWebDAV = True
    try:
        errors = webdavClient.cleanJobDirs(webdavBase, jobids)
        for error in errors:
            print(error)
    except Exception as exc:
        raise ACTClientError(f'Error cleaning up WebDAV dirs: {exc}')
    finally:
        if closeWebDAV:
            webdavClient.close()


def subcommandFetch(args, conf):
    checkConf(conf, ['server', 'token'])

    actrest = getACTRestClient(conf)
    ids = getIDParam(args)
    try:
        jsonData = actrest.fetchJobs(jobids=ids, name=args.name)
    except Exception as exc:
        raise ACTClientError(f'Error fetching jobs: {exc}')
    finally:
        actrest.close()

    print(f'Will fetch {len(jsonData)} jobs')


def subcommandGet(args, conf):
    checkConf(conf, ['server', 'token'])

    actrest = getACTRestClient(conf)
    ids = getIDParam(args)
    toclean = []
    try:
        jobs = actrest.getDownloadableJobs(jobids=ids, name=args.name, state=args.state)
        for job in jobs:
            try:
                dirname = actrest.downloadJobResults(job['c_id'])
            except Exception as e:
                print('Error downloading job {job["c_jobname"]}: {e}')
                continue

            if not dirname:
                print(f'No results for job {job["c_jobname"]}')
            else:
                print(f'Results for job {job["c_jobname"]} stored in {dirname}')
            toclean.append(job["c_id"])
    except Exception as exc:
        raise ACTClientError(f'Error downloading jobs: {exc}')
    except KeyboardInterrupt:
        print('Stopping job download ...')
    finally:
        disableSIGINT()

        # reconnect in case KeyboardInterrupt left connection in a weird state
        actrest.close()

        if toclean:
            try:
                toclean = actrest.cleanJobs(jobids=toclean)
            except Exception as exc:
                raise ACTClientError(f'Error cleaning up downloaded jobs: {exc}')
            finally:
                actrest.close()

            webdavCleanup(args, conf, toclean)


def subcommandKill(args, conf):
    checkConf(conf, ['server', 'token'])

    actrest = getACTRestClient(conf)
    ids = getIDParam(args)
    try:
        disableSIGINT()
        jsonData = actrest.killJobs(jobids=ids, name=args.name, state=args.state)
    except Exception as exc:
        raise ACTClientError(f'Error killing jobs: {exc}')
    finally:
        actrest.close()
    print(f'Will kill {len(jsonData)} jobs')

    # clean in WebDAV
    tokill = [job['c_id'] for job in jsonData if job['a_id'] is None or job['a_arcstate'] in ('tosubmit', 'submitting')]
    webdavCleanup(args, conf, tokill)


def subcommandProxy(args, conf):
    checkConf(conf, ['server', 'token', 'proxy'])

    actrest = getACTRestClient(conf, useToken=False)
    proxyStr = readFile(conf['proxy'])
    try:
        disableSIGINT()
        actrest.uploadProxy(proxyStr, conf['token'])
    except Exception as exc:
        raise ACTClientError(f'Error uploading proxy: {exc}')
    finally:
        actrest.close()

    print(f'Successfully inserted proxy. Access token stored in {conf["token"]}')


def subcommandResub(args, conf):
    checkConf(conf, ['server', 'token'])

    actrest = getACTRestClient(conf)
    ids = getIDParam(args)
    try:
        jsonData = actrest.resubmitJobs(jobids=ids, name=args.name)
    except Exception as exc:
        raise ACTClientError(f'Error resubmitting jobs: {exc}')
    finally:
        actrest.close()

    print(f'Will resubmit {len(jsonData)} jobs')


def subcommandStat(args, conf):
    checkConf(conf, ['server', 'token'])

    actrest = getACTRestClient(conf)
    try:
        if args.get_cols:
            getCols(actrest)
        else:
            getStats(args, actrest)
    finally:
        actrest.close()


def getCols(actrest):
    try:
        jsonData = actrest.getInfo()
    except Exception as exc:
        raise ACTClientError(f"Error fetching info from aCT server: {exc}")

    print('arc columns:')
    print(f'{",".join(jsonData["arc"])}')
    print()
    print('client columns:')
    print(f'{",".join(jsonData["client"])}')


def getStats(args, actrest):
    ids = getIDParam(args)
    try:
        jsonData = actrest.getJobStats(
            jobids=ids,
            name=args.name,
            state=args.state,
            clienttab=args.client.split(','),
            arctab=args.arc.split(',')
        )
    except Exception as exc:
        raise ACTClientError(f'Error fetching job status: {exc}')

    if not jsonData:
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
    for job in jsonData:
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
    for job in jsonData:
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

    if 'clusters' in conf:
        if args.clusterlist in conf['clusters']:
            clusterlist = conf['clusters'][args.clusterlist]
        else:
            clusterlist = args.clusterlist.split(',')
    else:
        clusterlist = args.clusterlist.split(',')

    actrest = getACTRestClient(conf)
    webdavClient = None
    webdavBase = None
    jobs = []
    try:
        if args.webdav:
            webdavBase = getWebDAVBase(args, conf)
            webdavClient = getWebDAVClient(conf, webdavBase)
        jobs = actrest.submitJobs(args.xRSL, clusterlist, webdavClient, webdavBase)
    except Exception as exc:
        raise ACTClientError(f'Error submitting jobs: {exc}')
    finally:
        disableSIGINT()

        # reconnect in case KeyboardInterrupt left connection in a weird state
        actrest.close()
        if webdavClient:
            webdavClient.close()

        # print results
        for job in jobs:
            if 'msg' in job:
                if 'name' in job:
                    print(f'Job {job["name"]} not submitted: {job["msg"]}')
                else:
                    print(f'Job description {job["descpath"]} not submitted: {job["msg"]}')
            elif not job['cleanup']:
                print(f'Inserted job {job["name"]} with ID {job["id"]}')

        # cleanup failed jobs
        try:
            submitCleanup(args, conf, actrest, jobs, webdavClient, webdavBase)
        finally:
            actrest.close()
            if webdavClient:
                webdavClient.close()


def submitCleanup(args, conf, actrest, jobs, webdavClient, webdavBase):
    # clean jobs that could not be submitted
    tokill = [job['id'] for job in jobs if job['cleanup']]
    if tokill:
        print('Cleaning up failed or cancelled jobs ...')
        try:
            jobs = actrest.killJobs(jobids=tokill)
        except Exception as exc:
            raise ACTClientError(f'Error cleaning up after job submission: {exc}')
        toclean = [job['c_id'] for job in jobs]
        webdavCleanup(args, conf, toclean, webdavClient, webdavBase)
