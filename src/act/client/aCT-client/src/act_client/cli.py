import argparse
import json
import os
import sys

from act_client.common import (ACTClientError, disableSIGINT, getIDParam,
                               getWebDAVBase)
from act_client.config import checkConf, expandPaths, loadConf
from act_client.httpclient import HTTP_BUFFER_SIZE, HTTPClient
from act_client.operations import (SubmissionInterrupt, getACTRestClient,
                                   getWebDAVClient)


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
    parser.add_argument(
        '-v',
        '--verbose',
        action='store_true',
        help='output debug logs'
    )


def addCommonJobFilterArgs(parser):
    parser.add_argument(
        '-a',
        '--all',
        action='store_true',
        help='all jobs that match other criteria'
    )
    parser.add_argument(
        '-i',
        '--id',
        default=[],
        help='a list of IDs of jobs that should be queried'
    )
    parser.add_argument(
        '-n',
        '--name',
        default='',
        help='substring that jobs should have in name'
    )


def addStateArg(parser):
    parser.add_argument(
        '-s',
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
    parserGet.add_argument(
        '--use-jobname',
        action='store_true',
        help='name of download dir should be the same as job name'
    )
    parserGet.add_argument(
        '--noclean',
        action='store_true',
        help='do not clean jobs'
    )

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

    parserCat = subparsers.add_parser(
        'cat',
        help='print stdout or stderr of the job'
    )
    addCommonJobFilterArgs(parserCat)
    addStateArg(parserCat)
    parserCat.add_argument(
        '-o', '--stdout', action='store_true', default=True,
        help='print job\'s stdout'
    )
    parserCat.add_argument(
        '-e', '--stderr', action='store_true',
        help='print job\'s stderr'
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
    elif args.command == 'cat':
        commandFun = subcommandCat

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
    except Exception as exc:
        print(exc)
        sys.exit(1)


def subcommandInfo(args, conf):
    checkConf(conf, ['server', 'token'])

    actrest = getACTRestClient(args, conf)
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

    actrest = getACTRestClient(args, conf)
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
    try:
        if webdavClient:
            closeWebDAV = False
        else:
            closeWebDAV = True
            webdavClient = getWebDAVClient(args, conf, webdavBase)

        errors = webdavClient.cleanJobDirs(webdavBase, jobids)
        for error in errors:
            print(error)
    except Exception as exc:
        raise ACTClientError(f'Error cleaning up WebDAV dirs: {exc}')
    finally:
        if closeWebDAV and webdavClient:
            webdavClient.close()


def subcommandFetch(args, conf):
    checkConf(conf, ['server', 'token'])

    actrest = getACTRestClient(args, conf)
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

    actrest = getACTRestClient(args, conf)
    ids = getIDParam(args)
    toclean = []
    try:
        jobs = actrest.getDownloadableJobs(jobids=ids, name=args.name, state=args.state)
        for job in jobs:
            try:
                if args.use_jobname:
                    dirname = job['c_jobname']
                else:
                    dirname = job['a_IDFromEndpoint']

                # if ouput directory already exists add a number to its name
                if os.path.isdir(dirname):
                    dirnum = 1
                    while os.path.isdir(f'{dirname}_{dirnum}'):
                        dirnum += 1
                        if dirnum > sys.maxsize:
                            raise ACTClientError('Extraction directory already exists')
                    dirname = f'{dirname}_{dirnum}'

                anyResults, errors = actrest.downloadJobResults(job['c_id'], downloadDir=dirname)
            except Exception as e:
                print(f'Error downloading job {job["c_jobname"]}: {e}')
                continue

            if errors:
                print(f'Errors downloading job {job["c_jobname"]}:')
                for error in errors:
                    print(f'    {error}')
                continue
            elif not anyResults:
                print(f'No output files for job {job["c_jobname"]}')
            else:
                print(f'Results for job {job["c_jobname"]} stored in {dirname}')
            toclean.append(job["c_id"])
    except Exception as exc:
        raise ACTClientError(f'Error downloading jobs: {exc}')
    except KeyboardInterrupt:
        print('Stopping job download ...')
    finally:
        if args.noclean:
            return

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

    actrest = getACTRestClient(args, conf)
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

    actrest = getACTRestClient(args, conf, useToken=False)
    try:
        with open(conf['proxy'], 'r') as f:
            proxyStr = f.read()
    except FileNotFoundError:
        raise ACTClientError(f'Could not find proxy certificate in {conf["proxy"]}')
    try:
        disableSIGINT()
        actrest.uploadProxy(proxyStr, conf['token'])
    finally:
        actrest.close()

    print(f'Successfully inserted proxy. Access token stored in {conf["token"]}')


def subcommandResub(args, conf):
    checkConf(conf, ['server', 'token'])

    actrest = getACTRestClient(args, conf)
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

    actrest = getACTRestClient(args, conf)
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

    actrest = getACTRestClient(args, conf)
    webdavClient = None
    webdavBase = None
    jobs = []
    try:
        if args.webdav:
            webdavBase = getWebDAVBase(args, conf)
            webdavClient = getWebDAVClient(args, conf, webdavBase)
        jobs = actrest.submitJobs(args.xRSL, clusterlist, webdavClient, webdavBase)
    except SubmissionInterrupt as exc:
        jobs = exc.results
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


def subcommandCat(args, conf):
    checkConf(conf, ['server', 'token', 'proxy'])

    if args.stderr:
        infoKey = "StdErr"
    else:
        infoKey = "StdOut"

    actrest = getACTRestClient(args, conf)
    ids = getIDParam(args)
    try:

        try:
            jsonData = actrest.getJobStats(
                jobids=ids,
                name=args.name,
                state=args.state,
                clienttab=['id', 'jobname'],
                arctab=['IDFromEndpoint', 'cluster', infoKey]
            )
        except Exception as exc:
            raise ACTClientError(f'Error fetching job {infoKey.lower()}: {exc}')

        if not jsonData:
            return

        # per cluster ARCRest; maybe there could be connect method on clients
        # to allow reconnection to another host?
        clients = {}

        for job in jsonData:
            # skip if required path not in DB yet
            if f'a_{infoKey}' not in job or job[f'a_{infoKey}'] is None:
                print(f"{infoKey.lower()} not yet available for job {job['c_id']} {job['c_jobname']}")
                continue

            # create a client for the cluster if it does not exist
            if job['a_cluster'] not in clients:
                try:
                    httpClient = HTTPClient(url=job['a_cluster'], proxypath=conf['proxy'], logger=actrest.logger)
                except Exception as exc:
                    print(f'Error creating REST client for ARC cluster {job["a_cluster"]} for job {job["c_id"]} {job["c_jobname"]}: {exc}')
                    continue
                clients[job['a_cluster']] = httpClient
            else:
                httpClient = clients[job['a_cluster']]

            # initiate file download
            url = f'/arex/rest/1.0/jobs/{job["a_IDFromEndpoint"]}/session/{job["a_"+infoKey]}'
            try:
                resp = httpClient.request('GET', url)
            except Exception as exc:
                print(f'Error fetching {infoKey.lower()} from {url} for job {job["c_id"]} {job["c_jobname"]}: {exc}')
                continue
            if resp.status != 200:
                text = resp.read().decode()
                httpClient.logger.debug(f"Response for {url} - {resp.status} {text}")
                try:
                    msg = (json.loads(text))['msg']
                    print(f'Error fetching {infoKey.lower()} from {url} for job {job["c_id"]} {job["c_jobname"]}: {msg}')
                except json.JSONDecodeError:
                    print(f'Error parsing JSON response from {url} for job {job["c_id"]} {job["c_jobname"]} - {resp.status} {text}')
                continue

            # stream file to stdout
            try:
                data = resp.read(HTTP_BUFFER_SIZE)
                while data:
                    print(data.decode(), end='')
                    data = resp.read(HTTP_BUFFER_SIZE)
            except Exception as exc:
                print(f'Error fetching {infoKey.lower()} from {url} for job {job["c_id"]} {job["c_jobname"]}: {exc}')
                continue

    finally:
        actrest.close()
