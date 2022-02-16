import argparse
import os
import sys
import zipfile

import trio
import httpx

from common import (addCommonArgs, addCommonJobFilterArgs, checkJobParams,
                    clean_webdav, disableSIGINT, readTokenFile,
                    showHelpOnCommandOnly, run_with_sigint_handler)
from config import checkConf, expandPaths, loadConf


# TODO: signal exit with exception


async def getFilteredJobIDs(client, jobsUrl, token, **kwargs):
    jobids = []
    headers = {'Authorization': 'Bearer ' + token}
    params = {'client': 'id'}
    if 'id' in kwargs:
        params['id'] = kwargs['id']
    if 'name' in kwargs:
        params['name'] = kwargs['name']
    if 'state' in kwargs:
        params['state'] = kwargs['state']

    try:
        resp = await client.get(jobsUrl, params=params, headers=headers)
        json = resp.json()
    except httpx.RequestError as e:
        print('request error: {}'.format(e))
        sys.exit(1)

    if resp.status_code != 200:
        print('error: filter response: {} - {}'.format(resp.status_code, json['msg']))
        sys.exit(1)

    jobids.extend([job['c_id'] for job in json])
    return set(jobids)


# Only 'done' and 'donefailed' jobs can be fetched so if no state is given,
# we have to get union of jobs in either of those states.
async def filterJobsToFetch(client, token, jobsUrl, args):
    kwargs = {}
    if args.id:
        kwargs['id'] = args.id
    if args.name:
        kwargs['name'] = args.name

    if args.state:
        if args.state not in ('done', 'donefailed'):
            print('error: wrong state parameter, should be "done" or "donefailed"')
            sys.exit(1)
        kwargs['state'] = args.state
        jobids = await getFilteredJobIDs(client, jobsUrl, token, **kwargs)
    else:
        kwargs['state'] = 'done'
        jobids = await getFilteredJobIDs(client, jobsUrl, token, **kwargs)
        kwargs['state'] = 'donefailed'
        jobids = jobids.union(await getFilteredJobIDs(client, jobsUrl, token, **kwargs))
    return jobids


# possible error conditions:
# - GET /results - must not clean the job (both aCT and webdav)
# - zip extraction failure - must not clean job (both aCT and webdav)
# - zip removal failure - can clean job
#
# Appends jobid to a given list of jobs to clean
async def getJob(jobid, client, token, resultsUrl, jobsUrl, toclean):
    # download result zip; if it fails don't clean job as user should decide
    headers = {'Authorization': 'Bearer ' + token}
    params = {'id': jobid}
    noResults = False  # in case where there is no result folder
    filename = ''
    cancelGet = False
    try:
        async with client.stream('GET', resultsUrl, params=params, headers=headers) as resp:
            if resp.status_code == 204:
                noResults = True
            elif resp.status_code == 200:
                # 'Content-Disposition': 'attachment; filename=ZrcMDm3nK4...m2cmmzn.zip'
                filename = resp.headers['Content-Disposition'].split()[1].split('=')[1]
                async with await trio.open_file(filename, mode='wb') as f:
                    async for chunk in resp.aiter_bytes():
                        await f.write(chunk)
            else:
                json = resp.json()
                print('response error: {} - {}'.format(resp.status_code, json['msg']))
                return
    except trio.Cancelled:
        cancelGet = True
    except httpx.RequestError as e:
        print('request error: {}'.format(e))
        return

    # extract and delete zip file
    if noResults:
        print('{} - no results to fetch for jobid {}'.format(204, jobid))
    else:
        # unzip results
        if not cancelGet and os.path.isfile(filename):
            try:
                dirname = os.path.splitext(filename)[0]
                with zipfile.ZipFile(filename, 'r') as zip_ref:
                    zip_ref.extractall(dirname)
            except (zipfile.BadZipFile, zipfile.LargeZipFile) as e:
                print('error extracting result zip: {}'.format(e))
                cancelGet = True

        # delete results archive
        try:
            if os.path.isfile(filename):
                os.remove(filename)
        except Exception as e:
            print('error deleting results zip: {}'.format(e))

        if cancelGet:
            return

        print('{} - results stored in {}'.format(resp.status_code, dirname))

    toclean.append(jobid)


async def clean_act(client, url, jobids, token):
    headers = {'Authorization': 'Bearer ' + token}
    params = {'id': ','.join([str(jobid) for jobid in jobids])}
    try:
        resp = await client.delete(url, params=params, headers=headers)
        json = resp.json()
    except trio.Cancelled:
        return []
    except httpx.RequestError as e:
        print('request error: {}'.format(e))
        return []

    if resp.status_code != 200:
        print('error cleaning jobs: {} - {}'.format(resp.status_code, json['msg']))
        return []

    return json


def main():
    trio.run(run_with_sigint_handler, program)


async def program():
    parser = argparse.ArgumentParser(description='Download job results')
    addCommonArgs(parser)
    addCommonJobFilterArgs(parser)
    parser.add_argument('--state', default=None,
            help='the state that jobs should be in')
    parser.add_argument('--dcache', nargs='?', const='dcache', default='',
            help='URL of user\'s dCache directory')
    args = parser.parse_args()
    showHelpOnCommandOnly(parser)

    checkJobParams(args)
    conf = loadConf(path=args.conf)

    # override values from configuration
    if args.server:
        conf['server'] = args.server
    if args.port:
        conf['port']   = args.port

    expandPaths(conf)
    checkConf(conf, ['server', 'port', 'token'])

    token = readTokenFile(conf['token'])

    urlBase = conf['server'] + ':' + str(conf['port'])
    resultsUrl = urlBase + '/results'
    jobsUrl = urlBase + '/jobs'

    async with httpx.AsyncClient() as client:
        jobids = await filterJobsToFetch(client, token, jobsUrl, args)

        toclean = []
        try:
            async with trio.open_nursery() as tasks:
                for jobid in jobids:
                    tasks.start_soon(getJob, jobid, client, token, resultsUrl, jobsUrl, toclean)
        except trio.Cancelled:
            pass
        finally:
            with trio.CancelScope(shield=True):
                if toclean:
                    toclean = await clean_act(client, jobsUrl, toclean, token)
                    await clean_webdav(conf, args, toclean)
