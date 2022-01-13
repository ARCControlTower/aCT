import argparse
import sys
import os
import zipfile
import asyncio
import aiohttp
import aiofiles

from config import loadConf, checkConf, expandPaths
from common import readTokenFile, addCommonArgs, showHelpOnCommandOnly
from common import addCommonJobFilterArgs, checkJobParams, cleandCache


async def getFilteredJobIDs(session, jobsUrl, token, **kwargs):
    jobids = []
    params = {'token': token, 'client': 'id'}
    if 'id' in kwargs:
        params['id'] = kwargs['id']
    if 'name' in kwargs:
        params['name'] = kwargs['name']
    if 'state' in kwargs:
        params['state'] = kwargs['state']

    async with session.get(jobsUrl, params=params) as resp:
        json = await resp.json()
        if resp.status != 200:
            print('error: filter response: {} - {}'.format(resp.status, json['msg']))
            sys.exit(1)

    jobids.extend([job['c_id'] for job in json])
    return set(jobids)


# Only 'done' and 'donefailed' jobs can be fetched so if no state is given,
# we have to get union of jobs in either of those states.
async def filterJobsToFetch(session, token, jobsUrl, args):
    try:
        kwargs = {}
        if args.id:
            kwargs['id'] = args.id
        if args.name:
            kwargs['name'] = args.name

        if args.state:
            if args.state not in ('done', 'donefailed'):
                print('error: wrong state parameter, should be \'done\' or \'donefailed\'')
                sys.exit(1)
            kwargs['state'] = args.state
            jobids = await getFilteredJobIDs(session, jobsUrl, token, **kwargs)
        else:
            kwargs['state'] = 'done'
            jobids = await getFilteredJobIDs(session, jobsUrl, token, **kwargs)
            kwargs['state'] = 'donefailed'
            jobids = jobids.union(await getFilteredJobIDs(session, jobsUrl, token, **kwargs))
    except aiohttp.ClientError as e:
        print('HTTP client error: filtering jobs to fetch: {}'.format(e))
        sys.exit(1)
    return jobids


async def getJob(jobid, session, token, resultsUrl, jobsUrl):
    # download result zip; if it fails don't clean job as user should decide
    params = {'token': token, 'id': jobid}
    try:
        async with session.get(resultsUrl, params=params) as resp:
            if resp.status != 200:
                print('response error: {} - {}'.format(resp.status, await resp.json()['msg']))
                return

            # 'Content-Disposition': 'attachment; filename=ZrcMDm3nK4...m2cmmzn.zip'
            filename = resp.headers['Content-Disposition'].split()[1].split('=')[1]
            async with aiofiles.open(filename, mode='wb') as f:
                async for chunk, _ in resp.content.iter_chunks():
                    await f.write(chunk)
    except aiohttp.ClientError as e:
        print('HTTP client error: fetching results for jobid {}: {}'.format(jobid, e))
        return
    except Exception as e: # for aiofiles exceptions probably
        print('error: {}'.format(e))
        return

    # extract and delete zip file
    try:
        dirname = os.path.splitext(filename)[0]
        with zipfile.ZipFile(filename, 'r') as zip_ref:
            zip_ref.extractall(dirname)
    except (zipfile.BadZipFile, zipfile.LargeZipFile) as e:
        print('error extracting result zip: {}'.format(e))
        return
    try:
        os.remove(filename)
    except Exception as e:
        print('error deleting results zip: {}'.format(e))
        return

    print('{} - results stored in {}'.format(resp.status, dirname))

    # delete job from act
    try:
        async with session.delete(jobsUrl, params=params) as resp:
            json = await resp.json()
            if resp.status != 200:
                print('error cleaning job: {}'.format(json['msg']))
    except aiohttp.ClientError as e:
        print('error: cleaning jobid {}: {}'.format(jobid, e))


def main():
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(program())
    except Exception as e:
        print('error: {}'.format(e))
        sys.exit(1)


async def program():
    parser = argparse.ArgumentParser(description='Download job results')
    addCommonArgs(parser)
    addCommonJobFilterArgs(parser)
    parser.add_argument('--state', default=None,
            help='the state that jobs should be in')
    parser.add_argument('--dcache', nargs='?', const='dcache', default='',
            help='whether files should be uploaded to dcache with optional \
                  location parameter')
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

    async with aiohttp.ClientSession() as session:
        # compute all IDs to fetch
        jobids = await filterJobsToFetch(session, token, jobsUrl, args)

        # fetch job result for every job
        tasks = []
        for jobid in jobids:
            #await getJob(jobid, session, token, resultsUrl, jobsUrl)
            tasks.append(asyncio.ensure_future(getJob(jobid, session, token, resultsUrl, jobsUrl)))
        await asyncio.gather(*tasks)

    await cleandCache(conf, args, jobids)
