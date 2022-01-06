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


def main():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(program())


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
        jobids = await getFilteredJobIDs(session, jobsUrl, token, state='done')
        jobids = jobids.union(await getFilteredJobIDs(session, jobsUrl, token, state='donefailed'))
        if args.id:
            jobids = jobids.intersection(await getFilteredJobIDs(session, jobsUrl, token, id=args.id))
        if args.name:
            jobids = jobids.intersection(await getFilteredJobIDs(session, jobsUrl, token, name=args.name))
        if args.state:
            jobids = jobids.intersection(await getFilteredJobIDs(session, jobsUrl, token, state=args.state))

        # fetch job result for every job
        for jobid in jobids:
            # download result zip
            params = {'token': token, 'id': jobid}
            async with session.get(resultsUrl, params=params) as resp:
                if resp.status != 200:
                    print('error: request response: {} - {}'.format(resp.status, await resp.json()['msg']))
                    continue

                # 'Content-Disposition': 'attachment; filename=ZrcMDm3nK4rneiavIpohlF4nABFKDmABFKDmggFKDmEBFKDm2cmmzn.zip'
                filename = resp.headers['Content-Disposition'].split()[1].split('=')[1]
                async with aiofiles.open(filename, mode='wb') as f:
                    async for chunk, _ in resp.content.iter_chunks():
                        await f.write(chunk)

            # extract and delete zip file
            try:
                dirname = os.path.splitext(filename)[0]
                with zipfile.ZipFile(filename, 'r') as zip_ref:
                    zip_ref.extractall(dirname)
                os.remove(filename)
            except Exception as e:
                print('error: results unzip: {}'.format(str(e)))
                continue

            print('{} - results stored in {}'.format(resp.status, dirname))

            # delete job from act
            async with session.delete(jobsUrl, params=params) as resp:
                json = await resp.json()
                if resp.status != 200:
                    print('error cleaning job: {}'.format(json['msg']))

    await cleandCache(conf, args, jobids)
