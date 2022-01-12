import argparse
import sys
import asyncio
import aiohttp

from config import loadConf, checkConf, expandPaths
from common import readTokenFile, addCommonArgs, showHelpOnCommandOnly
from common import checkJobParams, addCommonJobFilterArgs


def main():
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(program())
    except Exception as e:
        print('error: {}'.format(e))
        sys.exit(1)


async def program():
    parser = argparse.ArgumentParser(description="Fetch failed jobs")
    addCommonArgs(parser)
    addCommonJobFilterArgs(parser)
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

    requestUrl = conf['server'] + ':' + str(conf['port']) + '/jobs'

    params = {'token': token}
    if args.id or args.name:
        if args.id:
            params['id'] = args.id
        if args.name:
            params['name'] = args.name

    async with aiohttp.ClientSession() as session:
        async with session.patch(requestUrl, json={'arcstate': 'tofetch'}, params=params) as resp:
            json = await resp.json()
            if resp.status != 200:
                print('error: request response: {} - {}'.format(resp.status, json['msg']))
                sys.exit(1)
            else:
                print('Will fetch {} jobs'.format(len(json)))
