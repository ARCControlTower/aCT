import argparse
import asyncio
import sys

import aiohttp

from common import (addCommonArgs, addCommonJobFilterArgs, checkJobParams,
                    disableSIGINT, readTokenFile, showHelpOnCommandOnly)
from config import checkConf, expandPaths, loadConf


def main():
    try:
        disableSIGINT()
        loop = asyncio.get_event_loop()
        loop.run_until_complete(program())
    except Exception as e:
        print('error: {}'.format(e))
        sys.exit(1)


async def program():
    parser = argparse.ArgumentParser(description="Kill jobs")
    addCommonArgs(parser)
    addCommonJobFilterArgs(parser)
    parser.add_argument('--state', default=None,
            help='the state that jobs should be in')
    args = parser.parse_args()
    showHelpOnCommandOnly(parser)

    checkJobParams(args)
    conf = loadConf(path=args.conf)

    # override values from configuration
    if args.server:
        conf['server'] = args.server
    if args.port:
        conf['port'] = args.port

    expandPaths(conf)
    checkConf(conf, ['server', 'port', 'token'])

    token = readTokenFile(conf['token'])

    requestUrl = conf['server'] + ':' + str(conf['port']) + '/jobs'

    headers = {'Authorization': 'Bearer ' + token}
    params = {}
    if args.id or args.name:
        if args.id:
            params['id'] = args.id
        if args.state:
            params['state'] = args.state
        if args.name:
            params['name'] = args.name

    async with aiohttp.ClientSession() as session:
        async with session.patch(requestUrl, json={'arcstate': 'tocancel'}, params=params, headers=headers) as resp:
            json = await resp.json()
            status = resp.status

    if status != 200:
        print('response error: {} - {}'.format(status, json['msg']))
        sys.exit(1)
    else:
        print('Will kill {} jobs'.format(len(json)))
