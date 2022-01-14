import argparse
import sys
import asyncio
import aiohttp

from config import loadConf, checkConf, expandPaths
from common import readTokenFile, addCommonArgs, showHelpOnCommandOnly, cleandCache
from common import checkJobParams, addCommonJobFilterArgs


def main():
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(program())
    except Exception as e:
        print('error: {}'.format(e))
        sys.exit(1)


async def program():
    parser = argparse.ArgumentParser(description="Get jobs' status")
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

    requestUrl = conf['server'] + ':' + str(conf['port']) + '/jobs'

    headers = {'Authorization': 'Bearer ' + token}
    params = {}
    if args.id or args.state or args.name:
        if args.id:
            params['id'] = args.id
        if args.state:
            params['state'] = args.state
        if args.name:
            params['name'] = args.name

    async with aiohttp.ClientSession() as session:
        async with session.delete(requestUrl, params=params, headers=headers) as resp:
            json = await resp.json()
            if resp.status != 200:
                print('response error: {} - {}'.format(resp.status, json['msg']))
                sys.exit(1)
            else:
                print('Cleaned {} jobs'.format(len(json)))

    await cleandCache(conf, args, json)
