import argparse

import httpx
import trio

from common import (addCommonArgs, addCommonJobFilterArgs, checkJobParams,
                    clean_webdav, readTokenFile, runWithSIGINTHandler,
                    showHelpOnCommandOnly)
from config import checkConf, expandPaths, loadConf


def main():
    trio.run(runWithSIGINTHandler, program)


async def clean_jobs(url, params, token, conf, args):
    headers = {'Authorization': 'Bearer ' + token}
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.delete(url, params=params, headers=headers)
            json = resp.json()
        except httpx.RequestError as e:
            print('request error: {}'.format(e))
            return

    if resp.status_code != 200:
        print('response error: {} - {}'.format(resp.status_code, json['msg']))
        return
    else:
        print('Cleaned {} jobs'.format(len(json)))

    await clean_webdav(conf, args, json)


async def program():
    parser = argparse.ArgumentParser(description="Get jobs' status")
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
        conf['port'] = args.port

    expandPaths(conf)
    checkConf(conf, ['server', 'port', 'token'])

    token = readTokenFile(conf['token'])

    requestUrl = conf['server'] + ':' + str(conf['port']) + '/jobs'

    params = {}
    if args.id or args.state or args.name:
        if args.id:
            params['id'] = args.id
        if args.state:
            params['state'] = args.state
        if args.name:
            params['name'] = args.name

    with trio.CancelScope(shield=True):
        await clean_jobs(requestUrl, params, token, conf, args)
