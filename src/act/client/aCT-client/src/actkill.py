import argparse

import trio
import httpx

from common import (addCommonArgs, addCommonJobFilterArgs, checkJobParams,
                    clean_webdav, readTokenFile, showHelpOnCommandOnly, run_with_sigint_handler)
from config import checkConf, expandPaths, loadConf


def main():
    trio.run(run_with_sigint_handler, program)


async def kill_jobs(url, params, headers, conf, args):
    async with httpx.AsyncClient() as client:
        json = {'arcstate': 'tocancel'}
        try:
            resp = await client.patch(url, json=json, params=params, headers=headers)
            json = resp.json()
        except httpx.RequestError as e:
            print('request error: {}'.format(e))
            return

    if resp.status_code != 200:
        print('response error: {} - {}'.format(resp.status_code, json['msg']))
        return

    print('Will kill {} jobs'.format(len(json)))

    tokill = [job['c_id'] for job in json if job['a_id'] is None or job['a_arcstate'] in ('tosubmit', 'submitting')]
    await clean_webdav(conf, args, tokill)


async def program():
    parser = argparse.ArgumentParser(description="Kill jobs")
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

    headers = {'Authorization': 'Bearer ' + token}
    params = {}
    if args.id or args.name:
        if args.id:
            params['id'] = args.id
        if args.state:
            params['state'] = args.state
        if args.name:
            params['name'] = args.name

    with trio.CancelScope(shield=True):
        await kill_jobs(requestUrl, params, headers, conf, args)
