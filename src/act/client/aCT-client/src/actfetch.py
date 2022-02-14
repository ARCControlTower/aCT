import argparse
import sys

import trio
import httpx

from common import (addCommonArgs, addCommonJobFilterArgs, checkJobParams,
                    disableSIGINT, readTokenFile, showHelpOnCommandOnly, run_with_sigint_handler)
from config import checkConf, expandPaths, loadConf


def main():
    trio.run(run_with_sigint_handler, program)


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
        if args.name:
            params['name'] = args.name


    with trio.CancelScope(shield=True):
        async with httpx.AsyncClient() as client:
            resp = await client.patch(requestUrl, json={'arcstate': 'tofetch'}, params=params, headers=headers)
            json = resp.json()

    if resp.status_code != 200:
        print('response error: {} - {}'.format(resp.status_code, json['msg']))
        return
    else:
        print('Will fetch {} jobs'.format(len(json)))
