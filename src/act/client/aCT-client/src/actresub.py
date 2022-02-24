import argparse

import httpx
import trio

from common import (addCommonArgs, addCommonJobFilterArgs, checkJobParams,
                    readTokenFile, runWithSIGINTHandler, showHelpOnCommandOnly)
from config import checkConf, expandPaths, loadConf


def main():
    trio.run(runWithSIGINTHandler, program)


async def program():
    parser = argparse.ArgumentParser(description="Resubmit failed jobs")
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
            json = {'arcstate': 'toresubmit'}
            try:
                resp = await client.patch(requestUrl, json=json, params=params, headers=headers)
                json = resp.json()
            except httpx.RequestError as e:
                print('request error: {}'.format(e))
                return

    if resp.status_code != 200:
        print('response error: {} - {}'.format(resp.status_code, json['msg']))
        return
    else:
        print('Will resubmit {} jobs'.format(len(json)))
