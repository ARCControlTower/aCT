import argparse
import sys
import asyncio
import aiohttp

from config import loadConf, checkConf, expandPaths
from common import readTokenFile, addCommonArgs, showHelpOnCommandOnly
from common import checkJobParams, addCommonJobFilterArgs


def main():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(program())


async def program():
    parser = argparse.ArgumentParser(description="Get jobs' status")
    addCommonArgs(parser)
    addCommonJobFilterArgs(parser)
    parser.add_argument('--state', default=None,
            help='the state that jobs should be in')
    parser.add_argument('--arc', default='JobID,State,arcstate',
            help='a list of columns from ARC table')
    parser.add_argument('--client', default='id,jobname',
            help='a list of columns from client table')
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
    if args.id or args.arc or args.client or args.state or args.name:
        #requestUrl += '?'
        if args.id:
            params['id'] = args.id
        if args.arc:
            params['arc'] = args.arc
        if args.client:
            params['client'] = args.client
        if args.state:
            params['state'] = args.state
        if args.name:
            params['name'] = args.name

    async with aiohttp.ClientSession() as session:
        async with session.get(requestUrl, params=params) as resp:
            json = await resp.json()
            if resp.status != 200:
                print('error: request response: {} - {}'.format(resp.status, json['msg']))
                sys.exit(1)

    if args.arc:
        arccols = args.arc.split(',')
    else:
        arccols = []
    if args.client:
        clicols = args.client.split(',')
    else:
        clicols = []

    if not json:
        sys.exit(0)

    # For each column, determine biggest sized value so that output can
    # be nicely formatted.
    colsizes = {}
    for job in json:
        for key, value in job.items():
            # All keys have a letter and underscore prepended, which is not
            # used when printing
            colsize = max(len(str(key[2:])), len(str(value)))
            try:
                if colsize > colsizes[key]:
                    colsizes[key] = colsize
            except KeyError:
                colsizes[key] = colsize

    # Print table header
    for col in clicols:
        print('{:<{width}}'.format(col, width=colsizes['c_' + col]), end=' ')
    for col in arccols:
        print('{:<{width}}'.format(col, width=colsizes['a_' + col]), end=' ')
    print()
    line = ''
    for value in colsizes.values():
        line += '-' * value
    line += '-' * (len(colsizes) - 1)
    print(line)

    # Print jobs
    for job in json:
        for col in clicols:
            fullKey = 'c_' + col
            # fix from CLI actstat
            #print('{:<{width}}'.format(job.get(fullKey, ""), width=colsizes[fullKey]), end=' ')
            txt = job.get(fullKey)
            if not txt or str(txt).strip() == '': # short circuit important!
                txt = "''"
            print('{:<{width}}'.format(txt, width=colsizes[fullKey]), end=' ')
        for col in arccols:
            fullKey = 'a_' + col
            # fix from CLI actstat
            #print('{:<{width}}'.format(job.get(fullKey, ""), width=colsizes[fullKey]), end=' ')
            txt = job.get(fullKey)
            if not txt or str(txt).strip() == '': # short circuit important!
                txt = "''"
            print('{:<{width}}'.format(txt, width=colsizes[fullKey]), end=' ')
        print()
