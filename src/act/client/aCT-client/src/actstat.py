import argparse
import sys
import requests

from config import parseNonParamConf
from common import readProxyFile, addCommonArgs, showHelpOnCommandOnly
from common import isCorrectIDString, checkJobParams, addCommonJobFilterArgs


def main():

    confDict = {}

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

    confDict['proxy']  = args.proxy
    confDict['server'] = args.server
    confDict['port']   = args.port

    parseNonParamConf(confDict, args.conf)

    proxyStr = readProxyFile(confDict['proxy'])

    requestUrl = confDict['server'] + ':' + str(confDict['port']) + '/jobs'

    if args.id or args.arc or args.client or args.state or args.name:
        requestUrl += '?'
        if args.id:
            requestUrl += 'id=' + args.id + '&'
        if args.arc:
            requestUrl += 'arc=' + args.arc + '&'
        if args.client:
            requestUrl += 'client=' + args.client + '&'
        if args.state:
            requestUrl += 'state=' + args.state + '&'
        if args.name:
            requestUrl += 'name=' + args.name
        requestUrl = requestUrl.rstrip('&')

    try:
        r = requests.get(requestUrl, data={'proxy':proxyStr})
    except Exception as e:
        print('error: request: {}'.format(str(e)))
        sys.exit(1)

    if args.arc:
        arccols = args.arc.split(',')
    else:
        arccols = []
    if args.client:
        clicols = args.client.split(',')
    else:
        clicols = []

    if r.status_code != 200:
        print('error: request response: {} - {}'.format(r.status_code, r.text))
        sys.exit(1)

    try:
        jsonResp = r.json()
    except ValueError as e:
        print('error: response JSON: {}'.format(str(e)))
        sys.exit(1)

    if not jsonResp:
        sys.exit(0)

    # For each column, determine biggest sized value so that output can
    # be nicely formatted.
    colsizes = {}
    for job in jsonResp:
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
    for job in jsonResp:
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


if __name__ == '__main__':
    main()


