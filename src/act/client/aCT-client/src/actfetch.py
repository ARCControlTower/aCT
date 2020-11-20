import argparse
import sys
import requests

from config import parseNonParamConf
from common import readProxyFile, addCommonArgs, showHelpOnCommandOnly
from common import isCorrectIDString, checkJobParams, addCommonJobFilterArgs


def main():

    confDict = {}

    parser = argparse.ArgumentParser(description="Fetch failed jobs")
    addCommonArgs(parser)
    addCommonJobFilterArgs(parser)
    args = parser.parse_args()
    showHelpOnCommandOnly(parser)

    checkJobParams(args)

    confDict['proxy']  = args.proxy
    confDict['server'] = args.server
    confDict['port']   = args.port

    parseNonParamConf(confDict, args.conf)

    proxyStr = readProxyFile(confDict['proxy'])

    requestUrl = confDict['server'] + ':' + str(confDict['port']) + '/jobs'

    if args.id or args.name:
        requestUrl += '?'
        if args.id:
            requestUrl += 'id=' + args.id + '&'
        if args.name:
            requestUrl += 'name=' + args.name
        requestUrl = requestUrl.rstrip('&')

    try:
        r = requests.patch(requestUrl, data={'proxy':proxyStr,'arcstate':'tofetch'})
    except Exception as e:
        print('error: request: {}'.format(str(e)))
        sys.exit(1)

    if r.status_code != 200:
        print('error: request response: {} - {}'.format(r.status_code, r.text))
        sys.exit(1)

    print('Will fetch {} jobs'.format(r.text))


if __name__ == '__main__':
    main()


