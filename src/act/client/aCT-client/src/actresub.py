import argparse
import sys
import requests

from config import parseNonParamConf
from common import readProxyFile, addCommonArguments


def main():

    confDict = {}

    parser = argparse.ArgumentParser(description="Resubmit failed jobs")
    addCommonArguments(parser)
    parser.add_argument('--id', default=None,
            help='a list of IDs of jobs that should be queried')
    parser.add_argument('--name', default=None,
            help='substring that jobs should have in name')
    args = parser.parse_args()

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
        r = requests.patch(requestUrl, data={'proxy':proxyStr,'arcstate':'toresubmit'})
    except Exception as e:
        print('error: request: {}'.format(str(e)))
        sys.exit(1)

    if r.status_code != 200:
        print('error: request response: {} - {}'.format(r.status_code, r.text))
        sys.exit(1)

    print('Will resubmit {} jobs'.format(r.text))


if __name__ == '__main__':
    main()


