import argparse
import sys
import requests

from config import parseNonParamConf
from common import readProxyFile, addCommonArgs, showHelpOnCommandOnly


def main():

    confDict = {}

    parser = argparse.ArgumentParser(description='Delete proxy from aCT server')
    addCommonArgs(parser)
    parser.add_argument('--id', default=None,
            help='a list of proxy IDs of proxies that should be deleted')
    args = parser.parse_args()
    showHelpOnCommandOnly(parser)

    confDict['proxy']  = args.proxy
    confDict['server'] = args.server
    confDict['port']   = args.port

    parseNonParamConf(confDict, args.conf)

    proxyStr = readProxyFile(confDict['proxy'])

    requestUrl = confDict['server'] + ':' + str(confDict['port']) + '/proxies'
    requestUrl += '?id=' + args.id

    try:
        r = requests.delete(requestUrl, data={'proxy':proxyStr})
    except Exception as e:
        print('error: requests: {}'.format(str(e)))
        sys.exit(1)

    if r.status_code != 200:
        print('error: request response: {} - {}'.format(r.status_code, r.text))
        sys.exit(1)

    print('Deleted {} proxies'.format(r.text))


if __name__ == '__main__':
    main()


