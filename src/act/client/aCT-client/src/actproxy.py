import argparse
import sys
import requests

from config import parseNonParamConf
from common import readProxyFile


def main():

    confDict = {}

    parser = argparse.ArgumentParser(description='Submit proxy to aCT server')
    parser.add_argument('--proxy', default=None, type=str,
            help='path to proxy file')
    parser.add_argument('--server', default=None, type=str,
            help='URL to aCT server')
    parser.add_argument('--port', default=None, type=int,
            help='port on aCT server')
    parser.add_argument('--conf', default=None, type=str,
            help='path to configuration file')
    args = parser.parse_args()

    confDict['proxy']  = args.proxy
    confDict['server'] = args.server
    confDict['port']   = args.port

    parseNonParamConf(confDict, args.conf)

    proxyStr = readProxyFile(confDict['proxy'])

    requestUrl = confDict['server'] + ':' + str(confDict['port']) + '/proxies'

    try:
        r = requests.put(requestUrl, data={'proxy':proxyStr})
    except Exception as e:
        print('error: request: {}'.format(str(e)))
        sys.exit(1)

    if r.status_code != 200:
        print('error: request response: {} - {}'.format(r.status_code, r.text))
        sys.exit(1)

    print('{} - successfully inserted proxy with id {}'.format(r.status_code, r.text))


if __name__ == '__main__':
    main()


