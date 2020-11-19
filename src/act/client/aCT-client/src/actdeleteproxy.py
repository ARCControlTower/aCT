import argparse
import sys
import requests

from config import parseNonParamConf
from common import readProxyFile


def main():

    confDict = {}

    parser = argparse.ArgumentParser(description='Delete proxy from aCT server')
    parser.add_argument('--proxy', default=None,
            help='path to proxy file')
    parser.add_argument('--server', default=None,
            help='URL to aCT server')
    parser.add_argument('--port', default=None,
            help='port on aCT server')
    parser.add_argument('--conf', default=None,
            help='path to configuration file')
    parser.add_argument('--id', default=None,
            help='a list of proxy IDs of proxies that should be deleted')
    args = parser.parse_args()

    if not args.id:
        print('error: no proxy id given')
        sys.exit(1)

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


