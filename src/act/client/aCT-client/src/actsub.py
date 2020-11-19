import argparse
import sys
import requests

from config import parseNonParamConf
from common import readProxyFile


def main():

    confDict = {}

    parser = argparse.ArgumentParser(description='Submit job to aCT server')
    parser.add_argument('--proxy', default=None, type=str,
            help='path to proxy file')
    parser.add_argument('--server', default=None, type=str,
            help='URL to aCT server')
    parser.add_argument('--port', default=None, type=int,
            help='port on aCT server')
    parser.add_argument('--conf', default=None, type=str,
            help='path to configuration file')
    parser.add_argument('--site', default='default',
            help='site that jobs should be submitted to')
    parser.add_argument('xRSL', help='path to job description file')
    args = parser.parse_args()

    confDict['proxy']  = args.proxy
    confDict['server'] = args.server
    confDict['port']   = args.port

    parseNonParamConf(confDict, args.conf)

    proxyStr = readProxyFile(confDict['proxy'])

    try:
        with open(args.xRSL, 'r') as f:
            xrslStr = f.read()
    except Exception as e:
        print('error: xRSL file open: {}'.format(str(e)))
        sys.exit(1)

    requestUrl = confDict['server'] + ':' + str(confDict['port']) + '/jobs'
    form = {'site': args.site, 'proxy': proxyStr, 'xrsl': xrslStr}

    try:
        r = requests.post(requestUrl, data=form)
    except Exception as e:
        print('error: request: {}'.format(str(e)))
        sys.exit(1)

    if r.status_code != 200:
        print('error: request response: {} - {}'.format(r.status_code, r.text))
        sys.exit(1)

    print('{} - succesfully submited job with id {}'.format(r.status_code, r.text))


if __name__ == '__main__':
    main()


