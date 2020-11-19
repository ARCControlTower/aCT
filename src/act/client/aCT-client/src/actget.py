import argparse
import sys
import os
import zipfile
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
    parser.add_argument('--id', default='',
            help='a list of IDs of jobs that should be queried')
    args = parser.parse_args()

    confDict['proxy']  = args.proxy
    confDict['server'] = args.server
    confDict['port']   = args.port

    parseNonParamConf(confDict, args.conf)

    proxyStr = readProxyFile(confDict['proxy'])

    requestUrl = confDict['server'] + ':' + str(confDict['port']) + '/results'
    requestUrl += '?id=' + args.id

    try:
        r = requests.get(requestUrl, data={'proxy':proxyStr}, stream=True)
    except Exception as e:
        print('error: result request: {}'.format(str(e)))
        sys.exit(1)

    if r.status_code != 200:
        print('error: request response: {} - {}'.format(r.status_code, r.text))
        sys.exit(1)

    # 'Content-Disposition': 'attachment; filename=ZrcMDm3nK4rneiavIpohlF4nABFKDmABFKDmggFKDmEBFKDm2cmmzn.zip'
    filename = r.headers['Content-Disposition'].split()[1].split('=')[1]
    try:
        with open(filename, 'wb') as resultFile:
            for chunk in r.iter_content():
                if chunk: # filter out keep-alive new chunks
                    resultFile.write(chunk)
        dirname = os.path.splitext(filename)[0]
        with zipfile.ZipFile(filename, 'r') as zip_ref:
            zip_ref.extractall(dirname)
        os.remove(filename)
    except Exception as e:
        print('error: results fetch: {}'.format(str(e)))
        sys.exit(1)

    print('{} - results stored in {}'.format(r.status_code, dirname))

    requestUrl = confDict['server'] + ':' + str(confDict['port']) + '/jobs'
    requestUrl += '?id=' + args.id

    try:
        r = requests.delete(requestUrl, data={'proxy':proxyStr}, )
    except Exception as e:
        print('error: clean request: {}'.format(str(e)))
        sys.exit(1)

    if r.status_code != 200:
        print('error cleaning job: {}'.format(r.text))
        sys.exit(1)


if __name__ == '__main__':
    main()


