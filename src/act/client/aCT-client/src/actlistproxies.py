import argparse
import sys
import json
import requests

from config import parseNonParamConf
from common import readProxyFile, addCommonArguments


def main():

    confDict = {}

    parser = argparse.ArgumentParser(description='List proxies on aCT server')
    addCommonArguments(parser)
    args = parser.parse_args()

    confDict['proxy']  = args.proxy
    confDict['server'] = args.server
    confDict['port']   = args.port

    parseNonParamConf(confDict, args.conf)

    proxyStr = readProxyFile(confDict['proxy'])

    requestUrl = confDict['server'] + ':' + str(confDict['port']) + '/proxies'

    try:
        r = requests.get(requestUrl, data={'proxy':proxyStr})
    except Exception as e:
        print('requests error: {}'.format(str(e)))
        sys.exit(1)

    if r.status_code != 200:
        print('error: request response: {} - {}'.format(r.status_code, r.text))
        sys.exit(1)

    json_r = json.loads(r.text)
    for proxy in json_r:
        for key, value in proxy.items():
            print('{}:{} '.format(key, value), end=' ')
        print()


if __name__ == '__main__':
    main()


