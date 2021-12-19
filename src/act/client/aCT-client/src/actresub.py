import argparse
import sys
import requests

from config import parseNonParamConf, DEFAULT_TOKEN_PATH
from common import readTokenFile, addCommonArgs, showHelpOnCommandOnly
from common import isCorrectIDString, checkJobParams, addCommonJobFilterArgs


def main():

    confDict = {}

    parser = argparse.ArgumentParser(description="Resubmit failed jobs")
    addCommonArgs(parser)
    addCommonJobFilterArgs(parser)
    args = parser.parse_args()
    showHelpOnCommandOnly(parser)

    checkJobParams(args)

    confDict['proxy']  = args.proxy
    confDict['server'] = args.server
    confDict['port']   = args.port

    parseNonParamConf(confDict, args.conf)

    token = readTokenFile(DEFAULT_TOKEN_PATH)

    requestUrl = confDict['server'] + ':' + str(confDict['port']) + '/jobs'

    params = {'token': token}
    if args.id or args.name:
        if args.id:
            params['id'] = args.id
        if args.name:
            params['name'] = args.name

    try:
        r = requests.patch(requestUrl, json={'arcstate':'toresubmit'}, params=params)
    except Exception as e:
        print('error: request: {}'.format(str(e)))
        sys.exit(1)

    if r.status_code != 200:
        print('error: request response: {} - {}'.format(r.status_code, r.json()['msg']))
        sys.exit(1)

    print('Will resubmit {} jobs'.format(r.text))


if __name__ == '__main__':
    main()


