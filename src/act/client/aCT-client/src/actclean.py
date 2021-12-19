import argparse
import sys
import requests

from config import parseNonParamConf, DEFAULT_TOKEN_PATH
from common import readTokenFile, addCommonArgs, showHelpOnCommandOnly
from common import isCorrectIDString, checkJobParams, addCommonJobFilterArgs


def main():

    confDict = {}

    parser = argparse.ArgumentParser(description="Get jobs' status")
    addCommonArgs(parser)
    addCommonJobFilterArgs(parser)
    parser.add_argument('--state', default=None,
            help='the state that jobs should be in')
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
    if args.id or args.state or args.name:
        if args.id:
            params['id'] = args.id
        if args.state:
            params['state'] = args.state
        if args.name:
            params['name'] = args.name

    try:
        r = requests.delete(requestUrl, params=params)
    except Exception as e:
        print('error: request: {}'.format(str(e)))
        sys.exit(1)

    if r.status_code != 200:
        print('error: request response: {} - {}'.format(r.status_code, r.json()['msg']))
        sys.exit(1)

    print('Cleaned {} jobs'.format(r.text))


if __name__ == '__main__':
    main()


