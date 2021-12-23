import argparse
import sys
import requests

from config import loadConf, checkConf, expandPaths
from common import readTokenFile, addCommonArgs, showHelpOnCommandOnly, cleandCache
from common import isCorrectIDString, checkJobParams, addCommonJobFilterArgs


def main():
    parser = argparse.ArgumentParser(description="Get jobs' status")
    addCommonArgs(parser)
    addCommonJobFilterArgs(parser)
    parser.add_argument('--state', default=None,
            help='the state that jobs should be in')
    parser.add_argument('--dcache', nargs='?', const='dcache', default='',
            help='whether files should be uploaded to dcache with optional \
                  location parameter')
    args = parser.parse_args()
    showHelpOnCommandOnly(parser)

    checkJobParams(args)
    conf = loadConf(path=args.conf)

    # override values from configuration
    if args.server:
        conf['server'] = args.server
    if args.port:
        conf['port']   = args.port

    expandPaths(conf)
    checkConf(conf, ['server', 'port', 'token'])

    token = readTokenFile(conf['token'])

    requestUrl = conf['server'] + ':' + str(conf['port']) + '/jobs'

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

    # TODO: error handling for malformed JSON
    jobs = r.json()
    print('Cleaned {} jobs'.format(len(jobs)))

    for jobid in jobs:
        cleandCache(conf, args, jobid)


if __name__ == '__main__':
    main()


