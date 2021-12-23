import argparse
import sys
import os
import zipfile
import requests

from config import loadConf, checkConf, expandPaths
from common import readTokenFile, addCommonArgs, showHelpOnCommandOnly
from common import addCommonJobFilterArgs, checkJobParams, cleandCache


def getFilteredJobIDs(jobsUrl, token, **kwargs):
    jobids = []
    params = {'token': token, 'client': 'id'}
    if 'id' in kwargs:
        params['id'] = kwargs['id']
    if 'name' in kwargs:
        params['name'] = kwargs['name']
    if 'state' in kwargs:
        params['state'] = kwargs['state']

    try:
        r = requests.get(jobsUrl, params=params)
    except Exception as e:
        print('error: filter request: {}'.format(str(e)))
        sys.exit(1)
    if r.status_code != 200:
        print('error: filter response: {} - {}'.format(r.status_code, r.json()['msg']))
        sys.exit(1)
    try:
        jsonResp = r.json()
    except ValueError as e:
        print('error: filter response: JSON: {}'.format(str(e)))
        sys.exit(1)
    jobids.extend([job['c_id'] for job in jsonResp])
    return set(jobids)


def main():
    parser = argparse.ArgumentParser(description='Download job results')
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

    urlBase = conf['server'] + ':' + str(conf['port'])
    resultsUrl = urlBase + '/results'
    jobsUrl = urlBase + '/jobs'

    # compute all IDs to fetch
    jobids = getFilteredJobIDs(jobsUrl, token, state='done')
    jobids = jobids.union(getFilteredJobIDs(jobsUrl, token, state='donefailed'))
    if args.id:
        jobids = jobids.intersection(getFilteredJobIDs(jobsUrl, token, id=args.id))
    if args.name:
        jobids = jobids.intersection(getFilteredJobIDs(jobsUrl, token, name=args.name))
    if args.state:
        jobids = jobids.intersection(getFilteredJobIDs(jobsUrl, token, state=args.state))

    # fetch job result for every job
    for jobid in jobids:
        params = {'token': token, 'id': jobid}
        try:
            r = requests.get(resultsUrl, params=params, stream=True)
        except Exception as e:
            print('error: result request: {}'.format(str(e)))
            continue

        if r.status_code != 200:
            #print('error: request response: {} - {}'.format(r.status_code, r.json()['msg']))
            print('error: request response: {} - {}'.format(r.status_code, r.text))
            continue

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
            continue

        print('{} - results stored in {}'.format(r.status_code, dirname))


        try:
            r = requests.delete(jobsUrl, params=params)
        except Exception as e:
            print('error: clean request: {}'.format(str(e)))
            continue

        if r.status_code != 200:
            print('error cleaning job: {}'.format(r.json()['msg']))
            continue

        cleandCache(conf, args, jobid)


if __name__ == '__main__':
    main()
