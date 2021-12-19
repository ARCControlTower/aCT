import argparse
import sys
import os
import zipfile
import requests

from config import parseNonParamConf, DEFAULT_TOKEN_PATH
from common import readTokenFile, addCommonArgs, showHelpOnCommandOnly
from common import getIDParams


def main():

    confDict = {}

    parser = argparse.ArgumentParser(description='Submit proxy to aCT server')
    addCommonArgs(parser)
    parser.add_argument('-a', '--all', action='store_true',
            help='all jobs that match other criteria')
    parser.add_argument('--id', default=None,
            help='a list of IDs of jobs that should be queried')
    args = parser.parse_args()
    showHelpOnCommandOnly(parser)

    jobids = getIDParams(args)

    confDict['proxy']  = args.proxy
    confDict['server'] = args.server
    confDict['port']   = args.port

    parseNonParamConf(confDict, args.conf)

    token = readTokenFile(DEFAULT_TOKEN_PATH)

    urlBase = confDict['server'] + ':' + str(confDict['port'])
    resultsUrl = urlBase + '/results'
    jobsUrl = urlBase + '/jobs'

    # if -a flag is given, then IDs of all 'done' and 'donefailed' jobs need to
    # be fetched first
    if not jobids:
        # get IDs of all 'done' jobs
        params = {'token': token, 'client': 'id', 'state': 'done'}
        try:
            r = requests.get(jobsUrl, params=params)
        except Exception as e:
            print('error: request: "done" jobs: {}'.format(str(e)))
            sys.exit(1)
        if r.status_code != 200:
            print('error: response: "done" jobs: {} - {}'.format(r.status_code, r.json()['msg']))
            sys.exit(1)
        try:
            jsonResp = r.json()
        except ValueError as e:
            print('error: response: "done" jobs: JSON: {}'.format(str(e)))
            sys.exit(1)
        jobids.extend([job['c_id'] for job in jsonResp])

        # get IDs of all 'donefailed' jobs
        params = {'token': token, 'client': 'id', 'state': 'donefailed'}
        try:
            r = requests.get(jobsUrl, params=params)
        except Exception as e:
            print('error: request: "donefailed" jobs: {}'.format(str(e)))
            sys.exit(1)
        if r.status_code != 200:
            print('error: response: "donefailed" jobs: {} - {}'.format(r.status_code, r.json()['msg']))
            sys.exit(1)
        try:
            jsonResp = r.json()
        except ValueError as e:
            print('error: response: "donefailed" jobs: JSON: {}'.format(str(e)))
            sys.exit(1)
        jobids.extend([job['c_id'] for job in jsonResp])

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


if __name__ == '__main__':
    main()


