import argparse
import sys
import requests
import os
import arc

from config import parseNonParamConf, DEFAULT_TOKEN_PATH
from common import readTokenFile, addCommonArgs


def main():

    confDict = {}

    parser = argparse.ArgumentParser(description='Submit job to aCT server')
    addCommonArgs(parser)
    parser.add_argument('--site', default='default',
            help='site that jobs should be submitted to')
    parser.add_argument('xRSL', help='path to job description file')
    args = parser.parse_args()

    confDict['proxy']  = args.proxy
    confDict['server'] = args.server
    confDict['port']   = args.port

    parseNonParamConf(confDict, args.conf)

    token = readTokenFile(DEFAULT_TOKEN_PATH)

    try:
        with open(args.xRSL, 'r') as f:
            xrslStr = f.read()
    except Exception as e:
        print('error: xRSL file open: {}'.format(str(e)))
        sys.exit(1)

    # parse job description and input files first before doing anything else
    jobdescs = arc.JobDescriptionList()
    parseResult = arc.JobDescription_Parse(xrslStr, jobdescs)
    #if not arc.JobDescription_Parse(xrslStr, jobdescs):
    #    print('error: job description parse error')
    if not parseResult:
        sys.exit(1)
    files = [] # list of tuples of file name and path
    for infile in jobdescs[0].DataStaging.InputFiles:
        # TODO: add validation for different types of URLs
        path = infile.Sources[0].FullPath()
        if not path:
            path = infile.Name
        if not os.path.isfile(path):
            continue
        files.append((infile.Name, path))

    # submit job to receive jobid
    baseUrl= '{}:{}'.format(confDict['server'], str(confDict['port']))
    requestUrl = '{}/jobs'.format(baseUrl)
    jsonDict = {'site': args.site, 'desc': xrslStr}
    params = {'token': token}
    try:
        r = requests.post(requestUrl, json=jsonDict, params=params)
    except Exception as e:
        print('error: request: {}'.format(str(e)))
        sys.exit(1)
    jsonDict = r.json()
    if r.status_code != 200:
        print('error: request response: {} - {}'.format(r.status_code, jsonDict['msg']))
        sys.exit(1)
    jobid = jsonDict['id']

    # upload data files for given job
    requestUrl = '{}/data'.format(baseUrl, jobid)
    params = {'token': token, 'id': jobid}
    for name, path in files:
        filesDict = {'file': (name, open(path, 'rb'))}
        # TODO: handle exceptions
        r = requests.put(requestUrl, files=filesDict, params=params)
        if r.status_code != 200:
            print('error: unsuccessful upload of file {}: {} - {}'.format(path, r.status_code, r.json()['msg']))
            # TODO: what to do in such case? Kill job, retry?
            sys.exit(1)

    # complete job submission
    requestUrl = '{}/jobs'.format(baseUrl, jobid)
    jsonDict = {'id': jobid, 'desc': xrslStr}
    # TODO: handle exceptions
    r = requests.put(requestUrl, json=jsonDict, params=params)
    if r.status_code != 200:
        print('error: unsuccessful completion of job submission: {} - {}'.format(r.status_code, r.json()['msg']))
        sys.exit(1)

    jobid = r.json()['id']
    print('{} - succesfully submited job with id {}'.format(r.status_code, jobid))


if __name__ == '__main__':
    main()


