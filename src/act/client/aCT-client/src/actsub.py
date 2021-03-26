import argparse
import sys
import requests
import os
import arc

from config import parseNonParamConf
from common import readProxyFile, addCommonArgs


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

    proxyStr = readProxyFile(confDict['proxy'])

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
        print(parseResult)
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
    form = {'site': args.site, 'proxy': proxyStr, 'xrsl': xrslStr}
    try:
        r = requests.put(requestUrl, data=form)
    except Exception as e:
        print('error: request: {}'.format(str(e)))
        sys.exit(1)
    if r.status_code != 200:
        print('error: request response: {} - {}'.format(r.status_code, r.text))
        sys.exit(1)
    jobid = int(r.text)

    # upload data files for given job
    requestUrl = '{}/data?id={}'.format(baseUrl, jobid)
    form = {'proxy': proxyStr}
    for name, path in files:
        filesDict = {'file': (name, open(path, 'rb'))}
        # TODO: handle exceptions
        r = requests.put(requestUrl, data=form, files=filesDict)
        if r.status_code != 200:
            print('error: unsuccessful upload of file {}: {} - {}'.format(path, r.status_code, r.text))
            # TODO: what to do in such case? Kill job, retry?
            sys.exit(1)

    # complete job submission
    requestUrl = '{}/jobs?id={}'.format(baseUrl, jobid)
    form = {'proxy': proxyStr, 'xrsl': xrslStr}
    # TODO: handle exceptions
    r = requests.put(requestUrl, data=form)
    if r.status_code != 200:
        print('error: unsuccessful completion of job submission: {} - {}'.format(r.status_code, r.text))
        sys.exit(1)

    jobid = int(r.text)
    print('{} - succesfully submited job with id {}'.format(r.status_code, jobid))


if __name__ == '__main__':
    main()


