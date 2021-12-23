import argparse
import sys
import os
import subprocess
import requests
import arc

from config import loadConf, checkConf, expandPaths
from common import readTokenFile, addCommonArgs


def main():
    parser = argparse.ArgumentParser(description='Submit job to aCT server')
    addCommonArgs(parser)
    parser.add_argument('--site', default='default',
            help='site that jobs should be submitted to')
    parser.add_argument('xRSL', nargs='+', help='path to job description file')
    parser.add_argument('--dcache', nargs='?', const='dcache', default='',
            help='whether files should be uploaded to dcache with optional \
                  location parameter')
    args = parser.parse_args()

    conf = loadConf(path=args.conf)

    # override values from configuration
    if args.server:
        conf['server'] = args.server
    if args.port:
        conf['port']   = args.port

    expandPaths(conf)
    checkConf(conf, ['server', 'port', 'token'])

    # if dcache should be used get the location
    if args.dcache:
        if args.dcache == 'dcache':
            dcacheBase = conf.get('dcache', '')
            if not dcacheBase:
                print('error: dcache location not configured')
                sys.exit(1)
        else:
            dcacheBase = args.dcache
    else:
        dcacheBase = ''

    token = readTokenFile(conf['token'])

    for desc in args.xRSL:
        # read job description from file
        try:
            with open(desc, 'r') as f:
                xrslStr = f.read()
        except Exception as e:
            print('error: xRSL file open: {}'.format(str(e)))
            continue

        # parse job description
        jobdescs = arc.JobDescriptionList()
        if not arc.JobDescription_Parse(xrslStr, jobdescs):
            print('error: parse error in job description {}'.format(desc))
            continue

        # submit job to receive jobid
        baseUrl= conf['server'] + ':' + str(conf['port'])
        requestUrl = baseUrl + '/jobs'
        jsonDict = {'site': args.site, 'desc': xrslStr}
        params = {'token': token}
        try:
            r = requests.post(requestUrl, json=jsonDict, params=params)
        except Exception as e:
            print('error: request: {}'.format(str(e)))
            continue
        jsonDict = r.json()
        if r.status_code != 200:
            print('error: request response: {} - {}'.format(r.status_code, jsonDict['msg']))
            continue
        jobid = jsonDict['id']

        # upload local input files
        #
        # used for upload to internal data management
        requestUrl = baseUrl + '/data'
        params = {'token': token, 'id': jobid}
        for i in range(len(jobdescs[0].DataStaging.InputFiles)):
            # we use index for access to InputFiles because changes
            # (for dcache) are not preserved otherwise?
            infile = jobdescs[0].DataStaging.InputFiles[i]
            # TODO: add validation for different types of URLs
            path = infile.Sources[0].FullPath()
            if not path:
                path = infile.Name
            if not os.path.isfile(path):
                continue
            if not dcacheBase:
                # upload to internal data management
                filesDict = {'file': (infile.Name, open(path, 'rb'))}
                # TODO: handle exceptions
                r = requests.put(requestUrl, files=filesDict, params=params)
                if r.status_code != 200:
                    print('error: unsuccessful upload of file {}: {} - {}'.format(path, r.status_code, r.json()['msg']))
                    # TODO: what to do in such case? Kill job, retry?
                    continue
            else:
                # upload to dcache
                dst = '{}/{}/{}'.format(dcacheBase, jobid, infile.Name)
                jobdescs[0].DataStaging.InputFiles[i].Sources[0] = arc.SourceType(dst)
                result = subprocess.run(
                        ['/usr/bin/arccp', path, dst],
                        stdout=subprocess.PIPE,
                        stderr=subprocess.STDOUT
                )
                if result.returncode != 0:
                    print('error: transfer {} to {} failed: {}'.format(path, dst, result.stdout))

        # job description was modified and has to be unparsed
        if dcacheBase:
            # TODO: check for errors
            xrslStr = jobdescs[0].UnParse('', '')[1]

        # complete job submission
        requestUrl = baseUrl + '/jobs'
        jsonDict = {'id': jobid, 'desc': xrslStr}
        # TODO: handle exceptions
        r = requests.put(requestUrl, json=jsonDict, params=params)
        if r.status_code != 200:
            print('error: unsuccessful completion of job submission: {} - {}'.format(r.status_code, r.json()['msg']))
            continue

        jobid = r.json()['id']
        print('{} - succesfully submited job with id {}'.format(r.status_code, jobid))


if __name__ == '__main__':
    main()


