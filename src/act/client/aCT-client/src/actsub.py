import argparse
import os
import sys

import arc
import httpx
import trio

from common import (addCommonArgs, clean_webdav, readTokenFile,
                    runWithSIGINTHandler, getProxyCertClient)
from config import checkConf, expandPaths, loadConf

TRANSFER_BLOCK_SIZE = 2**16


async def webdav_mkdir(client, url):
    headers = {'Accept': '*/*', 'Connection': 'Keep-Alive'}
    try:
        resp = await client.request('MKCOL', url, headers=headers)
    except httpx.RequestError as e:
        print('request error: {}'.format(e))
        return

    if resp.status_code != 201:
        print('error: cannot create dCache directory {}: {} - {}'.format(url, resp.status_code, resp.text))
        return False
    else:
        return True


# https://docs.aiohttp.org/en/stable/client_quickstart.html?highlight=upload#streaming-uploads
#
# Exceptions are handled in code that uses this
async def file_sender(filename):
    async with await trio.open_file(filename, "rb") as f:
        chunk = await f.read(TRANSFER_BLOCK_SIZE)
        while chunk:
            yield chunk
            chunk = await f.read(TRANSFER_BLOCK_SIZE)


async def webdav_put(client, url, path, results):
    with trio.move_on_after(900):
        try:
            resp = await client.put(url, content=file_sender(path))
        except httpx.RequestError as e:
            print('request error: {}'.format(e))
            results.append(False)
            return

    if resp.status_code != 201:
        print('error: cannot upload file {} to dCache URL {}: {} - {}'.format(path, url, resp.status_code, resp.text))
        results.append(False)
    else:
        results.append(True)


async def http_put(client, name, path, url, jobid, token, results):
    headers = {'Authorization': 'Bearer ' + token}
    params = {'id': jobid, 'filename': name}
    with trio.move_on_after(900):
        try:
            resp = await client.put(url, content=file_sender(path), params=params, headers=headers)
            json = resp.json()
        except httpx.RequestError as e:
            print('request error: {}'.format(e))
            results.append(False)
            return

    if resp.status_code != 200:
        print('error: PUT /data: {} - {}'.format(resp.status_code, json['msg']))
        results.append(False)
    else:
        results.append(True)


async def kill_jobs(client, url, jobids, token):
    ids = ','.join(map(str, jobids))
    headers = {'Authorization': 'Bearer ' + token}
    params = {'id': ids}
    json = {'arcstate': 'tocancel'}
    print('Cleaning up failed or cancelled jobs ...')
    try:
        resp = await client.patch(url, json=json, params=params, headers=headers)
        json = resp.json()
    except httpx.RequestError as e:
        print('request error: {}'.format(e))
        return

    if resp.status_code != 200:
        print('error: killing jobs with failed input files: {} - {}'.format(resp.status_code, json['msg']))


async def upload_input_files(client, jobid, jobdesc, token, requestUrl, dcacheBase=None, dcclient=None):
    files = {}

    exepath = jobdesc.Application.Executable.Path
    if not os.path.isabs(exepath):
        files[os.path.basename(exepath)] = exepath

    # if a file with the same name as executable is provided in inputFiles
    # in xRSL the value from inputFiles will be used (or file entry discarded
    # if the executable file is remote)
    for i in range(len(jobdesc.DataStaging.InputFiles)):
        # we use index for access to InputFiles because changes
        # (for dcache) are not preserved otherwise?
        infile = jobdesc.DataStaging.InputFiles[i]

        if exepath == infile.Name and exepath in files:
            del files[exepath]

        # TODO: add validation for different types of URLs
        path = infile.Sources[0].FullPath()
        if not path:
            path = infile.Name
        if not os.path.isfile(path):
            continue
        if dcacheBase:
            dst = '{}/{}/{}'.format(dcacheBase, jobid, infile.Name)
            jobdesc.DataStaging.InputFiles[i].Sources[0] = arc.SourceType(dst)
            files[dst] = path
        else:
            files[infile.Name] = path

    if not files:
        return True

    results = []
    try:
        async with trio.open_nursery() as tasks:
            for dst, src in files.items():
                if dcacheBase:  # upload to dcache
                    tasks.start_soon(webdav_put, dcclient, dst, src, results)
                else:  # upload to internal data management
                    tasks.start_soon(http_put, client, dst, src, requestUrl, jobid, token, results)
    except trio.Cancelled:
        return False

    return all(results)


# TODO: could add checking if jobs list is empty for earlier exit
async def submit_jobs(client, descs, baseUrl, clusterlist, token, dcacheBase=None, dcclient=None):
    # read job descriptions into a list of job dictionaries
    jobs = []
    for desc in descs:
        job = {'clusterlist': clusterlist}
        try:
            with open(desc, 'r') as f:
                job['desc'] = f.read()
            job['descpath'] = desc
        except Exception as e:
            print('error: Job description file {}: {}'.format(desc, e))
            continue
        jobs.append(job)

    # submit jobs to aCT
    requestUrl = baseUrl + '/jobs'
    json = [{k: v for k, v in job.items() if k in ('desc', 'clusterlist')} for job in jobs]
    headers = {'Authorization': 'Bearer ' + token}
    try:
        resp = await client.post(requestUrl, headers=headers, json=json)
        json = resp.json()
    except httpx.RequestError as e:
        print('request error: {}'.format(e))
        return []
    except trio.Cancelled:
        return []
    if resp.status_code != 200:
        print('error: POST /jobs: {} - {}'.format(resp.status_code, json['msg']))
        return []

    # print errors for failed jobs and remove them
    for job, result in zip(jobs, json):
        if 'msg' in result:
            if 'name' in result:
                print('error: job {}: {}'.format(result['name'], result['msg']))
            else:
                print('error: job description {}: {}'.format(job['descpath'], result['msg']))
        else:
            job['name'] = result['name']
            job['id'] = result['id']
    jobs = [job for job, result in zip(jobs, json) if 'msg' not in result]

    # parse job descriptions
    tokill = []
    jobdescs = arc.JobDescriptionList()
    for job in jobs:
        if not arc.JobDescription_Parse(job['desc'], jobdescs):
            print('error: Parsing fail for job description {}'.format(job['descpath']))
            jobs.remove(job)
            tokill.append(job['id'])
            continue

    # upload input files
    #
    # A job should be killed unless data upload succeeds. Data upload function
    # should remove jobid from kill list on successful file upload.
    tokill.extend([job['id'] for job in jobs])
    try:
        async with trio.open_nursery() as tasks:
            for i in range(len(jobs)):
                tasks.start_soon(upload_job_data, client, jobs[i], jobdescs[i], token, tokill, baseUrl + '/data', dcacheBase, dcclient)
    except trio.Cancelled:
        return tokill
    jobs = [job for job in jobs if job['id'] not in tokill]

    # complete job submission
    requestUrl = baseUrl + '/jobs'
    headers = {'Authorization': 'Bearer ' + token}
    json = [{k: v for k, v in job.items() if k in ('desc', 'id')} for job in jobs]
    try:
        resp = await client.put(requestUrl, json=json, headers=headers)
        json = resp.json()
    except httpx.RequestError as e:
        print('request error: {}'.format(e))
        return tokill.extend([job['id'] for job in jobs])
    except trio.Cancelled:
        return tokill.extend([job['id'] for job in jobs])

    if resp.status_code != 200:
        print('error: PUT /jobs: {} - {}'.format(resp.status_code, json['msg']))
        return tokill.extend([job['id'] for job in jobs])

    # print submission results
    for job, result in zip(jobs, json):
        if 'msg' in result:
            print('error: job {}: {}'.format(job['name'], result['msg']))
            tokill.append(job['id'])
        else:
            print('Successfuly inserted job {} with ID {}'.format(job['name'], job['id']))

    return tokill


# function removes successful jobs from tokill list
async def upload_job_data(client, job, jobdesc, token, tokill, dataUrl, dcacheBase=None, dcclient=None):
    try:
        # create directory for job's local input files if using dcache
        if dcacheBase:
            url = dcacheBase + '/' + str(job['id'])
            if not await webdav_mkdir(dcclient, url):
                return

        # upload input files
        if not await upload_input_files(client, job['id'], jobdesc, token, dataUrl, dcacheBase, dcclient):
            return

        # job description was modified and has to be unparsed
        if dcacheBase:
            xrslStr = jobdesc.UnParse('', '')[1]
            if not xrslStr:
                print('error: generating job description file')
                return
            else:
                job['desc'] = xrslStr

    except trio.Cancelled:
        return

    tokill.remove(job['id'])


def main():
    trio.run(runWithSIGINTHandler, program)


async def cleanup(client, token, jobsURL, tokill, conf, args, dcclient):
    await kill_jobs(client, jobsURL, tokill, token)
    # function also closes client
    await clean_webdav(conf, args, tokill, dcclient)


async def program():
    parser = argparse.ArgumentParser(description='Submit job to aCT server')
    addCommonArgs(parser)
    parser.add_argument('--clusterlist', default='default',
                        help='a name of a list of clusters specified in config under "clusters" option OR a comma separated list of cluster URLs')
    parser.add_argument('xRSL', nargs='+', help='path to job description file')
    parser.add_argument('--dcache', nargs='?', const='dcache', default='',
            help='URL of user\'s dCache directory')
    args = parser.parse_args()

    conf = loadConf(path=args.conf)

    # override values from configuration
    if args.server:
        conf['server'] = args.server
    if args.port:
        conf['port'] = args.port

    expandPaths(conf)
    checkConf(conf, ['server', 'port', 'token'])

    token = readTokenFile(conf['token'])

    if 'clusters' in conf:
        if args.clusterlist in conf['clusters']:
            clusterlist = conf['clusters'][args.clusterlist]
        else:
            clusterlist = args.clusterlist.split(',')
    else:
        clusterlist = args.clusterlist.split(',')

    # get dcache location, create ssl and aio context
    if args.dcache:
        checkConf(conf, ['proxy'])
        if args.dcache == 'dcache':
            dcacheBase = conf.get('dcache', '')
            if not dcacheBase:
                print('error: dcache location not configured')
                sys.exit(1)
        else:
            dcacheBase = args.dcache

        dcclient = getProxyCertClient(conf['proxy'])
    else:
        dcacheBase = None
        dcclient = None

    baseUrl = conf['server'] + ':' + str(conf['port'])

    async with httpx.AsyncClient(limits=httpx.Limits(max_keepalive_connections=1, max_connections=1)) as client:

        tokill = []
        try:
            tokill = await submit_jobs(client, args.xRSL, baseUrl, clusterlist, token, dcacheBase, dcclient)
        except trio.Cancelled:
            pass
        finally:
            with trio.CancelScope(shield=True):
                if tokill:
                    await cleanup(client, token, baseUrl + '/jobs', tokill, conf, args, dcclient)
