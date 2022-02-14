import argparse
import os
import ssl
import sys

import arc
import trio
import httpx

from common import addCommonArgs, clean_webdav, readTokenFile, run_with_sigint_handler
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


# TODO: environment variables in paths!!!
async def upload_input_files(client, jobid, jobdescs, token, requestUrl, dcacheBase=None, dcclient=None):
    files = {}

    exepath = jobdescs[0].Application.Executable.Path
    if not os.path.isabs(exepath):
        files[os.path.basename(exepath)] = exepath

    # if a file with the same name as executable is provided in inputFiles
    # in xRSL the value from inputFiles will be used (or file entry discarded
    # if the executable file is remote)
    for i in range(len(jobdescs[0].DataStaging.InputFiles)):
        # we use index for access to InputFiles because changes
        # (for dcache) are not preserved otherwise?
        infile = jobdescs[0].DataStaging.InputFiles[i]

        if exepath == infile.Name:
            del files[exepath]

        # TODO: add validation for different types of URLs
        path = infile.Sources[0].FullPath()
        if not path:
            path = infile.Name
        if not os.path.isfile(path):
            continue
        if dcacheBase:
            dst = '{}/{}/{}'.format(dcacheBase, jobid, infile.Name)
            jobdescs[0].DataStaging.InputFiles[i].Sources[0] = arc.SourceType(dst)
            files[dst] = path
        else:
            files[infile.Name] = path

    if not files:
        return True

    results = []
    async with trio.open_nursery() as tasks:
        for dst, src in files.items():
            if dcacheBase:  # upload to dcache
                tasks.start_soon(webdav_put, dcclient, dst, src, results)
            else:  # upload to internal data management
                tasks.start_soon(http_put, client, dst, src, requestUrl, jobid, token, results)

    return all(results)


# if jobdescs are not set to None then the following error happens:
# <built-in function delete_JobDescriptionList> returned a result with an error set
# StopIteration: (1709, True)
async def submit_job(client, descpath, baseUrl, site, token, tokill, dcacheBase=None, dcclient=None):
    # read and parse job description
    try:
        with open(descpath, 'r') as f:
            xrslStr = f.read()
    except Exception as e:
        print('error reading job description file {}: {}'.format(descpath, e))
        return
    jobdescs = arc.JobDescriptionList()
    if not arc.JobDescription_Parse(xrslStr, jobdescs):
        print('error: parse error in job description {}'.format(descpath))
        return

    # submit job to receive jobid
    requestUrl = baseUrl + '/jobs'
    json = {'site': site, 'desc': xrslStr}
    headers = {'Authorization': 'Bearer ' + token}
    try:
        with trio.CancelScope(shield=True):
            resp = await client.post(requestUrl, headers=headers, json=json)
            json = resp.json()
    except httpx.RequestError as e:
        print('request error: {}'.format(e))
        return
    if resp.status_code != 200:
        print('error: POST /jobs: {} - {}'.format(resp.status_code, json['msg']))
        return
    jobid = json['id']

    try:
        # create directory for job's local input files if using dcache
        if dcacheBase:
            url = dcacheBase + '/' + str(jobid)
            if not await webdav_mkdir(dcclient, url):
                print('error: could not create webdav directory {}'.format(url))
                tokill.append(jobid)
                return

        # upload input files
        requestUrl = baseUrl + '/data'
        if not await upload_input_files(client, jobid, jobdescs, token, requestUrl, dcacheBase, dcclient):
            print('error: uploading input files')
            tokill.append(jobid)
            return

        # job description was modified and has to be unparsed
        if dcacheBase:
            xrslStr = jobdescs[0].UnParse('', '')[1]
            if not xrslStr:
                print('error: generating job description file')
                tokill.append(jobid)
                return

    except trio.Cancelled:
        tokill.append(jobid)
        return

    # complete job submission
    requestUrl = baseUrl + '/jobs'
    headers = {'Authorization': 'Bearer ' + token}
    json = {'id': jobid, 'desc': xrslStr}
    try:
        with trio.CancelScope(shield=True):
            resp = await client.put(requestUrl, json=json, headers=headers)
            json = resp.json()
    except httpx.RequestError as e:
        print('request error: {}'.format(e))
        tokill.append(jobid)
        return
    #except trio.Cancelled:
    #    tokill.append(jobid)
    #    return

    if resp.status_code != 200:
        print('error: PUT /jobs: {} - {}'.format(resp.status_code, json['msg']))
        tokill.append(jobid)
        return

    print('{} - succesfully submited job with id {}'.format(resp.status_code, json['id']))


def main():
    trio.run(run_with_sigint_handler, program)


async def cleanup(client, token, jobsURL, tokill, conf, args, dcclient):
    await kill_jobs(client, jobsURL, tokill, token)
    # function also closes client
    await clean_webdav(conf, args, tokill, dcclient)


async def program():
    parser = argparse.ArgumentParser(description='Submit job to aCT server')
    addCommonArgs(parser)
    parser.add_argument('--site', default='default',
            help='site that jobs should be submitted to')
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

        context = ssl.SSLContext(ssl.PROTOCOL_TLS)
        context.load_cert_chain(conf['proxy'], keyfile=conf['proxy'])
        _DEFAULT_CIPHERS = (
            'ECDH+AESGCM:DH+AESGCM:ECDH+AES256:DH+AES256:ECDH+AES128:DH+AES:ECDH+HIGH:'
            'DH+HIGH:ECDH+3DES:DH+3DES:RSA+AESGCM:RSA+AES:RSA+HIGH:RSA+3DES:!aNULL:'
            '!eNULL:!MD5'
        )
        context.set_ciphers(_DEFAULT_CIPHERS)

        dcclient = httpx.AsyncClient(verify=context)

    else:
        dcacheBase = None
        dcclient = None

    baseUrl = conf['server'] + ':' + str(conf['port'])

    async with httpx.AsyncClient() as client:

        tokill = []
        try:
            async with trio.open_nursery() as tasks:
                for desc in args.xRSL:
                    tasks.start_soon(submit_job, client, desc, baseUrl, args.site, token, tokill, dcacheBase, dcclient)
        except trio.Cancelled:
            pass

        if tokill:
            with trio.CancelScope(shield=True):
                await cleanup(client, token, baseUrl + '/jobs', tokill, conf, args, dcclient)
