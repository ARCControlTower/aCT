import argparse
import asyncio
import os
import ssl
import sys

import aiofiles
import aiohttp
import arc

from common import addCommonArgs, cleandCache, disableSIGINT, readTokenFile
from config import checkConf, expandPaths, loadConf


TRANSFER_BLOCK_SIZE = 2**16


async def webdav_mkdir(session, url):
    headers = {'Accept': '*/*', 'Connection': 'Keep-Alive'}
    try:
        async with session.request('MKCOL', url, headers=headers) as resp:
            if resp.status != 201:
                print('error: cannot create dCache directory {}: {} - {}'.format(url, resp.status, await resp.text()))
                return False
            else:
                return True
    except aiohttp.ClientError as e:
        print('HTTP client error: creating WebDAV directory {}: {}'.format(url, e))
        return False


# https://docs.aiohttp.org/en/stable/client_quickstart.html?highlight=upload#streaming-uploads
#
# Exceptions are handled in code that uses this
async def file_sender(filename):
    async with aiofiles.open(filename, "rb") as f:
        chunk = await f.read(TRANSFER_BLOCK_SIZE)
        while chunk:
            yield chunk
            chunk = await f.read(TRANSFER_BLOCK_SIZE)


async def webdav_put(session, url, path):
    try:
        timeout = aiohttp.ClientTimeout(total=900)
        async with session.put(url, data=file_sender(path), timeout=timeout) as resp:
            if resp.status != 201:
                print('error: cannot upload file {} to dCache URL {}: {} - {}'.format(path, url, resp.status, await resp.text()))
                return False
            else:
                return True
    except (aiohttp.ClientError, Exception) as e:
        print('error uploading file {} to {}: {}'.format(path, url, e))
        return False


async def http_put(session, name, path, url, jobid, token):
    try:
        data = aiohttp.FormData()
        data.add_field('file', file_sender(path), filename=name)
        headers = {'Authorization': 'Bearer ' + token}
        params = {'id': jobid}
        async with session.put(url, data=data, params=params, headers=headers) as resp:
            json = await resp.json()
            if resp.status != 200:
                print('error: PUT /data: {} - {}'.format(resp.status, json['msg']))
                return False
    except aiohttp.ClientError as e:
        print('error uploading input file {} stored in {}: {}'.format(name, path, e))
        return False
    return True


async def kill_jobs(session, url, jobids, token):
    ids = ','.join(map(str, jobids))
    headers = {'Authorization': 'Bearer ' + token}
    params = {'id': ids}
    json = {'arcstate': 'tocancel'}
    try:
        async with session.patch(url, json=json, params=params, headers=headers) as resp:
            json = await resp.json()
            if resp.status != 200:
                print('error: killing jobs with failed input files: {} - {}'.format(resp.status, json['msg']))
    except aiohttp.ClientError as e:
        print('HTTP client error: while killing jobs: {}'.format(e))


async def upload_input_files(session, jobid, jobdescs, token, requestUrl, dcacheBase=None, dcsession=None):
    tasks = []
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
        if dcacheBase:
            # upload to dcache
            dst = '{}/{}/{}'.format(dcacheBase, jobid, infile.Name)
            jobdescs[0].DataStaging.InputFiles[i].Sources[0] = arc.SourceType(dst)
            tasks.append(asyncio.ensure_future(webdav_put(dcsession, dst, path)))
        else:
            # upload to internal data management
            tasks.append(asyncio.ensure_future(http_put(session, infile.Name, path, requestUrl, jobid, token)))
    try:
        results = await asyncio.gather(*tasks)
    except Exception as e:
        print('error running concurrent input file upload: {}'.format(e))
        return False

    return all(results)


# returns a tuple of jobid integer (can be None) and bool that specifies
# whether given jobid has to be killed because of failure
#
# if jobdescs are not set to None then the following error happens:
# <built-in function delete_JobDescriptionList> returned a result with an error set
# StopIteration: (1709, True)
async def submit_job(session, descpath, baseUrl, site, token, dcacheBase=None, dcsession=None):
    # read and parse job description
    try:
        with open(descpath, 'r') as f:
            xrslStr = f.read()
    except Exception as e:
        print('error reading job description file {}: {}'.format(descpath, e))
        return None, False
    jobdescs = arc.JobDescriptionList()
    if not arc.JobDescription_Parse(xrslStr, jobdescs):
        print('error: parse error in job description {}'.format(descpath))
        jobdescs = None
        return None, False

    # submit job to receive jobid
    requestUrl = baseUrl + '/jobs'
    json = {'site': site, 'desc': xrslStr}
    headers = {'Authorization': 'Bearer ' + token}
    try:
        async with session.post(requestUrl, json=json, headers=headers) as resp:
            json = await resp.json()
            status = resp.status

    ## It seems that there is a possibility in both error cases that the submission
    ## could already be finished by the time exception is caught. clean just
    ## in case, at worst we get error that job doesn't exist.
    #except asyncio.CancelledError:
    #    return None, True
    except aiohttp.ClientError as e:
        print('HTTP client error: submitting job description {}: {}'.format(descpath, e))
        return None, True

    if status != 200:
        print('error: POST /jobs: {} - {}'.format(status, json['msg']))
        return None, False
    jobid = json['id']

    # handle coroutine cancel, signal killing if cancelled
    try:
        # create directory for job's local input files if using dcache
        if dcacheBase:
            if not await webdav_mkdir(dcsession, dcacheBase + '/' + str(jobid)):
                return jobid, True

        # upload input files
        requestUrl = baseUrl + '/data'
        if not await upload_input_files(session, jobid, jobdescs, token, requestUrl, dcacheBase, dcsession):
            jobdescs = None
            return jobid, True

    except asyncio.CancelledError:
        return jobid, True

    # job description was modified and has to be unparsed
    if dcacheBase:
        xrslStr = jobdescs[0].UnParse('', '')[1]
        if not xrslStr:
            return jobid, True

    # complete job submission
    requestUrl = baseUrl + '/jobs'
    headers = {'Authorization': 'Bearer ' + token}
    json = {'id': jobid, 'desc': xrslStr}
    try:
        async with session.put(requestUrl, json=json, headers=headers) as resp:
            json = await resp.json()
            status = resp.status
    except aiohttp.ClientError as e:
        print('HTTP client error: submitting job {}: {}'.format(descpath, e))
        return jobid, True
    except asyncio.CancelledError:
        return jobid, True

    if status != 200:
        print('error: PUT /jobs: {} - {}'.format(status, json['msg']))
        return jobid, True
    else:
        print('{} - succesfully submited job with id {}'.format(status, json['id']))

    jobdescs = None

    return jobid, False # success


def main():
    try:
        disableSIGINT()
        loop = asyncio.get_event_loop()
        loop.run_until_complete(program())
    except Exception as e:
        print('error: {}'.format(e))
        sys.exit(1)


async def program():
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

        connector = aiohttp.TCPConnector(ssl=context)
        dcsession = aiohttp.ClientSession(connector=connector)

    else:
        dcacheBase = None
        dcsession = None

    baseUrl= conf['server'] + ':' + str(conf['port'])

    async with aiohttp.ClientSession() as session:
        # submit jobs
        #tasks = []
        #try:
        #    for desc in args.xRSL:
        #        tasks.append(asyncio.ensure_future(submit_job(session, desc, baseUrl, args.site, token, dcacheBase, dcsession)))
        #    results = await asyncio.gather(*tasks)
        #except asyncio.CancelledError:
        #    for task in tasks:
        #        task.cancel()
        #finally:
        #    results = await asyncio.gather(*tasks)
        tokill = []
        for desc in args.xRSL:
            jobid, kill = await submit_job(session, desc, baseUrl, args.site, token, dcacheBase, dcsession)
            if kill:
                tokill.append(jobid)

        # !!!!!!!!!!!!!!!!!!!!
        # https://stackoverflow.com/questions/56052748/python-asyncio-task-cancellation
        # !!!!!!!!!!!!!!!!!!!!

        # Failed or cancelled job cleanup and dCache have to be cleaned
        # even if the coroutine is cancelled so that half submitted or
        # improperly submitted jobs are not left behind.

        # kill unsuccessfully submitted jobs
        #tokill = [jobid for jobid, shouldKill in results if shouldKill]
        if tokill:
            print('cleaning failed or cancelled jobs: {}'.format(tokill))
            await kill_jobs(session, baseUrl + '/jobs', tokill, token)

        if dcsession and tokill:
            # function closes session in its own async with statement
            await cleandCache(conf, args, tokill, session=dcsession)
