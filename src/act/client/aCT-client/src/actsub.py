import argparse
import sys
import os
import ssl
import arc
import asyncio
import aiohttp
import aiofiles

from config import loadConf, checkConf, expandPaths
from common import readTokenFile, addCommonArgs, cleandCache


TRANSFER_BLOCK_SIZE = 2**16


# TODO: exceptions for aiohttp and aiofiles
# TODO: response from dcache when removing directory is 204. Should we
#       fix on this or on less than 300?


async def webdav_mkdir(session, url):
    headers = {'Accept': '*/*', 'Connection': 'Keep-Alive'}
    async with session.request('MKCOL', url, headers=headers) as resp:
        if resp.status != 201:
            print('error: cannot create dCache directory {}: {} - {}'.format(url, resp.status, await resp.text()))
            return False
        else:
            return True


# https://docs.aiohttp.org/en/stable/client_quickstart.html?highlight=upload#streaming-uploads
async def file_sender(filename):
    async with aiofiles.open(filename, "rb") as f:
        chunk = await f.read(TRANSFER_BLOCK_SIZE)
        while chunk:
            yield chunk
            chunk = await f.read(TRANSFER_BLOCK_SIZE)


async def webdav_put(session, url, path):
    timeout = aiohttp.ClientTimeout(total=900)
    async with session.put(url, data=file_sender(path), timeout=timeout) as resp:
        if resp.status != 201:
            print('error: cannot upload file {} to dCache URL {}: {} - {}'.format(path, url, resp.status, await resp.text()))
            return False
        else:
            return True


async def kill_jobs(session, url, jobids, token):
    ids = ','.join(map(str, jobids))
    params = {'token': token, 'id': ids}
    json = {'arcstate': 'tocancel'}
    async with session.patch(url, json=json, params=params) as resp:
        json = await resp.json()
        if resp.status != 200:
            print('error: killing jobs with failed input files: {} - {}'.format(resp.status, json['msg']))


def main():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(program())


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
        conf['port']   = args.port

    expandPaths(conf)
    checkConf(conf, ['server', 'port', 'token'])

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

        useDcache = True
    else:
        useDcache = False

    token = readTokenFile(conf['token'])

    async with aiohttp.ClientSession() as session:
        # list of jobs whose input transfers failed and need to be killed
        tokill = []

        for desc in args.xRSL:
            # read job description from
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
            json = {'site': args.site, 'desc': xrslStr}
            params = {'token': token}
            # TODO: exceptions
            async with session.post(requestUrl, json=json, params=params) as resp:
                json = await resp.json()
                if resp.status != 200:
                    print('error: POST /jobs: {} - {}'.format(resp.status, json['msg']))
                    continue
                json = await resp.json()
                jobid = json['id']

            # create directory for job's local input files if using dcache
            if useDcache:
                if not await webdav_mkdir(dcsession, dcacheBase + '/' + str(jobid)):
                    tokill.append(jobid)
                    continue

            requestUrl = baseUrl + '/data'
            params = {'token': token, 'id': jobid}

            # upload local input files
            transferFail = False
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
                if useDcache: # TODO: exceptions
                    # upload to dcache
                    dst = '{}/{}/{}'.format(dcacheBase, jobid, infile.Name)
                    jobdescs[0].DataStaging.InputFiles[i].Sources[0] = arc.SourceType(dst)
                    if not await webdav_put(dcsession, dst, path):
                        transferFail = True
                        break
                else: # TODO: exceptions
                    # upload to internal data management
                    data = aiohttp.FormData()
                    data.add_field('file', file_sender(path), filename=infile.Name)
                    async with session.put(requestUrl, data=data, params=params) as resp:
                        json = await resp.json()
                        if resp.status != 200:
                            print('error: PUT /data: {} - {}'.format(resp.status, json['msg']))
                            transferFail = True
                            break

            if transferFail:
                tokill.append(jobid)
                continue

            # job description was modified and has to be unparsed
            if useDcache:
                # TODO: errors
                xrslStr = jobdescs[0].UnParse('', '')[1]

            # complete job submission
            requestUrl = baseUrl + '/jobs'
            json = {'id': jobid, 'desc': xrslStr}
            # TODO: exceptions
            async with session.put(requestUrl, json=json, params=params) as resp:
                json = await resp.json()
                if resp.status != 200:
                    print('error: PUT /jobs: {} - {}'.format(resp.status, json['msg']))
                    tokill.append(jobid)
                else:
                    print('{} - succesfully submited job with id {}'.format(resp.status, json['id']))

            jobdescs = None # Weird error with JobDescriptionList destructor?


        if tokill:
            print('killing jobs: {}'.format(tokill))
            await kill_jobs(session, baseUrl + '/jobs', tokill, token)

        if useDcache:
            await dcsession.close() # close dCache context that is not handled in with statement
            await cleandCache(conf, args, tokill) # uses its own session context
