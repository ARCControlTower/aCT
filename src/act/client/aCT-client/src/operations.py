import os
import shutil
import zipfile

import arc
import httpx
import trio
from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from json.decoder import JSONDecodeError

from common import ACTClientError, readFile, JobCleanup
from delegate_proxy import parse_issuer_cred
from x509proxy import sign_request


# TODO: use proper data structures for API rather than format expected
#       on backend; also use kwargs
# TODO: unify API: PATCH tocancel returns job dicts, others return list
#       of job IDs
# TODO: properly set up limits and timeouts for httpx library and handle
#       timeout errors


TRANSFER_BLOCK_SIZE = 2**16  # TODO: hardcoded


async def cleanJobs(client, url, token, params):
    url += '/jobs'
    headers = {'Authorization': 'Bearer ' + token}
    try:
        resp = await client.delete(url, params=params, headers=headers)
        json = resp.json()
    except httpx.RequestError as e:
        raise ACTClientError(f'Request error: {e}')
    except JSONDecodeError as e:
        raise ACTClientError(f'Response JSON decode error: {e}; Is your aCT URL correct?')
    except Exception as e:
        raise ACTClientError(f'{e}')
    if resp.status_code != 200:
        raise ACTClientError(f'Response error: {json["msg"]}')
    return json


async def patchJobs(client, url, token, params, arcstate):
    if arcstate not in ('tofetch', 'tocancel', 'toresubmit'):
        raise ACTClientError(f'Invalid arcstate argument "{arcstate}"')

    url += '/jobs'
    json = {'arcstate': arcstate}
    headers = {'Authorization': 'Bearer ' + token}
    try:
        resp = await client.patch(url, json=json, params=params, headers=headers)
        json = resp.json()
    except httpx.RequestError as e:
        raise ACTClientError(f'Request error: {e}')
    except JSONDecodeError as e:
        raise ACTClientError(f'Response JSON decode error: {e}; Is your aCT URL correct?')
    except Exception as e:
        raise ACTClientError(f'{e}')
    if resp.status_code != 200:
        raise ACTClientError(f'Response error: {json["msg"]}')
    return json


async def fetchJobs(client, url, token, params):
    return await patchJobs(client, url, token, params, 'tofetch')


async def killJobs(client, url, token, params):
    return await patchJobs(client, url, token, params, 'tocancel')


async def resubmitJobs(client, url, token, params):
    return await patchJobs(client, url, token, params, 'toresubmit')


async def postJobs(client, url, token, jobs):
    url += '/jobs'
    headers = {'Authorization': 'Bearer ' + token}
    try:
        resp = await client.post(url, headers=headers, json=jobs)
        json = resp.json()
    except httpx.RequestError as e:
        raise ACTClientError(f'Request error: {e}')
    except JSONDecodeError as e:
        raise ACTClientError(f'Response JSON decode error: {e}; Is your aCT URL correct?')
    except Exception as e:
        raise ACTClientError(f'{e}')
    if resp.status_code != 200:
        raise ACTClientError(f'Response error: {json["msg"]}')
    return json


async def putJobs(client, url, token, jobs):
    url += '/jobs'
    headers = {'Authorization': 'Bearer ' + token}
    try:
        resp = await client.put(url, json=jobs, headers=headers)
        json = resp.json()
    except httpx.RequestError as e:
        raise ACTClientError(f'Request error: {e}')
    except JSONDecodeError as e:
        raise ACTClientError(f'Response JSON decode error: {e}; Is your aCT URL correct?')
    except Exception as e:
        raise ACTClientError(f'{e}')
    if resp.status_code != 200:
        raise ACTClientError(f'Response error: {json["msg"]}')
    return json


async def cleanWebDAV(client, url, jobids):
    if not jobids:
        return []
    async with trio.open_nursery() as tasks:
        errors = []
        for jobid in jobids:
            dirUrl = url + '/' + str(jobid)
            tasks.start_soon(errorAdapter, errors, webdavRmdir, client, dirUrl)
    return errors


async def webdavRmdir(client, url):
    headers = {'Accept': '*/*', 'Connection': 'Keep-Alive'}
    try:
        resp = await client.delete(url, headers=headers)
    except httpx.RequestError as e:
        raise ACTClientError(f'Request error: {e}')
    except Exception as e:
        raise ACTClientError(f'{e}')
    # TODO: should we rely on 204 and 404 being the only right answers?
    if resp.status_code == 404:  # ignore, because we are just trying to delete
        return
    if resp.status_code >= 300:
        raise ACTClientError('Unexpected response for removal of WebDAV directory')


async def webdavMkdir(client, url):
    headers = {'Accept': '*/*', 'Connection': 'Keep-Alive'}
    try:
        resp = await client.request('MKCOL', url, headers=headers)
    except httpx.RequestError as e:
        raise ACTClientError(f'Request error: {e}')
    except Exception as e:
        raise ACTClientError(f'{e}')
    if resp.status_code != 201:
        raise ACTClientError(f'Error creating WebDAV directory {url}: {resp.text}')


async def webdavPut(client, url, path):
    try:
        resp = await client.put(url, content=fileSender(path))
    except httpx.RequestError as e:
        raise ACTClientError(f'Request error: {e}')
    except Exception as e:
        raise ACTClientError(f'{e}')
    except trio.Cancelled:
        raise ACTClientError(f'Upload cancelled for file {path} to {url}')
    if resp.status_code != 201:
        raise ACTClientError(f'Error uploading file {path} to {url}: {resp.text}')


async def httpPut(client, url, token, name, path, jobid):
    url += '/data'
    headers = {'Authorization': 'Bearer ' + token}
    params = {'id': jobid, 'filename': name}
    # how to distinguish between timeout cancel and ctrl-c cancel
    try:
        resp = await client.put(url, content=fileSender(path), params=params, headers=headers)
        json = resp.json()
    except httpx.RequestError as e:
        raise ACTClientError(f'Request error: {e}')
    except JSONDecodeError as e:
        raise ACTClientError(f'Response JSON decode error: {e}; Is your aCT URL correct?')
    except Exception as e:
        raise ACTClientError(f'{e}')
    except trio.Cancelled:
        raise ACTClientError(f'Upload cancelled fot file {path} to {url}')
    if resp.status_code != 200:
        raise ACTClientError(f'Error uploading file {path} to {url}: {json["msg"]}')


# https://docs.aiohttp.org/en/stable/client_quickstart.html?highlight=upload#streaming-uploads
#
# Exceptions are handled in code that uses this
async def fileSender(filename):
    async with await trio.open_file(filename, "rb") as f:
        chunk = await f.read(TRANSFER_BLOCK_SIZE)
        while chunk:
            yield chunk
            chunk = await f.read(TRANSFER_BLOCK_SIZE)


async def getJobStats(client, url, token, **kwargs):
    url += '/jobs'
    PARAM_KEYS = ('id', 'name', 'state', 'clienttab', 'arctab')
    params = {k: v for k, v in kwargs.items() if k in PARAM_KEYS}

    # convert names of table params to correct REST API
    if 'clienttab' in params:
        params['client'] = params['clienttab']
        del params['clienttab']
    if 'arctab' in params:
        params['arc'] = params['arctab']
        del params['arctab']

    headers = {'Authorization': 'Bearer ' + token}
    try:
        resp = await client.get(url, params=params, headers=headers)
        json = resp.json()
    except httpx.RequestError as e:
        raise ACTClientError(f'Request error: {e}')
    except JSONDecodeError as e:
        raise ACTClientError(f'Response JSON decode error: {e}; Is your aCT URL correct?')
    except Exception as e:
        raise ACTClientError(f'{e}')
    if resp.status_code != 200:
        raise ACTClientError(f'Response error: {json["msg"]}')
    return json


async def filterJobsToDownload(client, url, token, **kwargs):
    # add id to client columns if it is not there already
    if 'clienttab' in kwargs:
        if 'id' not in kwargs['clienttab']:
            if not kwargs['clienttab']:
                kwargs['clienttab'] = 'id'
            else:
                kwargs['clienttab'] += ',id'
        if 'jobname' not in kwargs['clienttab']:  # needed to print result
            kwargs['clienttab'] += ',jobname'
    else:
        kwargs['clienttab'] = 'id,jobname'

    if 'state' in kwargs:
        if kwargs['state'] not in ('done', 'donefailed'):
            raise ACTClientError('State parameter not "done" or "donefailed"')
        # json = await getJobStats(client, url, token, **kwargs)
        # jobids = [job['c_id'] for job in json]
        jobs = await getJobStats(client, url, token, **kwargs)
    else:
        kwargs['state'] = 'done'
        # json = await getJobStats(client, url, token, **kwargs)
        # jobids = set([job['c_id'] for job in json])
        jobs = await getJobStats(client, url, token, **kwargs)

        kwargs['state'] = 'donefailed'
        # json = await getJobStats(client, url, token, **kwargs)
        # jobids = jobids.union([job['c_id'] for job in json])
        jobs.extend(await getJobStats(client, url, token, **kwargs))
    #return jobids
    return jobs


async def downloadJobResults(client, url, token, jobid):
    # download result zip; if it fails don't clean job as user should decide
    url += '/results'
    headers = {'Authorization': 'Bearer ' + token}
    params = {'id': jobid}
    try:
        async with client.stream('GET', url, params=params, headers=headers) as resp:
            if resp.status_code == 204:
                return None
            elif resp.status_code == 200:
                # 'Content-Disposition': 'attachment; filename=ZrcMDm3nK4...m2cmmzn.zip'
                filename = resp.headers['Content-Disposition'].split()[1].split('=')[1]
                await storeResultChunks(resp, filename)
            else:
                json = resp.json()
                raise ACTClientError(f'Response error: {json["msg"]}')
    except httpx.RequestError as e:
        raise ACTClientError(f'Request error: {e}')
    except JSONDecodeError as e:
        raise ACTClientError(f'Response JSON decode error: {e}; Is your aCT URL correct?')
    except Exception as e:
        raise ACTClientError(f'{e}')
    return filename


async def storeResultChunks(resp, filename):
    try:
        async with await trio.open_file(filename, mode='wb') as f:
            async for chunk in resp.aiter_bytes():
                await f.write(chunk)

    except trio.Cancelled:
        try:
            if os.path.isfile(filename):
                os.remove(filename)
        except Exception as e:
            raise ACTClientError(f'{e}')
        raise

    except Exception as e:
        raise ACTClientError(f'{e}')


# possible error conditions:
# - GET /results - must not clean the job (both aCT and webdav)
# - zip extraction failure - must not clean job (both aCT and webdav)
# - zip removal failure - can clean job
#
# Returns path to results directory if results exist.
async def getJob(client, url, token, jobid):
    # download results
    filename = await downloadJobResults(client, url, token, jobid)

    if not filename:
        return ''

    # unzip results
    # extractFailed is needed to exit with error after zip file removal
    # if its extraction failes
    extractFailed = False
    dirname = ''
    if os.path.isfile(filename):
        try:
            dirname = os.path.splitext(filename)[0]
            with zipfile.ZipFile(filename, 'r') as zip_ref:
                zip_ref.extractall(dirname)
        except (zipfile.BadZipFile, zipfile.LargeZipFile) as e:
            msg = f'Result zip extraction error: {e}'
            extractFailed = True

    # delete results archive
    try:
        if os.path.isfile(filename):
            os.remove(filename)
    except Exception as e:
        raise ACTClientError(f'Results zip delete error: {e}')

    if extractFailed:
        shutil.rmtree(dirname, ignore_errors=True)
        raise ACTClientError(msg)

    return dirname


async def deleteProxy(client, url, token):
    url += '/proxies'
    headers = {'Authorization': 'Bearer ' + token}
    try:
        resp = await client.delete(url, headers=headers)
        json = resp.json()
    except httpx.RequestError as e:
        raise ACTClientError(f'Request error: {e}')
    except JSONDecodeError as e:
        raise ACTClientError(f'Response JSON decode error: {e}; Is your aCT URL correct?')
    except Exception as e:
        raise ACTClientError(f'{e}')
    if resp.status_code != 204:
        raise ACTClientError(f'Response error: {json["msg"]}')


async def uploadProxy(client, url, proxyStr, tokenPath):
    # submit proxy cert part to get CSR
    url += '/proxies'
    try:
        resp = await client.post(url, json={'cert': proxyStr})
        json = resp.json()
    except httpx.RequestError as e:
        raise ACTClientError(f'Request error: {e}')
    except JSONDecodeError as e:
        raise ACTClientError(f'Response JSON decode error: {e}; Is your aCT URL correct?')
    except Exception as e:
        raise ACTClientError(f'{e}')
    if resp.status_code != 200:
        raise ACTClientError(f'Response error: {json["msg"]}')
    token = json['token']

    # sign CSR
    try:
        proxyCert, _, issuerChains = parse_issuer_cred(proxyStr)
        csr = x509.load_pem_x509_csr(json['csr'].encode('utf-8'), default_backend())
        cert = sign_request(csr).decode('utf-8')
        chain = proxyCert.public_bytes(serialization.Encoding.PEM).decode('utf-8') + issuerChains + '\n'
    except Exception as e:
        await deleteProxy(client, url, token)
        raise ACTClientError(f'Error generating proxy: {e}')

    # upload signed cert
    json = {'cert': cert, 'chain': chain}
    headers = {'Authorization': 'Bearer ' + token}
    try:
        resp = await client.put(url, json=json, headers=headers)
        json = resp.json()
    except httpx.RequestError as e:
        await deleteProxy(client, url, token)
        raise ACTClientError(f'Request error: {e}')
    except JSONDecodeError as e:
        await deleteProxy(client, url, token)
        raise ACTClientError(f'Response JSON decode error: {e}; Is your aCT URL correct?')
    except Exception as e:
        await deleteProxy(client, url, token)
        raise ACTClientError(f'{e}')
    if resp.status_code != 200:
        await deleteProxy(client, url, token)
        raise ACTClientError(f'Response error: {json["msg"]}')

    # store auth token
    token = json['token']
    try:
        if not os.path.exists(tokenPath):
            os.makedirs(os.path.dirname(tokenPath))
        with open(tokenPath, 'w') as f:
            f.write(token)
        os.chmod(tokenPath, 0o600)
    except Exception as e:
        await deleteProxy(client, url, token)
        raise ACTClientError(f'Error saving token: {e}')


async def submitJobs(client, url, token, descs, clusterlist, webdavClient, webdavUrl):
    results = []  # resulting list of job dicts

    # read job descriptions into a list of job dictionaries
    jobs = []
    for desc in descs:
        job = {'clusterlist': clusterlist, 'descpath': desc, 'cleanup': False}
        try:
            job['desc'] = readFile(desc)
        except ACTClientError as e:
            job['msg'] = str(e)
            results.append(job)
        else:
            jobs.append(job)

    # submit jobs to aCT
    json = [{k: v for k, v in job.items() if k in ('desc', 'clusterlist')} for job in jobs]
    json = await postJobs(client, url, token, json)

    # move jobs with errors to results; do it backwards to not mess up index
    for i in range(len(jobs) - 1, -1, -1):
        if 'msg' in json[i]:
            if 'name' in json[i]:
                jobs[i]['name'] = json[i]['name']
            jobs[i]['msg'] = json[i]['msg']
            results.append(jobs.pop(i))
        else:
            jobs[i]['name'] = json[i]['name']
            jobs[i]['id'] = json[i]['id']

    # The approach to killing is that all jobs from now on should be killed
    # except for those that are submitted successfully and are removed from
    # this list by functions that continue submission.
    for job in jobs:
        job['cleanup'] = True

    # parse job descriptions
    #
    # We first traverse from left to right to populate jobdescs
    # list at the same indexes. We could not do this backwards
    # because this is a SWIG C++ custom data structure and not
    # regular python list.
    jobdescs = arc.JobDescriptionList()
    for job in jobs:
        if not arc.JobDescription_Parse(job['desc'], jobdescs):
            job['msg'] = f'Parsing fail for job description {job["descpath"]}'
    for i in range(len(jobs) - 1, -1, -1):  # remove failed jobs
        if 'msg' in jobs[i]:
            results.append(jobs.pop(i))

    # upload input files
    #
    # A job should be killed unless data upload succeeds. Data upload function
    # should remove jobid from kill list on successful file upload.
    try:
        async with trio.open_nursery() as tasks:
            for i in range(len(jobs)):
                tasks.start_soon(uploadJobData, client, url, token, jobs[i], jobdescs[i], webdavClient, webdavUrl)
    except trio.Cancelled:
        for i in range(len(jobs) - 1, -1, -1):  # add remaining jobs to results
            results.append(jobs.pop(i))
        return results

    # remove jobs that have to be cleaned up, mark other jobs
    # for cleanup for next step
    for i in range(len(jobs) - 1, -1, -1):
        if jobs[i]['cleanup']:
            results.append(jobs.pop(i))
        else:
            jobs[i]['cleanup'] = True

    # complete job submission
    json = [{k: v for k, v in job.items() if k in ('desc', 'id')} for job in jobs]
    try:
        json = await putJobs(client, url, token, json)
    except (trio.Cancelled, ACTClientError) as e:
        for i in range(len(jobs) - 1, -1, -1):
            results.append(jobs.pop(i))
        if isinstance(e, ACTClientError):
            raise JobCleanup(results, e)
        else:
            return results

    # process API errors
    for job, result in zip(jobs, json):
        if 'msg' in result:
            job['msg'] = result['msg']
        else:
            job['cleanup'] = False
        results.append(job)

    return results


async def uploadJobData(client, url, token, job, jobdesc, webdavClient, webdavUrl):
    # create directory for job's local input files if using WebDAV
    if webdavUrl:
        dirUrl = webdavUrl + '/' + str(job['id'])
        try:
            await webdavMkdir(webdavClient, dirUrl)
        except ACTClientError as e:
            job['msg'] = str(e)
            return
        except trio.Cancelled:
            return

    # upload input files
    try:
        errors = await uploadInputFiles(client, url, token, job['id'], jobdesc, webdavClient, webdavUrl)
    except trio.Cancelled:
        return
    if errors:
        job['msg'] = '\n'.join(errors)
        return

    # job description was modified and has to be unparsed
    if webdavUrl:
        xrslStr = jobdesc.UnParse('', '')[1]
        if not xrslStr:
            job['msg'] = 'Error generating job description'
            return
        else:
            job['desc'] = xrslStr

    job['cleanup'] = False


async def uploadInputFiles(client, url, token, jobid, jobdesc, webdavClient, webdavUrl):
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

        if webdavUrl:
            dst = f'{webdavUrl}/{jobid}/{infile.Name}'
            jobdesc.DataStaging.InputFiles[i].Sources[0] = arc.SourceType(dst)
            files[dst] = path
        else:
            files[infile.Name] = path

    if not files:
        return []

    errors = []
    async with trio.open_nursery() as tasks:
        for dst, src in files.items():
            if webdavUrl:  # upload to WebDAV
                tasks.start_soon(errorAdapter, errors, webdavPut, webdavClient, dst, src)
            else:  # upload to internal data management
                tasks.start_soon(errorAdapter, errors, httpPut, client, url, token, dst, src, jobid)
    return errors


async def errorAdapter(errors, asyncfun, *args):
    try:
        await asyncfun(*args)
    except ACTClientError as e:
        errors.append(str(e))
