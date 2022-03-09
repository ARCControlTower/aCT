import http.client
import json
import os
import shutil
import signal
import zipfile
from urllib.parse import urlencode, urlparse

import arc
from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

from act_client.common import (HTTP_BUFFER_SIZE, ACTClientError, SignalIgnorer,
                               deleteFile, getHTTPConn, readFile)
from act_client.delegate_proxy import parse_issuer_cred
from act_client.x509proxy import sign_request

# TODO: use proper data structures for API rather than format expected
#       on backend; also use kwargs
# TODO: unify API: PATCH tocancel returns job dicts, others return list
#       of job IDs
# TODO: properly set up limits and timeouts for httpx library and handle
#       timeout errors


# TODO: Since http.client requires that every response is actually read, does
# that require granular exception handling to call read() in every appropriate
# place? Check it out.


def httpRequest(conn, method, endpoint, **kwargs):
    headers = kwargs.get('headers', {})

    token = kwargs.get('token', None)
    if token:
        headers['Authorization'] = f'Bearer {token}'

    jsonDict = kwargs.get('json', None)
    if jsonDict:
        body = json.dumps(jsonDict).encode()
        headers['Content-type'] = 'application/json'
    else:
        body = kwargs.get('body', None)

    params = kwargs.get('params', {})
    for key, value in params.items():
        if isinstance(value, list):
            params[key] = ','.join([str(val) for val in value])

    query = ''
    if params:
        query = urlencode(params)

    if query:
        url = f'{endpoint}?{query}'
    else:
        url = endpoint

    try:
        conn.request(method, url, body=body, headers=headers)
    except http.client.HTTPException as e:
        raise ACTClientError(f'Request error: {e}')

    try:
        resp = conn.getresponse()
    except http.client.RemoteDisconnected:
        conn._connect()
        conn.request(method, url, body=body, headers=headers)
        resp = conn.getresponse()
    except http.client.HTTPException as e:
        raise ACTClientError(f'Request error: {e}')

    return resp


def aCTJSONRequest(*args, **kwargs):
    resp = httpRequest(*args, **kwargs)
    try:
        jsonDict = loadJSON(resp.read().decode())
    except http.client.HTTPException as e:
        raise ACTClientError(f'Error reading response: {e}')
    if resp.status != 200:
        raise ACTClientError(f'Response error: {jsonDict["msg"]}')
    return jsonDict


def loadJSON(jsonStr):
    try:
        jsonDict = json.loads(jsonStr)
    except json.decoder.JSONDecodeError as e:
        raise ACTClientError(f'Response JSON decode error: {e}')
    return jsonDict


def cleanJobs(conn, token, params):
    return aCTJSONRequest(conn, 'DELETE', '/jobs', token=token, params=params)


def patchJobs(conn, token, params, arcstate):
    if arcstate not in ('tofetch', 'tocancel', 'toresubmit'):
        raise ACTClientError(f'Invalid arcstate argument "{arcstate}"')
    jsonDict = {'arcstate': arcstate}
    return aCTJSONRequest(conn, 'PATCH', '/jobs', token=token, params=params, json=jsonDict)


def fetchJobs(*args):
    return patchJobs(*args, 'tofetch')


def killJobs(*args):
    return patchJobs(*args, 'tocancel')


def resubmitJobs(*args):
    return patchJobs(*args, 'toresubmit')


def postJobs(conn, token, jobs):
    return aCTJSONRequest(conn, 'POST', '/jobs', token=token, json=jobs)


def putJobs(conn, token, jobs):
    return aCTJSONRequest(conn, 'PUT', '/jobs', token=token, json=jobs)


def httpPut(conn, token, name, path, jobid):
    try:
        f = open(path, 'rb')
    except Exception as e:
        raise ACTClientError(f'Error opening file {path}: {e}')

    params = {'id': jobid, 'filename': name}
    return aCTJSONRequest(conn, 'PUT', '/data', token=token, body=f, params=params)


def getJobStats(conn, token, **kwargs):
    PARAM_KEYS = ('id', 'name', 'state', 'clienttab', 'arctab')
    params = {}
    for k, v in kwargs.items():
        if k not in PARAM_KEYS:
            raise ACTClientError(f'Invalid parameter for stat operation: {k}')
        else:
            params[k] = v

    # convert names of table params to correct REST API and
    # convert lists to comma separated string of values
    if 'clienttab' in params:
        params['client'] = params['clienttab']
        del params['clienttab']
    if 'arctab' in params:
        params['arc'] = params['arctab']
        del params['arctab']

    return aCTJSONRequest(conn, 'GET', '/jobs', token=token, params=params)


def webdavRmdir(conn, url):
    headers = {'Accept': '*/*', 'Connection': 'Keep-Alive'}
    resp = httpRequest(conn, 'DELETE', url, headers=headers)
    text = resp.read()

    # TODO: should we rely on 204 and 404 being the only right answers?
    if resp.status == 404:  # ignore, because we are just trying to delete
        return
    if resp.status >= 300:
        raise ACTClientError('Unexpected response for removal of WebDAV directory: {text}')


def webdavMkdir(conn, url):
    headers = {'Accept': '*/*', 'Connection': 'Keep-Alive'}
    resp = httpRequest(conn, 'MKCOL', url, headers=headers)
    text = resp.read()

    if resp.status != 201:
        raise ACTClientError(f'Error creating WebDAV directory {url}: {text}')


# Optimal upload to dCache requires Expect 100-continue redirect that is first
# attempted. If it doesn't succeed the file is uploaded normaly through central
# dCache server or to regular WebDAV server.
def webdavPut(conn, url, path):
    try:
        f = open(path, 'rb')
    except Exception as e:
        raise ACTClientError(f'Error opening file {path}: {e}')

    with f:
        resp = httpRequest(conn, 'PUT', url, headers={'Expect': '100-continue'})
        resp.read()
        if resp.status == 307:
            dstUrl = resp.getheader('Location')
            parts = urlparse(dstUrl)
            urlPath = f'{parts.path}?{parts.query}'
            upconn = getHTTPConn(dstUrl)
            try:
                resp = httpRequest(upconn, 'PUT', urlPath, body=f)
                text = resp.read()
                status = resp.status
            except http.client.HTTPException as e:
                raise ACTClientError(f'Error redirecting WebDAV upload for file {path}: {e}')
            finally:
                upconn.close()
        else:
            resp = httpRequest(conn, 'PUT', url, body=f)
            text = resp.read()
            status = resp.status

    if status != 201:
        raise ACTClientError(f'Error uploading file {path}: {text}')


def cleanWebDAV(conn, url, jobids):
    errors = []
    for jobid in jobids:
        dirUrl = f'{url}/{jobid}'
        try:
            webdavRmdir(conn, dirUrl)
        except Exception as e:
            errors.append(str(e))
    return errors


def filterJobsToDownload(*args, **kwargs):
    # specify job columns that have to be fetched
    kwargs['clienttab'] = ['id', 'jobname']

    if 'state' in kwargs:
        if kwargs['state'] not in ('done', 'donefailed'):
            raise ACTClientError('State parameter not "done" or "donefailed"')
        jobs = getJobStats(*args, **kwargs)
    else:
        kwargs['state'] = 'done'
        jobs = getJobStats(*args, **kwargs)

        kwargs['state'] = 'donefailed'
        jobs.extend(getJobStats(*args, **kwargs))
    return jobs


def downloadJobResults(conn, token, jobid):
    filename = ''
    try:
        query = urlencode({'id': jobid})
        url = f'/results?{query}'

        resp = httpRequest(conn, 'GET', url, token=token)
        if resp.status == 204:
            resp.read()
            return ''
        elif resp.status == 200:
            # 'Content-Disposition': 'attachment; filename=ZrcMD...cmmzn.zip'
            filename = resp.getheader('Content-Disposition').split()[1].split('=')[1]
            storeResultChunks(resp, filename)
        else:
            jsonDict = loadJSON(resp.read().decode())
            raise ACTClientError(f'Response error: {jsonDict["msg"]}')

    except (http.client.HTTPException, Exception) as e:
        raise ACTClientError(f'Error downloading results: {e}')

    # Cleanup code required here in case keyboard interrupt happens somewhere
    # between the creation of result file and propagation of filename to the
    # function getJob that performs cleanup as well.
    except KeyboardInterrupt:
        deleteFile(filename)
        raise

    return filename


def storeResultChunks(resp, filename):
    try:
        with open(filename, 'wb') as f:
            chunk = resp.read(HTTP_BUFFER_SIZE)
            while chunk:
                f.write(chunk)
                chunk = resp.read(HTTP_BUFFER_SIZE)
    except Exception as e:
        raise ACTClientError(f'Error storing job results to the file {filename}: {e}')


# Returns path to results directory if results exist.
def getJob(*args):
    filename = ''
    dirname = ''
    extractFailed = False
    try:
        # download results
        filename = downloadJobResults(*args)

        if not filename:
            return ''

        # Unzip results. extractFailed is needed to exit with error after zip
        # file removal if extraction failes.
        extractFailed = False
        if os.path.isfile(filename):
            try:
                dirname = os.path.splitext(filename)[0]
                with zipfile.ZipFile(filename, 'r') as zip_ref:
                    zip_ref.extractall(dirname)
            except (zipfile.BadZipFile, zipfile.LargeZipFile) as e:
                msg = f'Could not extract results zip: {e}'
                extractFailed = True
        else:
            raise ACTClientError(f'Path {filename} is not a file')

    finally:
        deleteFile(filename)

        # exit with error on extraction failure
        if extractFailed:
            shutil.rmtree(dirname, ignore_errors=True)
            raise ACTClientError(msg)

    return dirname


def deleteProxy(conn, token):
    resp = httpRequest(conn, 'DELETE', '/proxies', token=token)
    try:
        jsonDict = loadJSON(resp.read().decode())
    except http.client.HTTPException as e:
        raise ACTClientError(f'Proxy delete request error: {e}')
    if resp.status != 204:
        raise ACTClientError(f'Response error: {jsonDict["msg"]}')


def uploadProxy(conn, proxyStr, tokenPath):
    # submit proxy cert part to get CSR
    jsonDict = aCTJSONRequest(conn, 'POST', '/proxies', json={'cert':proxyStr})
    token = jsonDict['token']

    # sign CSR
    try:
        proxyCert, _, issuerChains = parse_issuer_cred(proxyStr)
        csr = x509.load_pem_x509_csr(jsonDict['csr'].encode('utf-8'), default_backend())
        cert = sign_request(csr).decode('utf-8')
        chain = proxyCert.public_bytes(serialization.Encoding.PEM).decode('utf-8') + issuerChains + '\n'
    except Exception as e:
        deleteProxy(conn, token)
        raise ACTClientError(f'Error generating proxy: {e}')

    # upload signed cert
    jsonDict = {'cert': cert, 'chain': chain}
    try:
        jsonDict = aCTJSONRequest(conn, 'PUT', '/proxies', json=jsonDict, token=token)
    except Exception:
        deleteProxy(conn, token)
        raise

    # store auth token
    token = jsonDict['token']
    try:
        if not os.path.exists(tokenPath):
            os.makedirs(os.path.dirname(tokenPath))
        with open(tokenPath, 'w') as f:
            f.write(token)
        os.chmod(tokenPath, 0o600)
    except Exception as e:
        deleteProxy(conn, token)
        raise ACTClientError(f'Error saving token: {e}')


# SIGINT is disabled to ensure uninterrupted execution where necessary
def submitJobs(conn, token, descs, clusterlist, webdavConn, webdavUrl):
    sigint = SignalIgnorer(signal.SIGINT)

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
    jsonDict = [{k: v for k, v in job.items() if k in ('desc', 'clusterlist')} for job in jobs]
    jsonDict = postJobs(conn, token, jsonDict)

    # move jobs with errors to results; do it backwards to not mess up index
    for i in range(len(jobs) - 1, -1, -1):
        if 'msg' in jsonDict[i]:
            if 'name' in jsonDict[i]:
                jobs[i]['name'] = jsonDict[i]['name']
            jobs[i]['msg'] = jsonDict[i]['msg']
            results.append(jobs.pop(i))
        else:
            jobs[i]['name'] = jsonDict[i]['name']
            jobs[i]['id'] = jsonDict[i]['id']

    # The approach to killing is that all jobs from now on should be killed
    # except for those that are submitted successfully and marked otherwise.
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

    # remove jobs with errors
    for i in range(len(jobs) - 1, -1, -1):
        if 'msg' in jobs[i]:
            results.append(jobs.pop(i))

    # upload input files
    try:
        sigint.restore()
        for i in range(len(jobs)):
            uploadJobData(conn, token, jobs[i], jobdescs[i], webdavConn, webdavUrl)
    except KeyboardInterrupt:
        results.extend(jobs)
        return results
    else:
        sigint.ignore()

    # remove jobs with errors
    for i in range(len(jobs) - 1, -1, -1):
        if 'msg' in jobs[i]:
            results.append(jobs.pop(i))

    # complete job submission
    try:
        jsonDict = [{k: v for k, v in job.items() if k in ('desc', 'id')} for job in jobs]
        jsonDict = putJobs(conn, token, jsonDict)
    except ACTClientError as e:
        for job in jobs:
            job['msg'] = str(e)
        results.extend(jobs)
        return results

    # process API errors
    for job, result in zip(jobs, jsonDict):
        if 'msg' in result:
            job['msg'] = result['msg']
        else:
            job['cleanup'] = False
        results.append(job)

    return results


def uploadJobData(conn, token, job, jobdesc, webdavConn, webdavUrl):
    # create directory for job's local input files if using WebDAV
    if webdavUrl:
        dirUrl = f'{webdavUrl}/{job["id"]}'
        try:
            webdavMkdir(webdavConn, dirUrl)
        except ACTClientError as e:
            job['msg'] = str(e)
            return

    # upload input files
    errors = uploadInputFiles(conn, token, job['id'], jobdesc, webdavConn, webdavUrl)
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


def uploadInputFiles(conn, token, jobid, jobdesc, webdavConn, webdavUrl):
    files = {}

    exepath = jobdesc.Application.Executable.Path
    if not os.path.isabs(exepath):
        files[os.path.basename(exepath)] = exepath

    # if a file with the same name as executable is provided in inputFiles
    # in xRSL the value from inputFiles will be used (or file entry discarded
    # if the executable file is remote)
    errors = []
    for i in range(len(jobdesc.DataStaging.InputFiles)):
        # we use index for access to InputFiles because changes
        # (for dcache) are not preserved otherwise?
        infile = jobdesc.DataStaging.InputFiles[i]

        if exepath == infile.Name and exepath in files:
            del files[exepath]

        path = infile.Sources[0].fullstr()
        if not path:
            path = infile.Name

        # parse as URL, remote resource if scheme or hostname
        url = urlparse(path)
        if url.scheme not in ('file', None, '') or url.hostname:
            continue

        path = url.path
        if not os.path.isfile(path):
            errors.append(f'Given path {path} is not a file')
            continue

        if webdavUrl:
            dst = f'{webdavUrl}/{jobid}/{infile.Name}'
            jobdesc.DataStaging.InputFiles[i].Sources[0] = arc.SourceType(dst)
            files[dst] = path
        else:
            files[infile.Name] = path

    if errors:
        return errors

    if not files:
        return []

    for dst, src in files.items():
        try:
            if webdavUrl:  # upload to WebDAV
                webdavPut(webdavConn, dst, src)
            else:  # upload to internal data management
                httpPut(conn, token, dst, src, jobid)
        except (Exception, ACTClientError) as e:
            errors.append(str(e))
    return errors
