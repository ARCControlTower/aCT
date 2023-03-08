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

from act_client.common import ACTClientError, Signal, deleteFile, readFile
from act_client.rest import HTTPClient
from act_client.x509proxy import parsePEM, signRequest

# TODO: hardcoded
HTTP_BUFFER_SIZE = 2**23


class ACTRest:

    def __init__(self, url, token=None):
        self.token = token
        self.httpClient = HTTPClient(url)

    def request(self, *args, **kwargs):
        resp = self.httpClient.request(*args, **kwargs)
        return json.loads(resp.read()), resp.status

    def manageJobs(self, method, errmsg, jobids=[], name='', state='', actionParam=None, clienttab=[], arctab=[]):
        params = {}
        if jobids:
            params['id'] = jobids
        if name:
            params['name'] = name
        if state:
            params['state'] = state
        if actionParam:
            params['action'] = actionParam
        if clienttab:
            params['client'] = clienttab
        if arctab:
            params['arc'] = arctab
        jsonData, status = self.request(method, '/jobs', token=self.token, params=params)
        if status != 200:
            raise ACTClientError(f'{errmsg}: {jsonData["msg"]}')
        return jsonData

    def cleanJobs(self, jobids=[], name='', state=''):
        return self.manageJobs(
            'DELETE', 'Error cleaning jobs', jobids, name, state
        )

    def fetchJobs(self, jobids=[], name=''):
        return self.manageJobs(
            'PATCH', 'Error fetching jobs', jobids, name, actionParam='fetch'
        )

    def killJobs(self, jobids=[], name='', state=''):
        return self.manageJobs(
            'PATCH', 'Error killing jobs', jobids, name, state, actionParam='cancel'
        )

    def resubmitJobs(self, jobids=[], name=''):
        return self.manageJobs(
            'PATCH', 'Error resubmitting jobs', jobids, name, actionParam='resubmit'
        )

    def uploadFile(self, jobid, name, path):
        try:
            f = open(path, 'rb')
        except Exception as e:
            raise ACTClientError(f'Error opening file {path}: {e}')

        params = {'id': jobid, 'filename': name}
        resp = self.httpClient.request('PUT', '/data', token=self.token, data=f, params=params)
        text = resp.read()
        if resp.status != 204:
            jsonData = json.loads(text)
            raise ACTClientError(f"Error uploading file {path}: {jsonData['msg']}")

    def getJobStats(self, jobids=[], name='', state='', clienttab=[], arctab=[]):
        return self.manageJobs(
            'GET', 'Error getting job status', jobids, name, state, clienttab=clienttab, arctab=arctab
        )

    def getDownloadableJobs(self, jobids=[], name='', state=''):
        clienttab = ['id', 'jobname']
        if state:
            if state not in ('done', 'donefailed'):
                raise ACTClientError('State parameter not "done" or "donefailed"')
            jobs = self.getJobStats(jobids=jobids, name=name, state=state, clienttab=clienttab)
        else:
            jobs = self.getJobStats(jobids=jobids, name=name, state='done', clienttab=clienttab)
            jobs.extend(self.getJobStats(jobids=jobids, name=name, state='donefailed', clienttab=clienttab))
        return jobs

    # Returns path to results directory if results exist.
    def downloadJobResults(self, jobid):
        filename = ''
        query = urlencode({'id': jobid})
        url = f'/results?{query}'
        try:
            resp = self.httpClient.request('GET', url, token=self.token)
            if resp.status == 204:
                resp.read()
            elif resp.status == 200:
                # 'Content-Disposition': 'attachment; filename=ZrcMD...cmmzn.zip'
                #filename = resp.getheader('Content-Disposition').split()[1].split('=')[1]
                filename = resp.getheader('Content-Disposition').split('=')[1]
                _storeResultChunks(resp, filename)
            else:
                jsonData = json.loads(resp.read().decode())
                raise ACTClientError(f'Response error: {jsonData["msg"]}')
        except ACTClientError:
            raise
        except Exception as e:
            raise ACTClientError(f'Error downloading results: {e}')
        # Cleanup code required here in case keyboard interrupt happens somewhere
        # between the creation of result file and propagation of filename to the
        # function getJob that performs cleanup as well.
        except KeyboardInterrupt:
            deleteFile(filename)
            raise

        if not filename:
            return ''

        dirname = ''
        extractFailed = False
        try:
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

    def deleteProxy(self):
        resp = self.httpClient.request('DELETE', '/proxies', token=self.token)
        if resp.status != 204:
            jsonData = json.loads(resp.read().decode())
            raise ACTClientError(f'Error deleting proxy: {jsonData["msg"]}')

    def uploadProxy(self, proxyStr, tokenPath):
        # submit proxy cert part to get CSR
        cert, _, chain = parsePEM(proxyStr)
        jsonData = {'cert': cert.public_bytes(serialization.Encoding.PEM).decode('utf-8'), 'chain': chain}
        jsonData, status = self.request('POST', '/proxies', jsonData=jsonData)
        ## submit proxy cert part to get CSR
        #jsonData, status = self.request('POST', '/proxies', jsonData={'cert': proxyStr})
        if status != 200:
            raise ACTClientError(jsonData['msg'])  # message is attached by API user
        token = jsonData['token']
        self.token = token

        # sign CSR
        try:
            proxyCert, _, issuerChains = parsePEM(proxyStr)
            csr = x509.load_pem_x509_csr(jsonData['csr'].encode(), default_backend())
            cert = signRequest(csr).decode()
            chain = proxyCert.public_bytes(serialization.Encoding.PEM).decode() + issuerChains + '\n'
        except Exception:
            self.deleteProxy()
            raise

        # upload signed cert
        jsonData = {'cert': cert, 'chain': chain}
        try:
            jsonData, status = self.request('PUT', '/proxies', jsonData=jsonData, token=self.token)
        except Exception:
            self.deleteProxy()
            raise

        if status != 200:
            raise ACTClientError(jsonData["msg"])  # message is attached by API user

        # store auth token
        token = jsonData['token']
        self.token = token
        try:
            os.makedirs(os.path.dirname(tokenPath), exist_ok=True)
            with open(tokenPath, 'w') as f:
                f.write(token)
            os.chmod(tokenPath, 0o600)
        except Exception:
            self.deleteProxy()
            raise

    # SIGINT is disabled to ensure uninterrupted execution where necessary.
    # Reverse iterations are done to allow deletion of elements from the list
    # without messing up iteration.
    def submitJobs(self, descs, clusterlist, webdavClient, webdavBase):
        sigint = Signal(signal.SIGINT, callback=lambda: print("\nCancelling submission ..."))
        sigint.defer()

        # read job descriptions into a list of job dictionaries and JSON for
        # aCT REST
        results = []  # resulting list of job dicts
        jobs = []  # a list of jobs being worked on (failed jobs get removed)
        jsonData = []
        for desc in descs:
            job = {'clusterlist': clusterlist, 'descpath': desc, 'cleanup': False}
            try:
                job['descstr'] = readFile(desc)
            except ACTClientError as e:
                job['msg'] = str(e)
            else:
                results.append(job)
                jobs.append(job)
                jsonData.append({'clusterlist': job['clusterlist']})

        # submit jobs to aCT
        jsonData, status = self.request('POST', '/jobs', token=self.token, jsonData=jsonData)
        if status != 200:
            raise ACTClientError(f'Error creating jobs: {jsonData["msg"]}')

        # Parse job descriptions of jobs without errors. Jobs with submission
        # or parsing errors are removed from working set.
        jobdescs = arc.JobDescriptionList()
        for i in range(len(jobs) - 1, -1, -1):
            if 'msg' in jsonData[i]:
                jobs[i]['msg'] = jsonData[i]['msg']
                jobs.pop(i)
                continue

            jobs[i]['id'] = jsonData[i]['id']

            # All jobs that were successfully POSTed need to be killed
            # unless the submission succeeds
            jobs[i]['cleanup'] = True

            # parse job description
            if not arc.JobDescription_Parse(jobs[i]['descstr'], jobdescs):
                jobs[i]['msg'] = f'Parsing fail for job description {jobs[i]["descpath"]}'
                jobs.pop(i)
            else:
                jobs[i]['desc'] = jobdescs[-1]

        # upload input files
        try:
            sigint.restore()
            for job in jobs:
                self.uploadJobData(job, webdavClient, webdavBase)
        except KeyboardInterrupt:
            return results
        else:
            sigint.defer()

        # Unparse modified job descriptions and prepare JSON. Jobs with upload
        # or unaprse errors are removed from the working set.
        jsonData = []
        for i in range(len(jobs) - 1, -1, -1):
            if 'msg' in jobs[i]:
                jobs.pop(i)
                continue

            jobs[i]['descstr'] = jobs[i]['desc'].UnParse('emies:adl')[1]
            if not jobs[i]['descstr']:
                jobs[i]['msg'] = 'Error generating job description'
                jobs.pop(i)
            else:
                # insert to beginning because of reverse iteration to preserve
                # the order of jobs processed by REST
                jsonData.insert(0, {
                    'id': jobs[i]['id'],
                    'desc': jobs[i]['descstr']
                })

        # complete job submission
        error = None
        if jsonData:
            try:
                jsonData, status = self.request('PUT', '/jobs', token=self.token, jsonData=jsonData)
            except ACTClientError as e:
                error = str(e)
            if status != 200:
                error = jsonData['msg']
            if error:
                for job in jobs:
                    job['msg'] = error
        else:
            error = True

        # process API errors
        if not error:
            for job, result in zip(jobs, jsonData):
                if 'name' in result:
                    job['name'] = result['name']
                if 'msg' in result:
                    job['msg'] = result['msg']
                else:
                    job['cleanup'] = False

        try:
            return results
        finally:
            sigint.restore()

    def uploadJobData(self, job, webdavClient, webdavBase):
        # create a dictionary of files to upload
        files = {}
        #for infile in job['desc'].DataStaging.InputFiles:
        for i in range(len(job['desc'].DataStaging.InputFiles)):
            infile = job['desc'].DataStaging.InputFiles[i]
            path = infile.Sources[0].fullstr()
            if not path:
                path = infile.Name

            # parse as URL, remote resource if scheme or hostname
            try:
                url = urlparse(path)
            except ValueError as e:
                job['msg'] = f'Error parsing source of file {infile.Name}: {e}'
                return

            # skip non local files
            if url.scheme not in ('file', None, '') or url.hostname:
                continue

            # check if local file exists
            path = url.path
            if not os.path.isfile(path):
                job['msg'] = f'Given path {path} is not a file'
                return

            # modify job description if using WebDAV
            if webdavBase:
                url = f'{webdavBase}/{job["id"]}/{infile.Name}'
                infile.Sources[0] = arc.SourceType(url)

            files[infile.Name] = path

        # create job directory in WebDAV storage
        if webdavBase:
            try:
                webdavClient.mkdir(f'{webdavBase}/{job["id"]}')
            except Exception as e:
                job['msg'] = str(e)
                return

        # upload input files
        for dst, src in files.items():
            try:
                if webdavBase:
                    webdavClient.uploadFile(f'{webdavBase}/{job["id"]}/{dst}', src)
                else:
                    self.uploadFile(job['id'], dst, src)
            except Exception as e:
                job['msg'] = f'Error uploading {src} to {dst}: {e}'
                return

    def getInfo(self):
        return self.request('GET', '/info', token=self.token)

    def close(self):
        self.httpClient.close()


class WebDAVClient:

    def __init__(self, url, proxypath=None):
        self.httpClient = HTTPClient(url, proxypath=proxypath)

    def rmdir(self, url):
        headers = {'Accept': '*/*', 'Connection': 'Keep-Alive'}
        resp = self.httpClient.request('DELETE', url, headers=headers)
        text = resp.read()

        # TODO: should we rely on 204 and 404 being the only right answers?
        if resp.status == 404:  # ignore, because we are just trying to delete
            return
        if resp.status >= 300:
            raise ACTClientError('Unexpected response for removal of WebDAV directory: {text}')

    def mkdir(self, url):
        headers = {'Accept': '*/*', 'Connection': 'Keep-Alive'}
        resp = self.httpClient.request('MKCOL', url, headers=headers)
        text = resp.read()

        if resp.status != 201:
            raise ACTClientError(f'Error creating WebDAV directory {url}: {text}')

    def uploadFile(self, url, path):
        try:
            f = open(path, 'rb')
        except Exception as e:
            raise ACTClientError(f'Error opening file {path}: {e}')

        with f:
            resp = self.httpClient.request('PUT', url, headers={'Expect': '100-continue'})
            resp.read()
            if resp.status == 307:
                dstUrl = resp.getheader('Location')
                parts = urlparse(dstUrl)
                urlPath = f'{parts.path}?{parts.query}'
                nodeClient = HTTPClient(dstUrl)
                try:
                    # if headers are not explicitly set to empty they will
                    # somehow be taken from previous separate connection
                    # contexts?
                    resp = nodeClient.request('PUT', urlPath, data=f, headers={})
                    text = resp.read()
                    status = resp.status
                except http.client.HTTPException as e:
                    raise ACTClientError(f'Error redirecting WebDAV upload for file {path}: {e}')
                finally:
                    nodeClient.close()
            else:
                resp = self.httpClient.request('PUT', url, data=f)
                text = resp.read()
                status = resp.status

        if status != 201:
            raise ACTClientError(f'Error uploading file {path}: {text}')

    def cleanJobDirs(self, url, jobids):
        errors = []
        for jobid in jobids:
            dirUrl = f'{url}/{jobid}'
            try:
                self.rmdir(dirUrl)
            except Exception as e:
                errors.append(str(e))
        return errors

    def close(self):
        self.httpClient.close()


def _storeResultChunks(resp, filename, chunksize=HTTP_BUFFER_SIZE):
    try:
        with open(filename, 'wb') as f:
            chunk = resp.read(chunksize)
            while chunk:
                f.write(chunk)
                chunk = resp.read(chunksize)
    except Exception as e:
        raise ACTClientError(f'Error storing job results to the file {filename}: {e}')


def getACTRestClient(conf, useToken=True):
    try:
        if useToken:
            token = readFile(conf['token'])
        else:
            token = None
        actrest = ACTRest(conf['server'], token=token)
    except Exception as exc:
        raise ACTClientError(f'Error creating aCT REST client: {exc}')
    return actrest


def getWebDAVClient(conf, webdavBase, useProxy=True):
    try:
        if useProxy:
            proxypath = conf['proxy']
        else:
            proxypath = None
        webdavClient = WebDAVClient(webdavBase, proxypath=proxypath)
    except Exception as exc:
        raise ACTClientError(f'Error creating WebDAV client: {exc}')
    return webdavClient
