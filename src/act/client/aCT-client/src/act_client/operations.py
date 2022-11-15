import http.client
import json
import logging
import os
import shutil
import signal
import sys
import zipfile
from urllib.parse import urlencode, urlparse

from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

from act_client.common import ACTClientError, Signal, deleteFile
from act_client.httpclient import HTTP_BUFFER_SIZE, HTTPClient
from act_client.x509proxy import parsePEM, signRequest
from act_client.xrsl import XRSLParser


class ACTRest:

    def __init__(self, url, token=None, logger=None):
        self.logger = logger
        if self.logger is None:
            self.logger = getNullLogger()

        self.token = token
        self.httpClient = HTTPClient(url, logger=self.logger)

    def request(self, *args, **kwargs):
        resp = self.httpClient.request(*args, **kwargs)
        data = resp.read().decode()
        try:
            return json.loads(data), resp.status
        except json.JSONDecodeError:
            raise ACTClientError('Error decoding JSON: aCT REST might not be running')

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
        self.logger.debug(f"Job manage response - {status} {jsonData}")
        if status != 200:
            raise ACTClientError(f'{errmsg}: {jsonData["msg"]}')
        return jsonData

    def manageJobBatch(self, *args, batchSize=100, jobids=[], **kwargs):
        if not jobids:
            return self.manageJobs(*args, jobids=jobids, **kwargs)
        results = []
        ix = 0
        while ix < len(jobids):
            results.extend(
                self.manageJobs(*args, jobids=jobids[ix:ix+batchSize], **kwargs)
            )
            ix += batchSize
        return results

    def cleanJobs(self, jobids=[], name='', state=''):
        return self.manageJobBatch(
            'DELETE', 'Error cleaning jobs', jobids=jobids, name=name, state=state
        )

    def fetchJobs(self, jobids=[], name=''):
        return self.manageJobBatch(
            'PATCH', 'Error fetching jobs', jobids=jobids, name=name, actionParam='fetch'
        )

    def killJobs(self, jobids=[], name='', state=''):
        return self.manageJobBatch(
            'PATCH', 'Error killing jobs', jobids=jobids, name=name, state=state, actionParam='cancel'
        )

    def resubmitJobs(self, jobids=[], name=''):
        return self.manageJobBatch(
            'PATCH', 'Error resubmitting jobs', jobids=jobids, name=name, actionParam='resubmit'
        )

    def getJobStats(self, jobids=[], name='', state='', clienttab=[], arctab=[]):
        return self.manageJobBatch(
            'GET', 'Error getting job status', jobids=jobids, name=name, state=state, clienttab=clienttab, arctab=arctab
        )

    def uploadFile(self, jobid, name, path):
        try:
            f = open(path, 'rb')
        except Exception as e:
            raise ACTClientError(f'Error opening file {path}: {e}')

        resp = self.httpClient.request('PUT', f'/jobs/{jobid}/data/{name}', token=self.token, data=f)
        text = resp.read().decode()
        self.logger.debug(f"Upload of file {name} from path {path} for job {jobid} - {resp.status} {text}")
        if resp.status != 204:
            jsonData = json.loads(text)
            raise ACTClientError(f"Error uploading file {path}: {jsonData['msg']}")

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
    def downloadJobResults(self, jobid, dirname=None):
        filename = ''
        query = urlencode({'id': jobid})
        url = f'/results?{query}'
        try:
            resp = self.httpClient.request('GET', url, token=self.token)
            if resp.status == 204:
                text = resp.read().decode()
                self.logger.debug(f"No results for job {jobid} - {resp.status} {text}")
            elif resp.status == 200:
                # 'Content-Disposition': 'attachment; filename=ZrcMD...cmmzn.zip'
                #filename = resp.getheader('Content-Disposition').split()[1].split('=')[1]
                filename = resp.getheader('Content-Disposition').split('=')[1]
                self.logger.debug(f"Downloading result file {filename} for job {jobid} - {resp.status}")
                _storeResultChunks(resp, filename)
            else:
                text = resp.read().decode()
                self.logger.debug(f"Error downloading results for job {jobid} - {resp.status} {text}")
                jsonData = json.loads(text)
                raise ACTClientError(f'Response error: {jsonData["msg"]}')
        except ACTClientError:
            raise
        except Exception as exc:
            self.logger.debug(f"Error downloading results for job {jobid}: {exc}")
            raise ACTClientError(f"Error downloading results for job {jobid}: {exc}")
        # Cleanup code required here in case keyboard interrupt happens somewhere
        # between the creation of result file and propagation of filename to the
        # function getJob that performs cleanup as well.
        except KeyboardInterrupt:
            deleteFile(filename)
            raise

        if not filename:
            return ''

        extractFailed = False
        msg = ''
        try:
            # Unzip results. extractFailed is needed to exit with error after zip
            # file removal if extraction failes.
            extractFailed = False
            if os.path.isfile(filename):
                try:
                    extractDir = dirname
                    if not extractDir:
                        extractDir = os.path.splitext(filename)[0]
                    if os.path.isdir(extractDir):
                        dirnum = 1
                        while os.path.isdir(f'{extractDir}_{dirnum}'):
                            dirnum += 1
                            if dirnum > sys.maxsize:
                                self.logger.debug("Extraction directory already exists")
                                raise ACTClientError('Extraction directory already exists')
                        extractDir = f'{extractDir}_{dirnum}'
                    with zipfile.ZipFile(filename, 'r') as zip_ref:
                        zip_ref.extractall(extractDir)
                    self.logger.debug(f"Extracted result file {filename} to {extractDir}")
                except (zipfile.BadZipFile, zipfile.LargeZipFile) as exc:
                    msg = f'Could not extract result file {filename} to {extractDir}: {exc}'
                    self.logger.debug(msg)
                    extractFailed = True
            else:
                raise ACTClientError(f'Path {filename} is not a file')

        finally:
            deleteFile(filename)

            # exit with error on extraction failure
            if extractFailed:
                shutil.rmtree(extractDir, ignore_errors=True)
                raise ACTClientError(msg)

        return extractDir

    def deleteProxy(self):
        resp = self.httpClient.request('DELETE', '/proxies', token=self.token)
        text = resp.read().decode()
        self.logger.debug(f"Proxy delete operation - {resp.status} {text}")
        if resp.status != 204:
            jsonData = json.loads(text)
            raise ACTClientError(f'Error deleting proxy: {jsonData["msg"]}')

    def uploadProxy(self, proxyStr, tokenPath):
        # submit proxy cert part to get CSR
        cert, _, chain = parsePEM(proxyStr)
        jsonData = {'cert': cert.public_bytes(serialization.Encoding.PEM).decode('utf-8'), 'chain': chain}
        jsonData, status = self.request('POST', '/proxies', jsonData=jsonData)
        self.logger.debug(f"Proxy POST response - {status} {jsonData}")
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
        except Exception as exc:
            self.logger.debug(f"Error signing CSR: {exc}")
            self.deleteProxy()
            raise

        # upload signed cert
        jsonData = {'cert': cert, 'chain': chain}
        try:
            jsonData, status = self.request('PUT', '/proxies', jsonData=jsonData, token=self.token)
        except Exception as exc:
            self.logger.debug(f"Proxy PUT error: {exc}")
            self.deleteProxy()
            raise
        self.logger.debug(f"Proxy PUT response: {status} {jsonData}")
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
        except Exception as exc:
            self.logger.debug(f"Error saving token to {tokenPath}: {exc}")
            self.deleteProxy()
            raise

    # SIGINT is disabled to ensure uninterrupted execution where necessary.
    # Reverse iterations are done to allow deletion of elements from the list
    # without messing up iteration.
    def submitJobBatch(self, descs, clusterlist, webdavClient, webdavBase):
        # Create a list of results, a list of jobs to be worked on and a JSON
        # structure for POST to REST API.
        sigint = parser = None
        try:
            sigint = Signal(signal.SIGINT, callback=lambda: print("\nCancelling submission ..."))
            parser = XRSLParser()
            results, jobs, jsonData = _prepareJobs(descs, clusterlist, parser)
        except KeyboardInterrupt:
            raise SubmissionInterrupt()
        else:
            sigint.defer()

        # submit jobs to aCT
        jsonData, status = self.request('POST', '/jobs', token=self.token, jsonData=jsonData)
        self.logger.debug(f"Jobs POST response - {status} {jsonData}")
        if status != 200:
            raise ACTClientError(f'Error creating jobs: {jsonData["msg"]}')

        # Parse job descriptions of jobs without errors. Jobs with submission
        # errors are removed from the working set.
        for i in range(len(jobs) - 1, -1, -1):
            if 'msg' in jsonData[i]:
                jobs[i]['msg'] = jsonData[i]['msg']
                jobs.pop(i)
                continue

            jobs[i]['id'] = jsonData[i]['id']

            # All jobs that were successfully POSTed need to be killed
            # unless the submission succeeds
            jobs[i]['cleanup'] = True

        # upload input files
        try:
            sigint.restore()
            for job in jobs:
                self.uploadJobData(job, webdavClient, webdavBase)
        except KeyboardInterrupt:
            raise SubmissionInterrupt(results)
        else:
            sigint.defer()

        # Unparse modified job descriptions and prepare JSON. Jobs with upload
        # or unaprse errors are removed from the working set.
        jsonData = []
        for i in range(len(jobs) - 1, -1, -1):
            if 'msg' in jobs[i]:
                jobs.pop(i)
                continue

            jobs[i]['descstr'] = parser.unparse(jobs[i]['desc'])
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
                self.logger.debug(f"Jobs PUT response - {status} {jsonData}")
            except ACTClientError as exc:
                self.logger.debug(f"Jobs PUT error: {exc}")
                error = str(exc)
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
            sigint.restore()
        except KeyboardInterrupt:
            raise SubmissionInterrupt(results)
        else:
            return results

    def submitJobs(self, descs, clusterlist, webdavClient, webdavBase):
        results = []
        for batch in _sublistGenerator(descs, size=100):
            print("Submitting batch of 100 jobs ...")
            try:
                results.extend(self.submitJobBatch(batch, clusterlist, webdavClient, webdavBase))
            except SubmissionInterrupt as exc:
                results.extend(exc.results)
                raise SubmissionInterrupt(results)
        return results

    def uploadJobData(self, job, webdavClient, webdavBase):
        # create a dictionary of files to upload
        files = {}
        for infile in job['desc'].get('inputfiles', []):
            path = infile[1]
            if not path:
                path = infile[0]

            # parse as URL, remote resource if scheme or hostname
            try:
                url = urlparse(path)
            except ValueError as e:
                job['msg'] = f'Error parsing source of file {infile[0]}: {e}'
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
                url = f'{webdavBase}/{job["id"]}/{infile[0]}'
                infile[1] = url

            files[infile[0]] = path

        # create job directory in WebDAV storage
        if webdavBase:
            try:
                dirURL = f"{webdavBase}/{job['id']}"
                webdavClient.mkdir(dirURL)
                self.logger.debug(f"Created WebDAV directory {dirURL}")
            except Exception as exc:
                self.logger.debug(f"Error creating WebDAV directory {dirURL}: {exc}")
                job['msg'] = str(exc)
                return

        # upload input files
        for dst, src in files.items():
            try:
                if webdavBase:
                    fileURL = f"{webdavBase}/{job['id']}/{dst}"
                    webdavClient.uploadFile(fileURL, src)
                    self.logger.debug(f"Uploaded {src} to {fileURL} for job {job['id']}")
                else:
                    self.uploadFile(job['id'], dst, src)
                    self.logger.debug(f"Uploaded {src} to {dst} for job {job['id']}")
            except Exception as exc:
                self.logger.debug(f"Error uploading {src} to {dst} for job {job['id']}: {exc}")
                job['msg'] = f'Error uploading {src} to {dst}: {exc}'
                return

    def getInfo(self):
        return self.request('GET', '/info', token=self.token)

    def close(self):
        self.httpClient.close()


class WebDAVClient:

    def __init__(self, url, proxypath=None, logger=None):
        self.logger = logger
        if self.logger is None:
            self.logger = getNullLogger()

        self.httpClient = HTTPClient(url, proxypath=proxypath, logger=self.logger)

    def rmdir(self, url):
        headers = {'Accept': '*/*', 'Connection': 'Keep-Alive'}
        resp = self.httpClient.request('DELETE', url, headers=headers)
        text = resp.read().decode()
        self.logger.debug(f"WebDAV DELETE response - {resp.status} {text}")

        # TODO: should we rely on 204 and 404 being the only right answers?
        if resp.status == 404:  # ignore, because we are just trying to delete
            return
        if resp.status >= 300:
            raise ACTClientError(f'Unexpected response for removal of WebDAV directory: {text}')

    def mkdir(self, url):
        headers = {'Accept': '*/*', 'Connection': 'Keep-Alive'}
        resp = self.httpClient.request('MKCOL', url, headers=headers)
        text = resp.read().decode()
        self.logger.debug(f"WebDAV MKDIR response - {resp.status} {text}")

        if resp.status != 201:
            raise ACTClientError(f'Error creating WebDAV directory {url}: {text}')

    def uploadFile(self, url, path):
        self.logger.debug(f"Uploading {path} to {url}")
        try:
            f = open(path, 'rb')
        except Exception as exc:
            self.logger.debug(f"Error uploading {path} to {url}: {exc}")
            raise ACTClientError(f'Error opening file {path}: {exc}')

        with f:
            resp = self.httpClient.request('PUT', url, headers={'Expect': '100-continue'})
            resp.read()
            self.logger.debug(f"Upload redirect check status: {resp.status}")
            if resp.status == 307:
                dstURL = resp.getheader('Location')
                self.logger.debug(f"Redirecting upload to {dstURL}")
                parts = urlparse(dstURL)
                urlPath = f'{parts.path}?{parts.query}'
                nodeClient = HTTPClient(dstURL, logger=self.logger)
                try:
                    # if headers are not explicitly set to empty they will
                    # somehow be taken from previous separate connection
                    # contexts?
                    resp = nodeClient.request('PUT', urlPath, data=f, headers={})
                    text = resp.read()
                    status = resp.status
                    self.logger.debug(f"Upload of {path} to {urlPath} response - {status} {text}")
                except http.client.HTTPException as exc:
                    self.logger.debug(f"Error uploading {path} to {urlPath}: {exc}")
                    raise ACTClientError(f"Error uploading {path} to {urlPath}: {exc}")
                finally:
                    nodeClient.close()
            else:
                resp = self.httpClient.request('PUT', url, data=f)
                text = resp.read()
                status = resp.status
                self.logger.debug(f"Upload of {path} to {url} response - {status} {text}")

        if status != 201:
            raise ACTClientError(f'Error uploading file {path}: {text}')

    def cleanJobDirs(self, url, jobids):
        errors = []
        for jobid in jobids:
            dirURL = f'{url}/{jobid}'
            try:
                self.rmdir(dirURL)
            except Exception as exc:
                errors.append(str(exc))
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
    except Exception as exc:
        raise ACTClientError(f'Error storing job results to the file {filename}: {exc}')


def _prepareJobs(descs, clusterlist, parser):
    # read job descriptions into a list of job dictionaries and JSON for
    # aCT REST
    results = []  # resulting list of job dicts
    jobs = []  # a list of jobs being worked on (failed jobs get removed)
    jsonData = []
    for desc in descs:
        try:
            with open(desc, 'r') as f:
                xrslstr = f.read()
            descdicts = parser.parse(xrslstr)
        except Exception as exc:
            results.append({'msg': str(exc), 'descpath': desc, 'cleanup': False})
        else:
            for descdict in descdicts:
                job = {'clusterlist': clusterlist, 'descpath': desc, 'cleanup': False}
                job['desc'] = descdict
                results.append(job)
                jobs.append(job)
                jsonData.append({'clusterlist': clusterlist})
    return results, jobs, jsonData


def _sublistGenerator(lst, size=100):
    if size < 1:
        raise ACTClientError("Invalid sublist size")
    start = 0
    end = len(lst)
    while start < end:
        yield lst[start:start + size]
        start += size


def getACTRestClient(args, conf, useToken=True):
    try:
        if useToken:
            with open(conf['token'], 'r') as f:
                token = f.read()
        else:
            token = None
        logger = getLogger(args)
        actrest = ACTRest(conf['server'], token=token, logger=logger)
    except FileNotFoundError:
        raise ACTClientError(f'Error reading token file {conf["token"]}. Run act proxy.')
    except Exception as exc:
        raise ACTClientError(f'Error creating aCT REST client: {exc}')
    return actrest


def getWebDAVClient(args, conf, webdavBase, useProxy=True):
    try:
        if useProxy:
            proxypath = conf['proxy']
        else:
            proxypath = None
        logger = getLogger(args)
        webdavClient = WebDAVClient(webdavBase, proxypath=proxypath, logger=logger)
    except Exception as exc:
        raise ACTClientError(f'Error creating WebDAV client: {exc}')
    return webdavClient


def getLogger(args):
    if args.verbose:
        return getStdoutLogger()
    else:
        return getNullLogger()


def getNullLogger():
    logger = logging.getLogger('null')
    if not logger.hasHandlers():
        logger.addHandler(logging.NullHandler())
    return logger


def getStdoutLogger():
    logger = logging.getLogger('logger')
    if not logger.hasHandlers():
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.DEBUG)
        handler.setFormatter(logging.Formatter(logging.BASIC_FORMAT))
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)
    return logger


class SubmissionInterrupt(Exception):

    def __init__(self, results=[]):
        self.results = results
