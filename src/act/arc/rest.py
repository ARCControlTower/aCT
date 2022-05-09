import concurrent.futures
import json
import logging
import os
import queue
import ssl
import threading
from http.client import (HTTPConnection, HTTPException, HTTPSConnection,
                         RemoteDisconnected)
from urllib.parse import urlencode, urlparse

from act.client.delegate_proxy import parse_issuer_cred
from act.client.x509proxy import sign_request
from act.common.exceptions import (ACTError, ARCHTTPError,
                                   DescriptionParseError,
                                   DescriptionUnparseError, InputFileError, NoValueInARCResult)
from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

import arc

HTTP_BUFFER_SIZE = 2**23


class HTTPClient:

    def __init__(self, hostname, proxypath=None, isHTTPS=False, port=None):
        """
        Store connection parameters and connect.

        Raises:
            - exceptions that are thrown by self._connect
        """
        self.host = hostname
        self.port = port
        self.proxypath = proxypath
        self.isHTTPS = isHTTPS

        self._connect()

    def _connect(self):
        """
        Connect to given host and port with optional HTTPS.

        Raises:
            - ssl.SSLError
            - http.client.HTTPException
            - ConnectionError
            - OSError, socket.gaierror (when DNS fails)
        """
        if self.proxypath or self.isHTTPS:
            if self.proxypath:
                context = ssl.SSLContext(ssl.PROTOCOL_TLS)
                context.load_cert_chain(self.proxypath, keyfile=self.proxypath)
            else:
                context = None
            if not self.port:
                self.port = 443
            self.conn = HTTPSConnection(self.host, port=self.port, context=context)
        else:
            if not self.port:
                self.port = 80
            self.conn = HTTPConnection(self.host, port=self.port)

    def request(self, method, endpoint, headers={}, token=None, jsonData=None, data=None, params={}):
        """
        Send request and retry on ConnectionErrors.

        Raises:
            - http.client.HTTPException
            - ConnectionError
            - OSError?
            - socket.gaierror
        """
        if token:
            headers['Authorization'] = f'Bearer {token}'

        if jsonData:
            body = json.dumps(jsonData).encode()
            headers['Content-type'] = 'application/json'
        else:
            body = data

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
            self.conn.request(method, url, body=body, headers=headers)
            resp = self.conn.getresponse()
        except (RemoteDisconnected, ConnectionError):
            # retry request
            self.conn.request(method, url, body=body, headers=headers)
            resp = self.conn.getresponse()

        return resp

    def close(self):
        """Close connection."""
        self.conn.close()


class RESTClient:

    def __init__(self, host, port=443, baseURL='/arex/rest/1.0', proxypath=None):
        self.baseURL = baseURL
        self.httpClient = HTTPClient(host, port=port, proxypath=proxypath)

    def close(self):
        self.httpClient.close()

    def POSTNewDelegation(self):
        resp = self.httpClient.request(
            "POST",
            f"{self.baseURL}/delegations?action=new",
            headers={"Accept": "application/json"}
        )
        respstr = resp.read().decode()

        if resp.status != 201:
            raise ARCHTTPError(resp.status, respstr, f"Cannot create delegation: {resp.status} {respstr}")

        return respstr, resp.getheader('Location').split('/')[-1]

    def POSTRenewDelegation(self, delegationID):
        resp = self.httpClient.request(
            "POST",
            f"{self.baseURL}/delegations/{delegationID}?action=renew"
        )
        respstr = resp.read().decode()

        if resp.status != 201:
            raise ARCHTTPError(resp.status, respstr, f"Cannot renew delegation {delegationID}: {resp.status} {respstr}")

        return respstr

    def PUTDelegation(self, delegationID, csrStr):
        try:
            with open(self.httpClient.proxypath) as f:
                proxyStr = f.read()

            proxyCert, _, issuerChains = parse_issuer_cred(proxyStr)
            chain = proxyCert.public_bytes(serialization.Encoding.PEM).decode() + issuerChains + '\n'
            csr = x509.load_pem_x509_csr(csrStr.encode(), default_backend())
            cert = sign_request(csr, self.httpClient.proxypath).decode()
            pem = (cert + chain).encode()

            resp = self.httpClient.request(
                'PUT',
                f'{self.baseURL}/delegations/{delegationID}',
                data=pem,
                headers={'Content-type': 'application/x-pem-file'}
            )
            respstr = resp.read().decode()

            if resp.status != 200:
                raise ARCHTTPError(resp.status, respstr, f"Cannot upload delegated cert: {resp.status} {respstr}")

        # cryptography exceptions are handled with the base exception so these
        # exceptions need to be handled explicitly to be passed through
        except (HTTPException, ConnectionError):
            raise

        except Exception as exc:
            try:
                self.deleteDelegation(delegationID)
            # ignore this error, delegations get deleted automatically anyway
            # and there is no simple way to encode both errors for now
            except (HTTPException, ConnectionError):
                pass
            raise ACTError(f"Error delegating proxy {self.httpClient.proxypath} for delegation {delegationID}: {exc}")

    def createDelegation(self):
        csr, delegationID = self.POSTNewDelegation()
        self.PUTDelegation(delegationID, csr)
        return delegationID

    def renewDelegation(self, delegationID):
        csr = self.POSTRenewDelegation(delegationID)
        self.PUTDelegation(delegationID, csr)

    def deleteDelegation(self, delegationID):
        resp = self.httpClient.request(
            'POST',
            f'{self.baseURL}/delegations/{delegationID}?action=delete'
        )
        respstr = resp.read().decode()
        if resp.status != 202:
            raise ARCHTTPError(resp.status, respstr, f"Error deleting delegation {delegationID}: {resp.status} {respstr}")

    def submitJobs(self, queue, jobs, logger):
        """
        Submit jobs specified in given list of job dicts.

        Raises:
            - ACTError
            - ARCHTTPError
            - http.client.HTTPException
            - ConnectionError
            - json.JSONDecodeError
            - SSL
        """
        # get delegation for proxy
        delegationID = self.createDelegation()

        # add delegation and queue to description, unparse to ADL
        jobdescs = arc.JobDescriptionList()
        tosubmit = []  # sublist of jobs that will be submitted
        for job in jobs:
            job["errors"] = []
            job["delegation"] = delegationID

            # parse job description, add queue and delegation, unparse
            if not arc.JobDescription_Parse(job["descstr"], jobdescs):
                job["errors"].append(DescriptionParseError("Failed to parse description"))
                continue
            job["desc"] = jobdescs[-1]
            job["desc"].Resources.QueueName = queue
            job["desc"].DataStaging.DelegationID = delegationID
            processJobDescription(job["desc"])
            unparseResult = job["desc"].UnParse("emies:adl")
            #unparseResult = job["desc"].UnParse("nordugrid:xrsl")
            if not unparseResult[0]:
                job["errors"].append(DescriptionUnparseError("Could not unparse modified description"))
                continue
            # throw away xml version node
            descstart = unparseResult[1].find("<ActivityDescription")
            job["adl"] = unparseResult[1][descstart:]

            tosubmit.append(job)

        # merge into bulk descriptions
        if len(tosubmit) == 1:
            bulkdesc = tosubmit[0]["adl"]
        else:
            bulkdesc = "<ActivityDescriptions>"
            for job in tosubmit:
                bulkdesc += job["adl"]
            bulkdesc += "</ActivityDescriptions>"

        # submit jobs to ARC
        resp = self.httpClient.request(
            "POST",
            f"{self.baseURL}/jobs?action=new",
            data=bulkdesc,
            headers={"Accept": "application/json", "Content-type": "application/xml"}
        )
        respstr = resp.read().decode()

        if resp.status != 201:
            raise ARCHTTPError(resp.status, respstr, f"Cannot submit jobs: {resp.status} {respstr}")

        jsonData = json.loads(respstr)

        # get a list of submission results
        if isinstance(jsonData["job"], dict):
            results = [jsonData["job"]]
        else:
            results = jsonData["job"]

        # process errors, prepare and upload files for a sublist of jobs
        toupload = []
        for job, result in zip(tosubmit, results):
            code, reason = int(result["status-code"]), result["reason"]
            if code != 201:
                # TODO: Are there any error cases where this error could be
                # recovered? In such case a specific exception is needed.
                job["errors"].append(ARCHTTPError(code, reason, f"Submittion error: {code} {reason}"))
            else:
                job["arcid"] = result["id"]
                job["state"] = result["state"]
                toupload.append(job)
        self.uploadJobFiles(toupload, logger)

        return jobs

    def getInputUploads(self, job):
        """
        Return a list of upload dicts.

        Raises:
            - InputFileError
        """
        uploads = []
        for infile in job["desc"].DataStaging.InputFiles:
            path = isLocalInputFile(infile.Name, infile.Sources[0].fullstr())
            if not path:
                continue

            if path and not os.path.isfile(path):
                raise InputFileError(f"Input path {path} is not a file")

            uploads.append({
                "jobid": job["id"],
                "url": f"{self.baseURL}/jobs/{job['arcid']}/session/{infile.Name}",
                "path": path
            })

        return uploads

    # TODO: blocksize is only added in python 3.7!!!!!!!
    # TODO: hardcoded number of upload workers
    def uploadJobFiles(self, jobs, logger, workers=10, blocksize=HTTP_BUFFER_SIZE):
        # create transfer queues
        uploadQueue = queue.Queue()
        resultQueue = queue.Queue()

        # put uploads to queue, create cancel events for jobs
        jobsdict = {}
        for job in jobs:
            jobsdict[job["id"]] = job
            job["cancel_event"] = threading.Event()
            try:
                uploads = self.getInputUploads(job)
            except InputFileError as exc:
                job["errors"].append(exc)
                continue
            for upload in uploads:
                uploadQueue.put(upload)
        if uploadQueue.empty():
            return jobs
        numWorkers = min(len(uploads), workers)

        # create HTTP clients for workers
        httpClients = []
        for i in range(numWorkers):
            httpClients.append(HTTPClient(
                self.httpClient.host,
                port=self.httpClient.port,
                proxypath=self.httpClient.proxypath
            ))

        # run upload threads on uploads
        with concurrent.futures.ThreadPoolExecutor(max_workers=numWorkers) as pool:
            futures = []
            for httpClient in httpClients:
                futures.append(pool.submit(
                    uploadTransferWorker,
                    httpClient,
                    jobsdict,
                    uploadQueue,
                    resultQueue,
                    logger
                ))
            concurrent.futures.wait(futures)

        # close HTTP clients
        for httpClient in httpClients:
            httpClient.close()

        # put error messages to job dicts
        while not resultQueue.empty():
            result = resultQueue.get()
            resultQueue.task_done()
            job = jobsdict[result["jobid"]]
            job["errors"].append(result["error"])

        return jobs

    # TODO: blocksize is only added in python 3.7!!!!!!!
    # TODO: hardcoded workers
    def fetchJobs(self, downloadDir, jobs, workers=10, blocksize=HTTP_BUFFER_SIZE, logger=None):
        if logger is None:
            logger = logging.getLogger(__name__).addHandler(logging.NullHandler())

        transferQueue = TransferQueue(workers)
        resultQueue = queue.Queue()

        jobsdict = {}
        for job in jobs:
            # create cancel event and add to jobsdict
            job["cancel_event"] = threading.Event()
            jobsdict[job["id"]] = job

            # Add diagnose files to transfer queue and remove them from
            # downloadfiles string. Replace download files with a list of
            # remaining download patterns.
            job["downloadfiles"] = self.processDiagnoseDownloads(job, transferQueue)

            # add job session directory as a listing transfer
            transferQueue.put({
                "jobid": job["id"],
                "url": f"{self.baseURL}/jobs/{job['arcid']}/session",
                "filename": "",
                "type": "listing"
            })

        # open connections for thread workers
        httpClients = []
        for i in range(workers):
            httpClients.append(HTTPClient(
                self.httpClient.host,
                port=self.httpClient.port,
                proxypath=self.httpClient.proxypath
            ))

        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
            futures = []
            for httpClient in httpClients:
                futures.append(pool.submit(downloadTransferWorker, httpClient, transferQueue, resultQueue, downloadDir, jobsdict, self.baseURL, logger))
            concurrent.futures.wait(futures)

        for httpClient in httpClients:
            httpClient.close()

        while not resultQueue.empty():
            result = resultQueue.get()
            jobsdict[result["jobid"]]["errors"].append(result["error"])
            resultQueue.task_done()

        return jobs

    def processDiagnoseDownloads(self, job, transferQueue):
        DIAG_FILES = [
            "failed", "local", "errors", "description", "diag", "comment",
            "status", "acl", "xml", "input", "output", "input_status",
            "output_status", "statistics"
        ]

        if not job["downloadfiles"]:
            return []

        # add all diagnose files to transfer queue and create
        # a list of download patterns
        downloads = job["downloadfiles"].split(";")
        newDownloads = []
        diagFiles = set()  # to remove any possible duplications
        for download in downloads:
            if download.startswith("diagnose="):
                # remove diagnose= part
                diagnose = download[len("diagnose="):]
                if not diagnose:
                    continue  # error?

                # add all files if entire log folder is specified
                if diagnose.endswith("/"):
                    for diagFile in DIAG_FILES:
                        diagFiles.add(f"{diagnose}{diagFile}")

                else:
                    diagFile = diagnose.split("/")[-1]
                    if diagFile not in DIAG_FILES:
                        continue  # error?
                    diagFiles.add(diagnose)
            else:
                newDownloads.append(download)

        for diagFile in diagFiles:
            diagName = diagFile.split("/")[-1]
            transferQueue.put({
                "jobid": job["id"],
                "url": f"{self.baseURL}/jobs/{job['arcid']}/diagnose/{diagName}",
                "filename": diagFile,
                "type": "diagnose"
            })

        return newDownloads

    def getJobsInfo(self, jobs):
        results = self.manageJobs(jobs, "info")
        return getJobOperationResults(jobs, results, "info_document")


    def getJobsStatus(self, jobs):
        results = self.manageJobs(jobs, "status")
        return getJobOperationResults(jobs, results, "state")


    def killJobs(self, jobs):
        results = self.manageJobs(jobs, "kill")
        return checkJobOperation(jobs, results)


    def cleanJobs(self, jobs):
        results = self.manageJobs(jobs, "clean")
        return checkJobOperation(jobs, results)


    def restartJobs(self, jobs):
        results = self.manageJobs(jobs, "restart")
        return checkJobOperation(jobs, results)


    def getJobsDelegations(self, jobs, logger=None):
        # AF BUG
        try:
            results = self.manageJobs(jobs, "delegations")
        except:
            logger.debug("DELEGATIONS FETCH EXCEPTION")
            import traceback
            logger.debug(traceback.format_exc())
            results = []
        return getJobOperationResults(jobs, results, "delegation_id")

    def manageJobs(self, jobs, action):
        ACTIONS = ("info", "status", "kill", "clean", "restart", "delegations")
        if not jobs:
            return jobs

        if action not in ACTIONS:
            raise ACTError(f"Invalid job management operation: {action}")

        # JSON data for request
        tomanage = [{"id": job["arcid"]} for job in jobs]
        jsonData = {}
        if len(tomanage) == 1:
            jsonData["job"] = tomanage[0]
        else:
            jsonData["job"] = tomanage

        # execute action and get JSON result
        resp = self.httpClient.request(
            "POST",
            f"{self.baseURL}/jobs?action={action}",
            jsonData=jsonData,
            headers={"Accept": "application/json", "Content-type": "application/json"}
        )
        respstr = resp.read().decode()
        if resp.status != 201:
            raise ARCHTTPError(resp.status, respstr, f"ARC jobs \"{action}\" action error: {resp.status} {respstr}")
        jsonData = json.loads(respstr)

        # convert data to list
        if isinstance(jsonData["job"], dict):
            return [jsonData["job"]]
        else:
            return jsonData["job"]


class TransferQueue:

    def __init__(self, numWorkers):
        self.queue = queue.Queue()
        self.lock = threading.Lock()
        self.barrier = threading.Barrier(numWorkers)

    def put(self, val):
        with self.lock:
            self.queue.put(val)
            self.barrier.reset()

    def get(self):
        while True:
            with self.lock:
                if not self.queue.empty():
                    val = self.queue.get()
                    self.queue.task_done()
                    return val

            try:
                self.barrier.wait()
            except threading.BrokenBarrierError:
                continue
            else:
                raise TransferQueueEmpty()


class TransferQueueEmpty(Exception):
    pass


def isLocalInputFile(name, path):
    """
    Return path if local or empty string if remote URL.

    Raises:
        - InputFileError
    """
    if not path:
        return name

    try:
        url = urlparse(path)
    except ValueError as exc:
        raise InputFileError("Error parsing source {path} of file {file.Name}: {exc}")
    if url.scheme not in ("file", None, "") or url.hostname:
        return ""

    return url.path


def uploadTransferWorker(httpClient, jobsdict, uploadQueue, resultQueue, logger):
    while True:
        try:
            upload = uploadQueue.get(block=False)
        except queue.Empty:
            break
        uploadQueue.task_done()

        job = jobsdict[upload["jobid"]]
        if job["cancel_event"].is_set():
            continue

        try:
            infile = open(upload["path"], "rb")
        except Exception as exc:
            job["cancel_event"].set()
            resultQueue.put({
                "jobid": upload["jobid"],
                "error": exc
            })
            continue

        with infile:
            try:
                resp = httpClient.request("PUT", upload["url"], data=infile)
                text = resp.read().decode()
                if resp.status != 200:
                    job["cancel_event"].set()
                    resultQueue.put({
                        "id": upload["jobid"],
                        "error": ARCHTTPError(resp.status, text, f"Upload {upload['path']} to {upload['url']} failed: {resp.status} {text}")
                    })
            except Exception as exc:
                job["cancel_event"].set()
                resultQueue.put({
                    "id": upload["jobid"],
                    "error": exc
                })


# TODO: add more logging context (job ID?)
def downloadTransferWorker(httpClient, transferQueue, resultQueue, downloadDir, jobsdict, endpoint, logger=None):
    if logger is None:
        logger = logging.getLogger(__name__).addHandler(logging.NullHandler())

    while True:
        try:
            transfer = transferQueue.get()
        except TransferQueueEmpty():
            break

        job = jobsdict[transfer["jobid"]]

        if job["cancel_event"].is_set():
            continue

        if transfer["type"] in ("file", "diagnose"):
            # filter out download files that are not specified
            if job["downloadfiles"] and not transfer["type"] == "diagnose":
                toDownload = False
                for pattern in job["downloadfiles"]:
                    # direct match
                    if pattern == transfer["filename"]:
                        toDownload = True
                        break
                    # recursive folder match
                    elif pattern.endswith("/") and transfer["filename"].startswith(pattern):
                        toDownload = True
                        break
                    # entire session directory, not matched by above if
                    elif pattern == "/":
                        toDownload = True
                        break
                if not toDownload:
                    continue

            # download file
            path = f"{downloadDir}/{job['arcid']}/{transfer['filename']}"
            try:
                downloadFile(httpClient, transfer["url"], path)
            except ARCHTTPError as exc:
                # don't stop downloading files when files are missing (404)
                if exc.status == 404:
                    # don't signal missing diagnose file as error
                    if transfer["type"] == "diagnose":
                        logger.info(f"Missing diagnose file {transfer['url']}")
                        continue
                else:
                    job["cancel_event"].set()

                logger.error(str(exc))
                resultQueue.put({
                    "jobid": job["id"],
                    "error": exc
                })

            except Exception as exc:
                job["cancel_event"].set()
                logger.error(str(exc))
                resultQueue.put({
                    "jobid": job["id"],
                    "error": exc
                })
                continue

            logger.info(f"Successfully downloaded file {transfer['url']} to {path}")

        elif transfer["type"] == "listing":

            # filter out listings that do not match download patterns
            if job["downloadfiles"]:
                toDownload = False
                for pattern in job["downloadfiles"]:
                    # part of pattern
                    if pattern.startswith(transfer["filename"]):
                        toDownload = True
                    # recursive folder match
                    elif pattern.endswith("/") and transfer["filename"].startswith(pattern):
                        toDownload = True
                if not toDownload:
                    continue

            # download listing
            try:
                listing = downloadListing(httpClient, transfer["url"])
            except ARCHTTPError as exc:
                logger.error(f"Error downloading listing {transfer['url']}: {exc}")
                resultQueue.put({
                    "jobid": job["id"],
                    "error": exc
                })
                continue
            except Exception as exc:
                job["cancel_event"].set()
                logger.error(str(exc))
                resultQueue.put({
                    "jobid": job["id"],
                    "error": exc
                })
                continue

            logger.info(f"Successfully downloaded listing {transfer['url']}")

            # create new transfer jobs; duplication except for "type" key
            if "file" in listing:
                if not isinstance(listing["file"], list):
                    listing["file"] = [listing["file"]]
                for f in listing["file"]:
                    if transfer["filename"]:
                        filename = f"{transfer['filename']}/{f}"
                    else:  # if session root, slash needs to be skipped
                        filename = f
                    transferQueue.put({
                        "jobid": job["id"],
                        "type": "file",
                        "filename": filename,
                        "url": f"{endpoint}/jobs/{job['arcid']}/session/{filename}"
                    })
            elif "dir" in listing:
                if not isinstance(listing["dir"], list):
                    listing["dir"] = [listing["dir"]]
                for d in listing["dir"]:
                    if transfer["filename"]:
                        filename = f"{transfer['filename']}/{d}"
                    else:  # if session root, slash needs to be skipped
                        filename = d
                    transferQueue.put({
                        "jobid": job["id"],
                        "type": "listing",
                        "filename": filename,
                        "url": f"{endpoint}/jobs/{job['arcid']}/session/{filename}"
                    })


def downloadFile(httpClient, url, path):
    resp = httpClient.request("GET", url)

    if resp.status != 200:
        text = resp.read().decode()
        raise ARCHTTPError(resp.status, text, f"Error downloading URL {url} to {path}: {resp.status} {text}")

    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        data = resp.read(HTTP_BUFFER_SIZE)
        while data:
            f.write(data)
            data = resp.read(HTTP_BUFFER_SIZE)


def downloadListing(httpClient, url):
    resp = httpClient.request("GET", url, headers={"Accept": "application/json"})

    if resp.status != 200:
        text = resp.read()
        raise ARCHTTPError(resp.status, text, f"Error downloading listing {url}: {resp.status} {text}")

    try:
        listing = json.loads(resp.read().decode())
    except json.JSONDecodeError as e:
        raise ACTError(f"Error decoding JSON listing {url}: {e}")

    return listing


def checkJobOperation(jobs, results):
    for job, result in zip(jobs, results):
        code, reason = int(result["status-code"]), result["reason"]
        if code != 202:
            job["errors"].append(ARCHTTPError(code, reason, f"{code} {reason}"))
    return jobs


def getJobOperationResults(jobs, results, key):
    for job, result in zip(jobs, results):
        code, reason = int(result["status-code"]), result["reason"]
        if code != 200:
            job["errors"].append(ARCHTTPError(code, reason, f"{code} {reason}"))
        elif key not in result:
            job["errors"].append(NoValueInARCResult(f"No {key} in positive result!!!"))
        else:
            job[key] = result[key]
    return jobs


def processJobDescription(jobdesc):
    exepath = jobdesc.Application.Executable.Path
    if exepath and exepath.startswith("/"):  # absolute paths are on compute nodes
        exepath = ""
    inpath = jobdesc.Application.Input
    outpath = jobdesc.Application.Output
    errpath = jobdesc.Application.Error
    logpath = jobdesc.Application.LogDir

    exePresent = False
    stdinPresent = False
    for infile in jobdesc.DataStaging.InputFiles:
        if exepath == infile.Name:
            exePresent = True
        elif inpath == infile.Name:
            stdinPresent = True

    stdoutPresent = False
    stderrPresent = False
    logPresent = False
    for outfile in jobdesc.DataStaging.OutputFiles:
        if outpath == outfile.Name:
            stdoutPresent = True
        elif errpath == outfile.Name:
            stderrPresent = True
        elif logpath == outfile.Name or logpath == outfile.Name[:-1]:
            logPresent = True

    if exepath and not exePresent:
        infile = arc.InputFileType()
        infile.Name = exepath
        jobdesc.DataStaging.InputFiles.append(infile)

    if inpath and not stdinPresent:
        infile = arc.InputFileType()
        infile.Name = inpath
        jobdesc.DataStaging.InputFiles.append(infile)

    if outpath and not stdoutPresent:
        outfile = arc.OutputFileType()
        outfile.Name = outpath
        jobdesc.DataStaging.OutputFiles.append(outfile)

    if errpath and not stderrPresent:
        outfile = arc.OutputFileType()
        outfile.Name = errpath
        jobdesc.DataStaging.OutputFiles.append(outfile)

    if logpath and not logPresent:
        outfile = arc.OutputFileType()
        if not logpath.endswith('/'):
            outfile.Name = f'{logpath}/'
        else:
            outfile.Name = logpath
        jobdesc.DataStaging.OutputFiles.append(outfile)
