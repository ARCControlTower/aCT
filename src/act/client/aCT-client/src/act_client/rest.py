import concurrent.futures
import datetime
import json
import logging
import os
import queue
import ssl
import threading
from http.client import (HTTPConnection, HTTPException, HTTPSConnection,
                         RemoteDisconnected)
from urllib.parse import urlencode, urlparse

from act_client.x509proxy import parsePEM, signRequest
from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

import arc

HTTP_BUFFER_SIZE = 2**23


# TODO: hardcoded timeout for http.client connection
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
            self.conn = HTTPSConnection(self.host, port=self.port, context=context, timeout=60)
        else:
            if not self.port:
                self.port = 80
            self.conn = HTTPConnection(self.host, port=self.port, timeout=60)

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
        # TODO: should the request be retried for aborted connection by peer?
        except (RemoteDisconnected, BrokenPipeError, ConnectionAbortedError, ConnectionResetError):
            # retry request
            self.conn.request(method, url, body=body, headers=headers)
            resp = self.conn.getresponse()

        return resp

    def close(self):
        """Close connection."""
        self.conn.close()


class ARCRest:

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

            proxyCert, _, issuerChains = parsePEM(proxyStr)
            chain = proxyCert.public_bytes(serialization.Encoding.PEM).decode() + issuerChains + '\n'
            csr = x509.load_pem_x509_csr(csrStr.encode(), default_backend())
            cert = signRequest(csr, self.httpClient.proxypath).decode()
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
            raise ARCError(f"Error delegating proxy {self.httpClient.proxypath} for delegation {delegationID}: {exc}")

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
        Submit jobs specified in given list of job objects.

        Raises:
            - ARCError
            - ARCHTTPError
            - http.client.HTTPException
            - ConnectionError
            - json.JSONDecodeError
        """
        # get delegation for proxy
        delegationID = self.createDelegation()

        jobdescs = arc.JobDescriptionList()
        tosubmit = []  # sublist of jobs that will be submitted
        bulkdesc = ""
        for job in jobs:
            job.delegid = delegationID

            # parse job description
            if not arc.JobDescription_Parse(job.descstr, jobdescs):
                job.errors.append(DescriptionParseError("Failed to parse description"))
                continue

            # add queue and delegation, modify description as necessary for
            # ARC client
            job.desc = jobdescs[-1]
            job.desc.Resources.QueueName = queue
            job.desc.DataStaging.DelegationID = delegationID
            processJobDescription(job.desc)

            # unparse modified description, remove xml version node because it
            # is not accepted by ARC CE, add to bulk description
            unparseResult = job.desc.UnParse("emies:adl")
            if not unparseResult[0]:
                job.errors.append(DescriptionUnparseError("Could not unparse modified description"))
                continue
            descstart = unparseResult[1].find("<ActivityDescription")
            bulkdesc += unparseResult[1][descstart:]

            tosubmit.append(job)

        # merge into bulk description
        if len(tosubmit) > 1:
            bulkdesc = f"<ActivityDescriptions>{bulkdesc}</ActivityDescriptions>"

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

        # get a list of submission results
        jsonData = json.loads(respstr)
        if isinstance(jsonData["job"], dict):
            results = [jsonData["job"]]
        else:
            results = jsonData["job"]

        # process errors, prepare and upload files for a sublist of jobs
        toupload = []
        for job, result in zip(tosubmit, results):
            code, reason = int(result["status-code"]), result["reason"]
            if code != 201:
                job.errors.append(ARCHTTPError(code, reason, f"Submittion error: {code} {reason}"))
            else:
                job.id = result["id"]
                job.state = result["state"]
                toupload.append(job)
        self.uploadJobFiles(toupload, logger)

    def getInputUploads(self, job):
        """
        Return a list of upload dicts.

        Raises:
            - InputFileError
        """
        uploads = []
        for infile in job.desc.DataStaging.InputFiles:
            try:
                path = isLocalInputFile(infile.Name, infile.Sources[0].fullstr())
            except InputFileError as exc:
                job.errors.append(exc)
                continue
            if not path:
                continue

            if path and not os.path.isfile(path):
                job.errors.append(InputFileError(f"Input path {path} is not a file"))
                continue

            uploads.append({
                "jobid": job.id,
                "url": f"{self.baseURL}/jobs/{job.id}/session/{infile.Name}",
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
            uploads = self.getInputUploads(job)
            if job.errors:
                continue

            jobsdict[job.id] = job
            job.cancelEvent = threading.Event()

            for upload in uploads:
                uploadQueue.put(upload)
        if uploadQueue.empty():
            return
        numWorkers = min(uploadQueue.qsize(), workers)

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
            job.errors.append(result["error"])

    # TODO: blocksize is only added in python 3.7!!!!!!!
    # TODO: hardcoded workers
    def fetchJobs(self, downloadDir, jobs, workers=10, blocksize=HTTP_BUFFER_SIZE, logger=None):
        if logger is None:
            logger = logging.getLogger(__name__).addHandler(logging.NullHandler())

        transferQueue = TransferQueue(workers)
        resultQueue = queue.Queue()

        jobsdict = {}
        for job in jobs:
            jobsdict[job.id] = job
            job.cancelEvent = threading.Event()

            # Add diagnose files to transfer queue and remove them from
            # downloadfiles string. Replace download files with a list of
            # remaining download patterns.
            self.processDiagnoseDownloads(job, transferQueue)

            # add job session directory as a listing transfer
            transferQueue.put({
                "jobid": job.id,
                "url": f"{self.baseURL}/jobs/{job.id}/session",
                "path": "",
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
            jobsdict[result["jobid"]].errors.append(result["error"])
            resultQueue.task_done()

    def processDiagnoseDownloads(self, job, transferQueue):
        DIAG_FILES = [
            "failed", "local", "errors", "description", "diag", "comment",
            "status", "acl", "xml", "input", "output", "input_status",
            "output_status", "statistics"
        ]

        if not job.downloadFiles:
            return []

        # add all diagnose files to transfer queue and create
        # a list of download patterns
        newDownloads = []
        diagFiles = set()  # to remove any possible duplications
        for download in job.downloadFiles:
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
                "jobid": job.id,
                "url": f"{self.baseURL}/jobs/{job.id}/diagnose/{diagName}",
                "path": diagFile,
                "type": "diagnose"
            })

        job.downloadFiles = newDownloads

    def getJobsList(self):
        resp = self.httpClient.request(
            "GET",
            f"{self.baseURL}/jobs",
            headers={"Accept": "application/json"}
        )
        respstr = resp.read().decode()
        if resp.status != 200:
            raise ARCHTTPError(resp.status, respstr, f"ARC jobs list error: {resp.status} {respstr}")
        try:
            jsonData = json.loads(respstr)
        except json.JSONDecodeError:
            return []

        # convert data to list
        if isinstance(jsonData["job"], dict):
            return [jsonData["job"]]
        else:
            return jsonData["job"]

    def getJobsInfo(self, jobs):
        results = self.manageJobs(jobs, "info")
        for job, result in zip(jobs, results):
            code, reason = int(result["status-code"]), result["reason"]
            if code != 200:
                job.errors.append(ARCHTTPError(code, reason, f"{code} {reason}"))
            elif "info_document" not in result:
                job.errors.append(NoValueInARCResult(f"No info document in successful info response"))
            else:
                job.updateFromInfo(result["info_document"])

    def getJobsStatus(self, jobs):
        results = self.manageJobs(jobs, "status")
        for job, result in zip(jobs, results):
            code, reason = int(result["status-code"]), result["reason"]
            if code != 200:
                job.errors.append(ARCHTTPError(code, reason, f"{code} {reason}"))
            elif "state" not in result:
                job.errors.append(NoValueInARCResult(f"No state in successful status response"))
            else:
                job.state = result["state"]

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
        for job, result in zip(jobs, results):
            code, reason = int(result["status-code"]), result["reason"]
            if code != 200:
                job.errors.append(ARCHTTPError(code, reason, f"{code} {reason}"))
            elif "delegation_id" not in result:
                job.errors.append(NoValueInARCResult(f"No delegation ID in successful response"))
            else:
                job.delegid = result["delegation_id"]

    def manageJobs(self, jobs, action):
        ACTIONS = ("info", "status", "kill", "clean", "restart", "delegations")
        if not jobs:
            return []

        if action not in ACTIONS:
            raise ARCError(f"Invalid job management operation: {action}")

        # JSON data for request
        tomanage = [{"id": job.id} for job in jobs]
        if len(tomanage) == 1:
            jsonData = {"job": tomanage[0]}
        else:
            jsonData = {"job": tomanage}

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


class ARCJob:

    def __init__(self):
        self.id = None
        self.name = None
        self.delegid = None
        self.descstr = None
        self.desc = None
        self.state = None
        self.tstate = None
        self.cancelEvent = None
        self.errors = []
        self.downloadFiles = []

        self.ExecutionNode = None
        self.UsedTotalWallTime = None
        self.UsedTotalCPUTime = None
        self.RequestedTotalWallTime = None
        self.RequestedTotalCPUTime = None
        self.RequestedSlots = None
        self.ExitCode = None
        self.Type = None
        self.LocalIDFromManager = None
        self.WaitingPosition = None
        self.Owner = None
        self.LocalOwner = None
        self.StdIn = None
        self.StdOut = None
        self.StdErr = None
        self.LogDir = None
        self.Queue = None
        self.UsedMainMemory = None
        self.SubmissionTime = None
        self.EndTime = None
        self.WorkingAreaEraseTime = None
        self.ProxyExpirationTime = None
        self.RestartState = []
        self.Error = []

    def updateFromInfo(self, infoDocument):
        infoDict = infoDocument.get("ComputingActivity", {})
        if not infoDict:
            return

        if "Name" in infoDict:
            self.name = infoDict["Name"]

        # get state from a list of activity states in different systems
        for state in infoDict.get("State", []):
            if state.startswith("arcrest:"):
                self.state = state[len("arcrest:"):]

        if "Error" in infoDict:
            if isinstance(infoDict["Error"], list):
                self.Error = infoDict["Error"]
            else:
                self.Error = [infoDict["Error"]]

        if "ExecutionNode" in infoDict:
            if isinstance(infoDict["ExecutionNode"], list):
                self.ExecutionNode = infoDict["ExecutionNode"]
            else:
                self.ExecutionNode = [infoDict["ExecutionNode"]]
            # throw out all non ASCII characters from nodes
            for i in range(len(self.ExecutionNode)):
                self.ExecutionNode[i] = ''.join([i for i in self.ExecutionNode[i] if ord(i) < 128])

        if "UsedTotalWallTime" in infoDict:
            self.UsedTotalWallTime = int(infoDict["UsedTotalWallTime"])

        if "UsedTotalCPUTime" in infoDict:
            self.UsedTotalCPUTime = int(infoDict["UsedTotalCPUTime"])

        if "RequestedTotalWallTime" in infoDict:
            self.RequestedTotalWallTime = int(infoDict["RequestedTotalWallTime"])

        if "RequestedTotalCPUTime" in infoDict:
            self.RequestedTotalCPUTime = int(infoDict["RequestedTotalCPUTime"])

        if "RequestedSlots" in infoDict:
            self.RequestedSlots = int(infoDict["RequestedSlots"])

        if "ExitCode" in infoDict:
            self.ExitCode = int(infoDict["ExitCode"])

        if "Type" in infoDict:
            self.Type = infoDict["Type"]

        if "LocalIDFromManager" in infoDict:
            self.LocalIDFromManager = infoDict["LocalIDFromManager"]

        if "WaitingPosition" in infoDict:
            self.WaitingPosition = int(infoDict["WaitingPosition"])

        if "Owner" in infoDict:
            self.Owner = infoDict["Owner"]

        if "LocalOwner" in infoDict:
            self.LocalOwner = infoDict["LocalOwner"]

        if "StdIn" in infoDict:
            self.StdIn = infoDict["StdIn"]

        if "StdOut" in infoDict:
            self.StdOut = infoDict["StdOut"]

        if "StdErr" in infoDict:
            self.StdErr = infoDict["StdErr"]

        if "LogDir" in infoDict:
            self.LogDir = infoDict["LogDir"]

        if "Queue" in infoDict:
            self.Queue = infoDict["Queue"]

        if "UsedMainMemory" in infoDict:
            self.UsedMainMemory = int(infoDict["UsedMainMemory"])

        if "SubmissionTime" in infoDict:
            self.SubmissionTime = datetime.datetime.strptime(
                infoDict["SubmissionTime"],
                "%Y-%m-%dT%H:%M:%SZ"
            )

        if "EndTime" in infoDict:
            self.EndTime = datetime.datetime.strptime(
                infoDict["EndTime"],
                "%Y-%m-%dT%H:%M:%SZ"
            )

        if "WorkingAreaEraseTime" in infoDict:
            self.WorkingAreaEraseTime = datetime.datetime.strptime(
                infoDict["WorkingAreaEraseTime"],
                "%Y-%m-%dT%H:%M:%SZ"
            )

        if "ProxyExpirationTime" in infoDict:
            self.ProxyExpirationTime = datetime.datetime.strptime(
                infoDict["ProxyExpirationTime"],
                "%Y-%m-%dT%H:%M:%SZ"
            )

        if "RestartState" in infoDict:
            self.RestartState = infoDict["RestartState"]


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
        if job.cancelEvent.is_set():
            continue

        try:
            infile = open(upload["path"], "rb")
        except Exception as exc:
            job.cancelEvent.set()
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
                    job.cancelEvent.set()
                    resultQueue.put({
                        "jobid": upload["jobid"],
                        "error": ARCHTTPError(resp.status, text, f"Upload {upload['path']} to {upload['url']} failed: {resp.status} {text}")
                    })
            except Exception as exc:
                job.cancelEvent.set()
                resultQueue.put({
                    "jobid": upload["jobid"],
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

        if job.cancelEvent.is_set():
            continue

        try:
            if transfer["type"] in ("file", "diagnose"):
                # filter out download files that are not specified
                if not transfer["type"] == "diagnose":
                    if filterOutFile(job.downloadFiles, transfer["path"]):
                        continue

                # download file
                path = f"{downloadDir}/{transfer['jobid']}/{transfer['path']}"
                try:
                    downloadFile(httpClient, transfer["url"], path)
                except ARCHTTPError as exc:
                    logger.error(f"Error downloading file {transfer['url']}: {exc}")
                    error = exc
                    if exc.status == 404:
                        if transfer["type"] == "diagnose":
                            error = MissingDiagnoseFile(transfer["url"])
                        else:
                            error = MissingOutputFile(transfer["url"])
                    resultQueue.put({
                        "jobid": transfer["jobid"],
                        "error": error
                    })
                    continue
                except Exception as exc:
                    job.cancelEvent.set()
                    logger.error(str(exc))
                    resultQueue.put({
                        "jobid": transfer["jobid"],
                        "error": exc
                    })
                    continue

                logger.info(f"Successfully downloaded file {transfer['url']} to {path}")

            elif transfer["type"] == "listing":

                # filter out listings that do not match download patterns
                if filterOutListing(job.downloadFiles, transfer["path"]):
                    continue

                # download listing
                try:
                    listing = downloadListing(httpClient, transfer["url"])
                except ARCHTTPError as exc:
                    logger.error(f"Error downloading listing {transfer['url']}: {exc}")
                    resultQueue.put({
                        "jobid": transfer["jobid"],
                        "error": exc
                    })
                    continue
                except Exception as exc:
                    job.cancelEvent.set()
                    logger.error(str(exc))
                    resultQueue.put({
                        "jobid": transfer["jobid"],
                        "error": exc
                    })
                    continue

                logger.info(f"Successfully downloaded listing {transfer['url']}")

                # create new transfer jobs
                transfers = createTransfersFromListing(
                    endpoint, listing, transfer["path"], transfer["jobid"]
                )
                for transfer in transfers:
                    transferQueue.put(transfer)
        except:
            import traceback
            excstr = traceback.format_exc()
            job.cancelEvent.set()
            logger.error(excstr)
            resultQueue.put({
                "jobid": transfer["jobid"],
                "error": excstr
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
    text = resp.read().decode()

    if resp.status != 200:
        raise ARCHTTPError(resp.status, text, f"Error downloading listing {url}: {resp.status} {text}")

    try:
        listing = json.loads(text)
    except json.JSONDecodeError as e:
        if text == '':  # due to invalid JSON returned by ARC REST
            return {}
        raise ARCError(f"Error decoding JSON listing {url}: {e}")

    return listing


def checkJobOperation(jobs, results):
    for job, result in zip(jobs, results):
        code, reason = int(result["status-code"]), result["reason"]
        if code != 202:
            job.errors.append(ARCHTTPError(code, reason, f"{code} {reason}"))


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


class ARCError(Exception):

    def __init__(self, msg=""):
        self.msg = msg

    def __str__(self):
        return self.msg


class ARCHTTPError(ARCError):
    """ARC REST HTTP status error."""

    def __init__(self, status, text, msg=""):
        super().__init__(msg)
        self.status = status
        self.text = text


class DescriptionParseError(ARCError):
    pass


class DescriptionUnparseError(ARCError):
    pass


class InputFileError(ARCError):
    pass


class NoValueInARCResult(ARCError):
    pass


class MissingResultFile(ARCError):

    def __init__(self, filename):
        self.filename = filename
        super().__init__(str(self))

    def __str__(self):
        return f"Missing result file {self.filename}"


class MissingOutputFile(MissingResultFile):

    def __init__(self, filename):
        super().__init__(filename)

    def __str__(self):
        return f"Missing output file {self.filename}"


class MissingDiagnoseFile(MissingResultFile):

    def __init__(self, filename):
        super().__init__(filename)

    def __str__(self):
        return f"Missing diagnose file {self.filename}"


def filterOutFile(downloadFiles, filePath):
    if not downloadFiles:
        return False
    for pattern in downloadFiles:
        # direct match
        if pattern == filePath:
            return False
        # recursive folder match
        elif pattern.endswith("/") and filePath.startswith(pattern):
            return False
        # entire session directory, not matched by above if
        elif pattern == "/":
            return False
    return True


def filterOutListing(downloadFiles, listingPath):
    if not downloadFiles:
        return False
    for pattern in downloadFiles:
        # part of pattern
        if pattern.startswith(listingPath):
            return False
        # recursive folder match
        elif pattern.endswith("/") and listingPath.startswith(pattern):
            return False
    return True


def createTransfersFromListing(endpoint, listing, path, jobid):
    transfers = []
    # create new transfer jobs; duplication except for "type" key
    if "file" in listing:
        if not isinstance(listing["file"], list):
            listing["file"] = [listing["file"]]
        for f in listing["file"]:
            if path:
                newpath = f"{path}/{f}"
            else:  # if session root, slash needs to be skipped
                newpath = f
            transfers.append({
                "jobid": jobid,
                "type": "file",
                "path": newpath,
                "url": f"{endpoint}/jobs/{jobid}/session/{newpath}"
            })
    if "dir" in listing:
        if not isinstance(listing["dir"], list):
            listing["dir"] = [listing["dir"]]
        for d in listing["dir"]:
            if path:
                newpath = f"{path}/{d}"
            else:  # if session root, slash needs to be skipped
                newpath = d
            transfers.append({
                "jobid": jobid,
                "type": "listing",
                "path": newpath,
                "url": f"{endpoint}/jobs/{jobid}/session/{newpath}"
            })
    return transfers
