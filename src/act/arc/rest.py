import concurrent.futures
import datetime
import json
import logging
import os
import queue
import threading
from http.client import HTTPException
from urllib.parse import urlparse

from act.arc.httpclient import HTTP_BUFFER_SIZE, HTTPClient
from act.arc.x509proxy import parsePEM, signRequest
from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

import arc


class ARCRest:

    def __init__(self, url, basePath='/arex/rest/1.0', proxypath=None, logger=None):
        self.logger = logger
        if not self.logger:
            self.logger = logging.getLogger('null')
            if not self.logger.hasHandlers():
                self.logger.addHandler(logging.NullHandler())

        self.basePath = basePath
        self.httpClient = HTTPClient(url=url, proxypath=proxypath, logger=self.logger)

    def close(self):
        self.httpClient.close()

    def POSTNewDelegation(self):
        resp = self.httpClient.request(
            "POST",
            f"{self.basePath}/delegations?action=new",
            headers={"Accept": "application/json"}
        )
        respstr = resp.read().decode()

        self.logger.debug(f"Create delegation response - {resp.status} {respstr}")

        if resp.status != 201:
            raise ARCHTTPError(resp.status, respstr, f"Cannot create delegation - {resp.status} {respstr}")

        return respstr, resp.getheader('Location').split('/')[-1]

    def POSTRenewDelegation(self, delegationID):
        resp = self.httpClient.request(
            "POST",
            f"{self.basePath}/delegations/{delegationID}?action=renew"
        )
        respstr = resp.read().decode()

        self.logger.debug(f"Renew delegaton {delegationID} response - {resp.status} {respstr}")

        if resp.status != 201:
            raise ARCHTTPError(resp.status, respstr, f"Cannot renew delegation {delegationID}: {resp.status} {respstr}")

        return respstr

    def PUTDelegation(self, delegationID, csrStr, lifetime=None):
        try:
            with open(self.httpClient.proxypath) as f:
                proxyStr = f.read()

            proxyCert, _, issuerChains = parsePEM(proxyStr)
            chain = proxyCert.public_bytes(serialization.Encoding.PEM).decode() + issuerChains + '\n'
            csr = x509.load_pem_x509_csr(csrStr.encode(), default_backend())
            cert = signRequest(csr, self.httpClient.proxypath, lifetime=lifetime).decode()
            pem = (cert + chain).encode()

            resp = self.httpClient.request(
                'PUT',
                f'{self.basePath}/delegations/{delegationID}',
                data=pem,
                headers={'Content-type': 'application/x-pem-file'}
            )
            respstr = resp.read().decode()

            self.logger.debug(f"Upload delegation {delegationID} response - {resp.status} {respstr}")

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

    def createDelegation(self, lifetime=None):
        csr, delegationID = self.POSTNewDelegation()
        self.PUTDelegation(delegationID, csr, lifetime=lifetime)
        self.logger.debug(f"Successfully created delegation {delegationID}")
        return delegationID

    def renewDelegation(self, delegationID, lifetime=None):
        csr = self.POSTRenewDelegation(delegationID)
        self.PUTDelegation(delegationID, csr, lifetime=lifetime)
        self.logger.debug(f"Successfully renewed delegation {delegationID}")

    def deleteDelegation(self, delegationID):
        resp = self.httpClient.request(
            'POST',
            f'{self.basePath}/delegations/{delegationID}?action=delete'
        )
        respstr = resp.read().decode()

        self.logger.debug(f"Delete delegation {delegationID} response - {resp.status} {respstr}")

        if resp.status != 202:
            raise ARCHTTPError(resp.status, respstr, f"Error deleting delegation {delegationID}: {resp.status} {respstr}")

        self.logger.debug(f"Successfully deleted delegation {delegationID}")

    # TODO: determine if this interface for optional data upload should be
    # used.
    def submitJobs(self, queue, jobs, uploadData=True):
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
                self.logger.debug(f"Failed to parse description {job.descstr}")
                continue

            # add queue and delegation, modify description as necessary for
            # ARC client
            desc = jobdescs[-1]
            desc.Resources.QueueName = queue
            desc.DataStaging.DelegationID = delegationID
            processJobDescription(desc)

            # get input files from description
            self.getInputFiles(job, desc)

            # read name from description
            job.name = desc.Identification.JobName
            self.logger.debug(f"Job name from description: {job.name}")

            # unparse modified description, remove xml version node because it
            # is not accepted by ARC CE, add to bulk description
            unparseResult = desc.UnParse("emies:adl")
            if not unparseResult[0]:
                job.errors.append(DescriptionUnparseError(f"Could not unparse modified description of job {job.name}"))
                self.logger.debug(f"Could not unparse modified description of job {job.name}")
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
            f"{self.basePath}/jobs?action=new",
            data=bulkdesc,
            headers={"Accept": "application/json", "Content-type": "application/xml"}
        )
        respstr = resp.read().decode()

        self.logger.debug(f"Job submit response - {resp.status} {respstr}")

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
        if uploadData:
            self.uploadJobFiles(toupload)

    def getInputFiles(self, job, desc):
        job.inputFiles = {}
        for infile in desc.DataStaging.InputFiles:
            source = None
            if len(infile.Sources) > 0:
                source = infile.Sources[0].fullstr()
            job.inputFiles[infile.Name] = source

    def getInputUploads(self, job):
        """
        Return a list of upload dicts.

        Raises:
            - InputFileError
        """
        uploads = []
        for name, source in job.inputFiles.items():
            try:
                path = isLocalInputFile(name, source)
            except InputFileError as exc:
                job.errors.append(exc)
                self.logger.debug(f"Error parsing input {name} at {source} for job {job.id}: {exc}")
                continue
            if not path:
                self.logger.debug(f"Skipping non local input {name} at {source} for job {job.id}")
                continue

            if not os.path.isfile(path):
                msg = f"Local input {name} at {path} for job {job.id} is not a file"
                job.errors.append(InputFileError(msg))
                self.logger.debug(msg)
                continue

            uploads.append({
                "jobid": job.id,
                "url": f"{self.basePath}/jobs/{job.id}/session/{name}",
                "path": path
            })
            self.logger.debug(f"Will upload local input {name} at {path} for job {job.id}")

        return uploads

    # TODO: blocksize is only added in python 3.7!!!!!!!
    # TODO: hardcoded number of upload workers
    def uploadJobFiles(self, jobs, workers=10, blocksize=HTTP_BUFFER_SIZE):
        # create transfer queues
        uploadQueue = queue.Queue()
        resultQueue = queue.Queue()

        # put uploads to queue, create cancel events for jobs
        jobsdict = {}
        for job in jobs:
            uploads = self.getInputUploads(job)
            if job.errors:
                self.logger.debug(f"Skipping job {job.id} due to input file errors")
                continue

            jobsdict[job.id] = job
            job.cancelEvent = threading.Event()

            for upload in uploads:
                uploadQueue.put(upload)
        if uploadQueue.empty():
            self.logger.debug("No local inputs to upload")
            return
        numWorkers = min(uploadQueue.qsize(), workers)

        # create HTTP clients for workers
        httpClients = []
        for i in range(numWorkers):
            httpClients.append(HTTPClient(
                host=self.httpClient.host,
                port=self.httpClient.port,
                isHTTPS=True,
                proxypath=self.httpClient.proxypath,
                logger=self.logger
            ))

        self.logger.debug(f"Created {len(httpClients)} upload workers")

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
                    logger=self.logger
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
    def fetchJobs(self, downloadDir, jobs, workers=10, blocksize=HTTP_BUFFER_SIZE):
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
                "url": f"{self.basePath}/jobs/{job.id}/session",
                "path": "",
                "type": "listing"
            })

        # open connections for thread workers
        httpClients = []
        for i in range(workers):
            httpClients.append(HTTPClient(
                host=self.httpClient.host,
                port=self.httpClient.port,
                isHTTPS=True,
                proxypath=self.httpClient.proxypath,
                logger=self.logger
            ))

        self.logger.debug(f"Created {len(httpClients)} download workers")

        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
            futures = []
            for httpClient in httpClients:
                futures.append(pool.submit(
                    downloadTransferWorker,
                    httpClient,
                    transferQueue,
                    resultQueue,
                    downloadDir,
                    jobsdict,
                    self.basePath,
                    logger=self.logger
                ))
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
            self.logger.debug(f"No files to download for job {job.id}")
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
                    self.logger.debug(f"Skipping empty download entry: {download}")
                    continue  # error?

                # add all files if entire log folder is specified
                if diagnose.endswith("/"):
                    self.logger.debug(f"Will download all diagnose files to {diagnose}")
                    for diagFile in DIAG_FILES:
                        diagFiles.add(f"{diagnose}{diagFile}")

                else:
                    diagFile = diagnose.split("/")[-1]
                    if diagFile not in DIAG_FILES:
                        self.logger.debug(f"Skipping download {download} for because of unknown diagnose file {diagFile}")
                        continue  # error?
                    self.logger.debug(f"Will download diagnose file {diagFile} to {download}")
                    diagFiles.add(diagnose)
            else:
                self.logger.debug(f"Will download {download}")
                newDownloads.append(download)

        for diagFile in diagFiles:
            diagName = diagFile.split("/")[-1]
            transferQueue.put({
                "jobid": job.id,
                "url": f"{self.basePath}/jobs/{job.id}/diagnose/{diagName}",
                "path": diagFile,
                "type": "diagnose"
            })

        job.downloadFiles = newDownloads

    def getJobsList(self):
        resp = self.httpClient.request(
            "GET",
            f"{self.basePath}/jobs",
            headers={"Accept": "application/json"}
        )
        respstr = resp.read().decode()

        self.logger.debug(f"Jobs list response - {resp.status} {respstr}")

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
                job.errors.append(NoValueInARCResult("No info document in successful info response"))
            else:
                job.updateFromInfo(result["info_document"])

    def getJobsStatus(self, jobs):
        results = self.manageJobs(jobs, "status")
        for job, result in zip(jobs, results):
            code, reason = int(result["status-code"]), result["reason"]
            if code != 200:
                job.errors.append(ARCHTTPError(code, reason, f"{code} {reason}"))
            elif "state" not in result:
                job.errors.append(NoValueInARCResult("No state in successful status response"))
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
                job.errors.append(NoValueInARCResult("No delegation ID in successful response"))
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
            f"{self.basePath}/jobs?action={action}",
            jsonData=jsonData,
            headers={"Accept": "application/json", "Content-type": "application/json"}
        )
        respstr = resp.read().decode()

        self.logger.debug(f"Jobs manage response - {resp.status} {respstr}")

        if resp.status != 201:
            raise ARCHTTPError(resp.status, respstr, f"ARC jobs \"{action}\" action error: {resp.status} {respstr}")
        jsonData = json.loads(respstr)

        # convert data to list
        if isinstance(jsonData["job"], dict):
            return [jsonData["job"]]
        else:
            return jsonData["job"]


class ARCJob:

    def __init__(self, id=None, descstr=None):
        self.id = id
        self.descstr = descstr
        self.name = None
        self.delegid = None
        self.state = None
        self.tstate = None
        self.cancelEvent = None
        self.errors = []
        self.downloadFiles = []
        self.inputFiles = {}

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
        raise InputFileError(f"Error parsing source {path} of file {name}: {exc}")
    if url.scheme not in ("file", None, "") or url.hostname:
        return ""

    return url.path


def uploadTransferWorker(httpClient, jobsdict, uploadQueue, resultQueue, logger=None):
    if not logger:
        logger = logging.getLogger('null')
        if not logger.hasHandlers():
            logger.addHandler(logging.NullHandler())

    while True:
        try:
            upload = uploadQueue.get(block=False)
        except queue.Empty:
            break
        uploadQueue.task_done()

        job = jobsdict[upload["jobid"]]
        if job.cancelEvent.is_set():
            logger.debug(f"Skipping upload for cancelled job {upload['jobid']}")
            continue

        try:
            infile = open(upload["path"], "rb")
        except Exception as exc:
            job.cancelEvent.set()
            resultQueue.put({
                "jobid": upload["jobid"],
                "error": exc
            })
            logger.debug(f"Error opening input file {upload['path']} for job {upload['jobid']}: {exc}")
            continue

        with infile:
            try:
                resp = httpClient.request("PUT", upload["url"], data=infile)
                text = resp.read().decode()
                logger.debug(f"Upload of input {upload['path']} to {upload['url']} for job {upload['jobid']} - {resp.status} {text}")
                if resp.status != 200:
                    job.cancelEvent.set()
                    msg = f"Upload {upload['path']} to {upload['url']} for job {upload['jobid']} failed: {resp.status} {text}"
                    resultQueue.put({
                        "jobid": upload["jobid"],
                        "error": ARCHTTPError(resp.status, text, msg)
                    })
            except Exception as exc:
                job.cancelEvent.set()
                resultQueue.put({
                    "jobid": upload["jobid"],
                    "error": exc
                })
                logger.debug(f"Upload {upload['path']} to {upload['url']} for job {upload['jobid']} failed: {exc}")


def downloadTransferWorker(httpClient, transferQueue, resultQueue, downloadDir, jobsdict, endpoint, logger=None):
    if not logger:
        logger = logging.getLogger('null')
        if not logger.hasHandlers():
            logger.addHandler(logging.NullHandler())

    while True:
        try:
            transfer = transferQueue.get()
        except TransferQueueEmpty():
            break

        job = jobsdict[transfer["jobid"]]
        if job.cancelEvent.is_set():
            logger.debug(f"Skipping download for cancelled job {transfer['jobid']}")
            continue

        try:
            if transfer["type"] in ("file", "diagnose"):
                # download file
                path = f"{downloadDir}/{transfer['jobid']}/{transfer['path']}"
                try:
                    downloadFile(httpClient, transfer["url"], path)
                except ARCHTTPError as exc:
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
                    logger.debug(f"Download {transfer['url']} to {path} for job {transfer['jobid']} failed: {error}")
                except Exception as exc:
                    job.cancelEvent.set()
                    resultQueue.put({
                        "jobid": transfer["jobid"],
                        "error": exc
                    })
                    logger.debug(f"Download {transfer['url']} to {path} for job {transfer['jobid']} failed: {exc}")
                else:
                    logger.debug(f"Download {transfer['url']} to {path} for job {transfer['jobid']} successful")

            elif transfer["type"] == "listing":
                # download listing
                try:
                    listing = downloadListing(httpClient, transfer["url"])
                except ARCHTTPError as exc:
                    resultQueue.put({
                        "jobid": transfer["jobid"],
                        "error": exc
                    })
                    logger.debug(f"Download listing {transfer['url']} for job {transfer['jobid']} failed: {exc}")
                except Exception as exc:
                    job.cancelEvent.set()
                    resultQueue.put({
                        "jobid": transfer["jobid"],
                        "error": exc
                    })
                    logger.debug(f"Download listing {transfer['url']} for job {transfer['jobid']} failed: {exc}")
                else:
                    # create new transfer jobs
                    transfers = createTransfersFromListing(
                        job.downloadFiles, endpoint, listing, transfer["path"], transfer["jobid"]
                    )
                    for transfer in transfers:
                        transferQueue.put(transfer)
                    logger.debug(f"Download listing {transfer['url']} for job {transfer['jobid']} successful: {listing}")
        except:
            import traceback
            excstr = traceback.format_exc()
            job.cancelEvent.set()
            resultQueue.put({
                "jobid": transfer["jobid"],
                "error": excstr
            })
            logger.debug(f"Download URL {transfer['url']} and path {transfer['path']} for job {transfer['jobid']} failed: {excstr}")


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


def createTransfersFromListing(downloadFiles, endpoint, listing, path, jobid):
    transfers = []
    # create new transfer jobs
    if "file" in listing:
        if not isinstance(listing["file"], list):
            listing["file"] = [listing["file"]]
        for f in listing["file"]:
            if path:
                newpath = f"{path}/{f}"
            else:  # if session root, slash needs to be skipped
                newpath = f
            if not filterOutFile(downloadFiles, newpath):
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
            if not filterOutListing(downloadFiles, newpath):
                transfers.append({
                    "jobid": jobid,
                    "type": "listing",
                    "path": newpath,
                    "url": f"{endpoint}/jobs/{jobid}/session/{newpath}"
                })
    return transfers
