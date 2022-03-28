import http.client
import json
import os
import ssl
import concurrent.futures
import queue
import arc
import logging
from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from urllib.parse import urlencode, urlparse

from act.client.x509proxy import sign_request
from act.client.delegate_proxy import parse_issuer_cred
from act.common.exceptions import ACTError, SubmitError


HTTP_BUFFER_SIZE = 2**23


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
        resp = conn.getresponse()
    except http.client.HTTPException as e:
        raise ACTError(f'Request error: {e}')
    except ConnectionError as e:
        raise ACTError(f'Connection error: {e}')
    except ssl.SSLError as e:
        raise ACTError(f'SSL error: {e}')

    return resp


def createDelegation(conn, endpoint, proxypath):
    try:
        resp = httpRequest(conn, "POST", f"{endpoint}/delegations?action=new", headers={"Accept": "application/json"})
        respstr = resp.read().decode()
    except (ACTError, http.client.HTTPException, ConnectionError) as e:
        raise ACTError(f"Could not start delegation process: {e}")

    if resp.status != 201:
        raise ACTError(f"Could not start delegation process: {resp.status} {respstr}")

    try:
        delegationID = resp.getheader('Location').split('/')[-1]
        with open(proxypath) as f:
            proxyStr = f.read()
        proxyCert, _, issuerChains = parse_issuer_cred(proxyStr)
        chain = proxyCert.public_bytes(serialization.Encoding.PEM).decode() + issuerChains + '\n'
        csr = x509.load_pem_x509_csr(respstr.encode(), default_backend())
        cert = sign_request(csr, proxypath).decode()
        pem = (cert + chain).encode()
        resp = httpRequest(conn, 'PUT', f'{endpoint}/delegations/{delegationID}', body=pem, headers={'Content-type': 'application/x-pem-file'})
        respStr = resp.read().decode()
        if resp.status != 200:
            raise Exception(f"Error response for signed cert upload for proxy {proxypath} and delegation {delegationID}: {resp.status} {respStr}")
    except Exception as error:
        msg = f"Delegation error: {error}"
        try:
            deleteDelegation(conn, endpoint, delegationID)
        except ACTError as anotherError:
            raise ACTError(f'{msg}\n{anotherError}')
        raise ACTError(msg)
    return delegationID


def deleteDelegation(conn, endpoint, delegationID):
    try:
        resp = httpRequest(conn, 'POST', f'{endpoint}/delegations/{delegationID}?action=delete')
        respstr = resp.read().decode()
    except (ACTError, http.client.HTTPException, ConnectionError) as e:
        raise ACTError("Cannot delete delegation {delegationID}: {e}")
    if resp.status != 200:
        raise ACTError(f'Cannot delete delegation {delegationID}: {resp.status} {respstr}')


# TODO: refactor common code with createDelegation (2nd step of delegation process)
def renewDelegation(conn, endpoint, delegationID, proxypath):
    try:
        resp = httpRequest(conn, "POST", f"{endpoint}/delegations/{delegationID}?action=renew")
        respstr = resp.read().decode()
    except (ACTError, http.client.HTTPException, ConnectionError) as e:
        raise ACTError(f"Could not start delegation renewal process: {e}")

    if resp.status != 200:
        raise ACTError(f"Could not start delegaton renewal process: {resp.status} {respstr}")

    try:
        with open(proxypath) as f:
            proxyStr = f.read()
        proxyCert, _, issuerChains = parse_issuer_cred(proxyStr)
        chain = proxyCert.public_bytes(serialization.Encoding.PEM).decode() + issuerChains + '\n'
        csr = x509.load_pem_x509_csr(respstr.encode(), default_backend())
        cert = sign_request(csr, proxypath).decode()
        pem = (cert + chain).encode()
        resp = httpRequest(conn, 'PUT', f'{endpoint}/delegations/{delegationID}', body=pem, headers={'Content-type': 'application/x-pem-file'})
        respstr = resp.read().decode()
        if resp.status != 200:
            raise Exception(f"Error response for signed cert upload for proxy {proxypath} and delegation {delegationID}: {resp.status} {respstr}")
    except Exception as e:
        raise ACTError(f"Delegation renewal error: {e}")


def getProxySSLContext(proxypath):
    """Create SSL context authenticated with user's proxy certificate."""
    context = ssl.SSLContext(ssl.PROTOCOL_TLS)
    context.load_cert_chain(proxypath, keyfile=proxypath)
    return context


def getInputUploadJobs(jobid, jobdesc, endpoint, arcID):
    """Return a list of upload dicts."""
    uploadJobs = []
    for file in jobdesc.DataStaging.InputFiles:
        path = file.Sources[0].fullstr()
        if not path:
            path = file.Name

        # this is how we determine remote resource
        try:
            fileUrl = urlparse(path)
        except ValueError as e:
            raise ACTError("Error parsing source of file {file.Name}: {e}")
        if fileUrl.scheme not in ('file', None, '') or fileUrl.hostname:
            continue

        # some paths might be given in URL form so we will use path
        # element of path parsed as URL
        path = fileUrl.path
        if not os.path.isfile(path):
            raise ACTError(f"Input {path} is not a local file")

        uploadJobs.append({
            "id": jobid,
            "url": f"{endpoint}/jobs/{arcID}/session/{file.Name}",
            "path": path
        })

    return uploadJobs


# Modifies list of dicts returned by getArcJobsInfo and populated with
# job descriptions in "descstr" key.
#
# If ACTError is returned, all jobs should be put back to "tosubmit"
#
# TODO: should delegation be deleted on errors?
def submitJobs(conn, queue, proxypath, jobs, **kwargs):
    logger = kwargs.get("logger", logging.getLogger(__name__).addHandler(logging.NullHandler()))

    # get delegation for proxy
    try:
        delegationID = createDelegation(conn, "/arex/rest/1.0", proxypath)
    except ACTError as e:
        raise SubmitError("tosubmit", str(e))

    jobdescs = arc.JobDescriptionList()
    tosubmit = []  # sublist of jobs that will be submitted
    for job in jobs:
        job["delegation"] = delegationID

        # parse job description
        if not arc.JobDescription_Parse(job["descstr"], jobdescs):
            job["msg"] = "Failed to parse description"
            continue
        job["desc"] = jobdescs[-1]

        # add queue and delegation to job description and unparse
        job["desc"].Resources.QueueName = queue
        job["desc"].DataStaging.DelegationID = delegationID
        unparseResult = job["desc"].UnParse("emies:adl")
        if not unparseResult[0]:
            job["msg"] = "Could not modify description"
            continue

        # cut away xml version node
        descstart = unparseResult[1].find("<ActivityDescription")
        job["adl"] = unparseResult[1][descstart:]

        tosubmit.append(job)

    if len(tosubmit) == 1:
        bulkdesc = tosubmit[0]["adl"]
    else:
        bulkdesc = "<ActivityDescriptions>"
        for job in tosubmit:
            bulkdesc += job["adl"]
        bulkdesc += "</ActivityDescriptions>"

    # submit jobs to ARC
    try:
        resp = httpRequest(conn, "POST", "/arex/rest/1.0/jobs?action=new", body=bulkdesc, headers={"Accept": "application/json", "Content-type": "application/rsl"})
        respStr = resp.read().decode()
    except ACTError as e:
        raise SubmitError("tosubmit", f"ARC communication error: {e}")

    if resp.status != 201:
        raise SubmitError("tosubmit", f"ARC submit error: {resp.status} {respStr}")

    try:
        jsonData = json.loads(respStr)
    except json.decoder.JSONDecodeError as e:
        raise SubmitError("tocancel", f"Submission returned invalid JSON: {e}")

    # get a list of submission results
    if isinstance(jsonData["job"], dict):
        results = [jsonData["job"]]
    else:
        results = jsonData["job"]

    # process job submissions
    # toupload is a dictionary rather than list so that we can mark
    # jobs with failed uploads in constant time
    toupload = {}
    for job, result in zip(tosubmit, results):
        if int(result["status-code"]) != 201:
            job["msg"] = f"{result['status-code']} {result['reason']}"
        else:
            job["arcid"] = result["id"]
            job["state"] = result["state"]
            toupload[job["id"]] = job

            # create a list of upload dicts for job input files
            try:
                job["uploads"] = getInputUploadJobs(job["id"], job["desc"], "/arex/rest/1.0", job["arcid"])
            except ACTError as e:
                job["msg"] = str(e)

    # upload input files of successfully submitted jobs
    uploads = []
    for job in toupload.values():
        uploads.extend(job["uploads"])
    if uploads:
        # TODO: hardcoded workers
        try:
            results = uploadFiles(conn.host, conn.port, proxypath, uploads, 10)
        # this is error when upload connections cannot be created - jobs should
        # be cleaned from ARC and set "tosubmit"
        except ACTError as error:
            msg = f"Error uploading input files: {error}"
            try:
                cleanJobs(conn, toupload.values())
            except ACTError as anotherError:
                raise SubmitError("tosubmit", f"{msg}\nCould not clean jobs from ARC: {anotherError}")
            else:
                raise SubmitError("tosubmit", msg)

        for result in results:
            if not result["success"]:
                if toupload[result["id"]]["msg"]:  # aggregate upload errors
                    toupload[result["id"]]["msg"] += f"\n{result['msg']}"
                else:
                    toupload[result["id"]]["msg"] = result["msg"]

    return jobs


# TODO: blocksize is only added in python 3.7!!!!!!!
def uploadFiles(host, port, proxypath, uploads, workers, blocksize=HTTP_BUFFER_SIZE):
    numWorkers = min(len(uploads), workers)

    # open upload thread workers
    conns = []
    for i in range(numWorkers):
        try:
            context = ssl.SSLContext(ssl.PROTOCOL_TLS)
            context.load_cert_chain(proxypath, keyfile=proxypath)
            # TODO: python 3.7 blocksize
            conns.append(http.client.HTTPSConnection(host, port=port, context=context))
        except ssl.SSLError as e:
            raise ACTError(f"Could not create SSL context for proxy {proxypath}: {e}")
        except (http.client.HTTPException, ConnectionError) as e:
            raise ACTError(f"Could not connect to cluster {host}:{port}: {e}")

    # create transfer job queues
    # no timeout or queue.Full exception handlers needed as all this is done
    # in a single thread
    uploadQueue = queue.Queue()
    for upload in uploads:
        uploadQueue.put(upload)
    resultQueue = queue.Queue()

    # run upload threads on upload jobs
    with concurrent.futures.ThreadPoolExecutor(max_workers=numWorkers) as pool:
        futures = []
        for i in range(numWorkers):
            futures.append(pool.submit(fileUploader, conns[i], uploadQueue, resultQueue))
        concurrent.futures.wait(futures)

    # close upload thread workers
    for i in range(numWorkers):
        conns[i].close()

    # convert queue object to list
    results = []
    while not resultQueue.empty():
        results.append(resultQueue.get())
        resultQueue.task_done()
    return results


def fileUploader(conn, uploadQueue, resultQueue):
    while True:
        try:
            upload = uploadQueue.get(block=False)
        except queue.Empty:
            break

        with open(upload["path"], "rb") as file:
            # TODO: on certain connection errors the upload should be
            # repeated?
            try:
                resp = httpRequest(conn, "PUT", upload["url"], body=file)
                text = resp.read()
                if resp.status != 200:
                    resultQueue.put({
                        "id": upload["id"],
                        "success": False,
                        "msg": f"Upload {upload['path']} to {upload['url']} failed with {resp.status} {text}"
                    })
            except (http.client.HTTPException, ConnectionError, OSError) as e:
                resultQueue.put({
                    "id": upload["id"],
                    "success": False,
                    "msg": f"Upload {upload['path']} to {upload['url']} failed with {e}"
                })
            else:
                resultQueue.put({
                    "id": upload["id"],
                    "success": True
                })
        uploadQueue.task_done()


def getJobsInfo(conn, jobs):
    results = manageJobs(conn, jobs, "info")
    return getJobOperationResults(jobs, results, "info_document")


def getJobsStatus(conn, jobs):
    results = manageJobs(conn, jobs, "status")
    return getJobOperationResults(jobs, results, "state")


def killJobs(conn, jobs):
    results = manageJobs(conn, jobs, "kill")
    return checkJobOperation(jobs, results)


def cleanJobs(conn, jobs):
    results = manageJobs(conn, jobs, "clean")
    return checkJobOperation(jobs, results)


def restartJobs(conn, jobs):
    results = manageJobs(conn, jobs, "restart")
    return checkJobOperation(jobs, results)


def getJobsDelegations(conn, jobs):
    results = manageJobs(conn, jobs, "delegations")
    return getJobOperationResults(jobs, results, "delegation_id")


def checkJobOperation(jobs, results):
    for job, result in zip(jobs, results):
        if int(result["status-code"]) != 202:
            job["msg"] = f"{result['status-code']} {result['reason']}"
    return jobs


def getJobOperationResults(jobs, results, key):
    for job, result in zip(jobs, results):
        if int(result["status-code"]) != 200:
            job["msg"] = f"{result['status-code']} {result['reason']}"
        else:
            job[key] = result[key]
    return jobs


# requires a list of dictionary jobs:
# {
#   "arcid": ...,
# }
def manageJobs(conn, jobs, action):
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

    try:
        resp = httpRequest(
            conn,
            "POST",
            f"/arex/rest/1.0/jobs?action={action}",
            json=jsonData,
            headers={"Accept": "application/json", "Content-type": "application/json"}
        )
        respstr = resp.read().decode()
        if resp.status != 201:
            raise ACTError(f"ARC error response: {resp.status} {respstr}")
        jsonData = json.loads(respstr)
    except json.JSONDecodeError as e:
        raise ACTError(f"Could not parse JSON response: {e}")

    # convert data to list
    if isinstance(jsonData["job"], dict):
        return [jsonData["job"]]
    else:
        return jsonData["job"]
