import logging
import os
import shutil
from datetime import datetime
from urllib.parse import urlparse

import arc
import jwt
from act.client.errors import (ConfigError, InvalidColumnError,
                               InvalidJobIDError, InvalidJobRangeError,
                               RESTError, UnknownClusterError)
from act.client.jobmgr import JobManager, getIDsFromList
from act.client.proxymgr import ProxyManager, getVOMSProxyAttributes
from act.common.aCTConfig import aCTConfigAPP, aCTConfigARC
from act.db.aCTDBMS import getDB
from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from flask import Flask, jsonify, request, send_file
from pyarcrest.arc import isLocalInputFile
from pyarcrest.x509 import checkRFCProxy, createProxyCSR
from werkzeug.exceptions import BadRequest, UnsupportedMediaType

# TODO: see if checkJobExists should be used anywhere else
# TODO: implement proper logging
# TODO: HTTP return codes
# TODO: consistently change relevant jobmgr API to return jobs as dicts
#       rather than list of IDs
# TODO: API should not return underscored column names for as many endpoints
#       possible
# TODO: can app context be used for global variables?


STREAM_CHUNK_SIZE = 4096
appconf = aCTConfigAPP()
arcconf = aCTConfigARC()
logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
db = getDB(logger, arcconf)
pmgr = ProxyManager(db=db)
jmgr = JobManager(db=db)


# can only be called after appconf global var exists
def parseClusters():
    clusters = []
    for cluster in appconf.user.clusters:
        try:
            parts = urlparse(cluster, scheme="https")
        except Exception as exc:
            raise Exception(f"Error parsing cluster URL {cluster}: {exc}")

        scheme = parts.scheme
        host = parts.hostname
        port = parts.port
        path = parts.path

        if scheme != "https":
            raise Exception(f"Cluster URL {cluster} not using HTTPS")
        if host is None:
            raise Exception(f"Cluster URL {cluster} has no host")
        if port is None:
            port = 443

        clusters.append(f"https://{host}:{port}{path}")

    return clusters


clusters = parseClusters()


app = Flask(__name__)


@app.route('/jobs', methods=['GET'])
def stat():
    '''
    Return status info for jobs in JSON format.

    There are several parameters that can be given in URL. Possible
    filtering parameters are:
        'id': a list of job IDs
        'name': a substring that has to be present in job names
        'state': state that jobs have to be in

    There are also two parameters that define which attributes should be
    returned:
        'client': a list of column names from client table
        'arc': a list of column names from arc table

    Returns:
        status 200: A JSON list of JSON objects with jobs' status info.
        status 4**: A string with error message.
    '''
    name_filter = request.args.get('name', default='')
    state_filter = request.args.get('state', default='')
    clicols = request.args.get('client', default=[])
    if clicols:
        clicols = clicols.split(',')
    arccols = request.args.get('arc', default=[])
    if arccols:
        arccols = arccols.split(',')

    try:
        token = getToken()
        proxyid = token['proxyid']
        jobids = getIDs()
        jobdicts = jmgr.getJobStats(proxyid, jobids, state_filter, name_filter, clicols, arccols)
    except InvalidColumnError as e:
        print(f'error: GET /jobs: {e}')
        return {'msg': str(e)}, 400
    except RESTError as e:
        print(f'error: GET /jobs: {e}')
        return {'msg': str(e)}, e.httpCode
    except Exception as e:
        print(f'error: GET /jobs: {e}')
        return {'msg': 'Server error'}, 500
    else:
        return jsonify(jobdicts)


@app.route('/jobs', methods=['DELETE'])
def clean():
    '''
    Clean jobs that satisfy parameters in current request context.

    Parameters are given in request URL, they are:
        'id': a list of job IDs
        'name': a substring that has to be present in job names
        'state': state that jobs have to be in

    Returns:
        status 200: A string with number of cleaned jobs.
        status 401: A string with error message.
    '''
    try:
        token = getToken()
        jobids = getIDs()
    except RESTError as e:
        print(f'error: DELETE /jobs: {e}')
        return {'msg': str(e)}, e.httpCode
    proxyid = token['proxyid']

    name_filter = request.args.get('name', default='')
    state_filter = request.args.get('state', default='')

    try:
        deleted = jmgr.cleanJobs(proxyid, jobids, state_filter, name_filter)
        for jobid in deleted:
            datadir = jmgr.getJobDataDir(jobid)
            shutil.rmtree(jmgr.getJobDataDir(datadir), ignore_errors=True)
    except Exception as e:
        print(f'error: DELETE /jobs: {e}')
        return {'msg': 'Server error'}, 500

    return jsonify(deleted)


# expects JSON object with 'state' attribute:
# { "state": "fetch|cancel|resubmit" }
@app.route('/jobs', methods=['PATCH'])
def patch():
    '''
    Set jobs' state based on request parameters.

    Parameters that are passed in URL:
        'id': a list of job IDs
        'name': a substring that has to be present in job names
        'state': state that jobs have to be in
        'action': what action should be performed on jobs
            (fetch|cancel|resubmit)

    Returns:
        status 200: A string with a number of affected jobs.
        status 4**: A string with error message.
    '''
    try:
        token = getToken()
        jobids = getIDs()
    except BadRequest as e:
        print(f'error: PATCH /jobs: {e}')
        return {'msg': str(e)}, 400
    except RESTError as e:
        print(f'error: PATCH /jobs: {e}')
        return {'msg': str(e)}, e.httpCode
    proxyid = token['proxyid']

    name_filter = request.args.get('name', default='')
    state_filter = request.args.get('state', default='')

    action = request.args.get('action', None)
    if action is None:
        return {'msg': 'Request has no action parameter'}, 400
    elif action not in ('fetch', 'cancel', 'resubmit'):
        return {'msg': f'Invalid action "{action}"'}, 400

    try:
        if action == 'fetch':
            jobs = jmgr.fetchJobs(proxyid, jobids, name_filter)
        elif action == 'cancel':
            # One state in which a job can be killed is before it is passed
            # to ARC. Such jobs have None as arcid. Data dirs for jobs are
            # otherwise cleaned by cleaning operation but this is one exception
            # where killing destroys the job immediately and has to remove the
            # data dir as well.
            jobs = jmgr.killJobs(proxyid, jobids, state_filter, name_filter)
            for job in jobs:
                if job['a_id'] is None or job['a_arcstate'] in ('tosubmit', 'submitting'):
                    datadir = jmgr.getJobDataDir(job['c_id'])
                    shutil.rmtree(jmgr.getJobDataDir(datadir), ignore_errors=True)
        elif action == 'resubmit':
            jobs = jmgr.resubmitJobs(proxyid, jobids, name_filter)
    except Exception as e:
        print(f'error: PATCH /jobs: {e}')
        return {'msg': 'Server error'}, 500
    return jsonify(jobs)


# expects a JSON list of job objects in the following form:
# [
#   {
#     "clusterlist": "<list of clusters>"
#   },
#   {
#     "clusterlist": "<list of clusters>"
#   },
#   ...
# ]
@app.route('/jobs', methods=['POST'])
def create_jobs():
    errpref = 'error: POST /jobs: '
    try:
        token = getToken()
        jobs = request.get_json()
    except RESTError as e:
        print(f'{errpref}{e}')
        return {'msg': str(e)}, e.httpCode
    except (BadRequest, UnsupportedMediaType) as e:  # raised for invalid JSON
        print(f'{errpref}{e}')
        return {'msg': str(e)}, 400
    except Exception as e:
        print(f'{errpref}{e}')
        return {'msg': 'Server error'}, 500

    if not jobs:
        return jsonify([])

    results = []
    for job in jobs:
        result = {}
        results.append(result)

        try:
            # check clusters
            if 'clusterlist' not in job or not job['clusterlist']:
                print(f'{errpref}No clusters given')
                result['msg'] = 'No clusters given'
                continue
            clusterlist = checkClusters(job['clusterlist'])

            # insert job
            jobid = jmgr.clidb.insertJob(token['proxyid'], ','.join(clusterlist))
        except UnknownClusterError as e:
            print(f'{errpref}Unknown cluster {e.name}')
            result['msg'] = f'Unknown cluster {e.name}'
            continue
        except Exception as e:
            print(f'{errpref}{e}')
            result['msg'] = 'Server error'
            continue

        result['id'] = jobid

    return jsonify(results)


# expects a JSON list of job objects in the following form:
# [
#   {
#     "desc": "<xRSL or ADL>",
#     "id": "<job ID>"
#   },
#   {
#     "desc": "<xRSL or ADL>",
#     "id": "<job ID>"
#   },
#   ...
# ]
@app.route('/jobs', methods=['PUT'])
def confirm_jobs():
    errpref = 'error: PUT /jobs: '
    try:
        token = getToken()
        proxyid = token['proxyid']
        submissions = request.get_json()
    except (BadRequest, UnsupportedMediaType) as e:
        print(f'{errpref}{e}')
        return {'msg': str(e)}, 400
    except RESTError as e:
        print(f'{errpref}{e}')
        return {'msg': str(e)}, e.httpCode
    except Exception as e:
        print(f'{errpref}{e}')
        return {'msg': 'Server error'}, 500

    if not submissions:
        return jsonify([])
    elif not isinstance(submissions, list):
        print(f'{errpref}Input JSON is not a list: {submissions}')
        return {'msg': 'Input JSON is not a list: {submissions}'}, 400

    jobs = []
    jobids = []
    tocheck = []
    for submission in submissions:
        job = {}
        jobs.append(job)
        if not isinstance(submission, dict):
            print(f'{errpref}Job element is not an object: {submission}')
            job['msg'] = f'Job element is not an object: {submission}'
        elif 'id' not in submission:
            print(f'{errpref}No job ID given')
            job['msg'] = 'No job ID given'
        else:
            job.update(submission)
            jobids.append(job['id'])
            tocheck.append(job)

    # get info for all jobs and check which ones don't exist
    tosubmit = []
    stats = jmgr.getJobStats(proxyid, jobids, '', '', ['id',], [], '')
    for job in tocheck:
        inStats = False
        for stat in stats:
            if stat['c_id'] == job['id']:
                inStats = True
        if not inStats:
            print(f'{errpref}Job ID {job["id"]} does not exist')
            job['msg'] = f'Job ID {job["id"]} does not exist'
        else:
            tosubmit.append(job)

    jobdescs = arc.JobDescriptionList()

    for job in tosubmit:

        # parse job description
        if 'desc' not in job:
            print(f'{errpref}No job description given')
            job['msg'] = 'No job description given'
            continue
        if not arc.JobDescription.Parse(job['desc'], jobdescs):
            print(f'{errpref}Invalid job description')
            job['msg'] = 'Invalid job description'
            continue

        job['name'] = jobdescs[-1].Identification.JobName

        # get job's data directory
        try:
            jobDataDir = jmgr.getJobDataDir(job['id'])
        except ConfigError as e:
            print(f'{errpref}{e}')
            job['msg'] = 'Server error'
            continue

        # modify job description for local input files
        #
        # InputFiles need to be accessed through index otherwise
        # the changes do not survive outside of for loop.
        for i in range(len(jobdescs[-1].DataStaging.InputFiles)):
            filename = jobdescs[-1].DataStaging.InputFiles[i].Name
            filepath = isLocalInputFile(
                jobdescs[-1].DataStaging.InputFiles[i].Name,
                jobdescs[-1].DataStaging.InputFiles[i].Sources[0].fullstr()
            )
            if not filepath:  # remote file
                continue

            path = os.path.abspath(os.path.join(jobDataDir, filename))
            if not os.path.isfile(path):
                job['msg'] = f'Input file {filepath} missing'
                break

            jobdescs[-1].DataStaging.InputFiles[i].Sources[0].ChangeFullPath(path)

        # errors on missing input files
        if 'msg' in job:
            print(f'{errpref}{job["msg"]}')
            continue

        # TODO: ADL unparsing works but it doesn't unparse modified
        # input files
        desc = jobdescs[-1].UnParse('nordugrid:xrsl')[1]
        #desc = jobdescs[0].UnParse('emies:adl')[1]
        if not arc.JobDescription.Parse(desc, jobdescs):
            print(f'{errpref}Invalid modified job description')
            job['msg'] = 'Server error'
            continue

        # update job entry and confirm job for submission
        try:
            jmgr.clidb.updateJob(job['id'], {
                'jobdesc': desc,
                'jobname': job['name'],
                'modified': jmgr.clidb.getTimeStamp()
            })
        except Exception as e:
            print(f'{errpref}{e}')
            job['msg'] = 'Server error'
            continue

        del job['desc']  # don't want to return description in result

    return jsonify(jobs)


@app.route('/jobs/<int:jobid>/results/', defaults={'path': ''}, methods=['GET'])
@app.route('/jobs/<int:jobid>/results/<path:path>', methods=['GET'])
def serveResults(jobid, path):
    try:
        token = getToken()
    except RESTError as e:
        return {'msg': str(e)}, e.httpCode
    proxyid = token['proxyid']

    try:
        # The next two 404 conditions are an example how an error scheme is
        # required that is different and separate from HTTP status codes, since
        # it is impossible to deduce the error without comparing the string.
        # The same goes for the error cases of files or dirs that do not exist
        # in later parts of the code.
        results = jmgr.getJobs(proxyid, [jobid])
        if not results.jobdicts:
            return {'msg': 'No results in current job state'}, 404
        resultDir = results.jobdicts[0]['dir']
        if not resultDir:
            return {'msg': 'Job has no results'}, 404

        if path == '' or path.endswith('/'):
            dirPath = os.path.join(resultDir, path)
            if not os.path.isdir(dirPath):
                return {'msg': 'Output directory does not exist'}, 404
            listing = os.listdir(dirPath)

            files = []
            dirs = []
            for entry in listing:
                entryPath = os.path.join(dirPath, entry)
                if os.path.isfile(entryPath):
                    files.append(entry)
                elif os.path.isdir(entryPath):
                    dirs.append(entry)
            return jsonify({'file': files, 'dir': dirs})
        else:
            filePath = os.path.join(resultDir, path)
            if not os.path.isfile(filePath):
                return {'msg': 'Output file does not exist'}, 404
            return send_file(filePath)

    except Exception as e:
        print(f'error: GET /jobs/{jobid}/results/{path}: {e}')
        return {'msg': 'Server error'}, 500


@app.route('/proxies', methods=['POST'])
def getCSR():
    # get issuer cert string from request
    try:
        jsonData = request.get_json()
    except (BadRequest, UnsupportedMediaType) as e:
        print(f'error: POST /proxies: {e}')
        return {'msg': str(e)}, 400
    except Exception as e:
        print(f'error: POST /proxies: creating proxy manager: {e}')
        return {'msg': 'Server error'}, 500

    if not jsonData:
        print('error: POST /proxies: No JSON data')
        return {'msg': 'No JSON data'}, 400
    else:
        issuer_pem = jsonData.get('cert', None)
        if not issuer_pem:
            print('error: POST /proxies: missing issuer certificate')
            return {'msg': 'Missing issuer certificate'}, 400
        chain_pem = jsonData.get('chain', None)
        if not chain_pem:
            print('error: POST /proxies: missing certificate chain')
            return {'msg': 'Missing certificate chain'}, 400

    dn, exptime = pmgr.readProxyString(issuer_pem)
    if datetime.utcnow() >= exptime:
        print('error: POST /proxies: expired certificate')
        return {'msg': 'Given certificate is expired'}, 400
    attr = getVOMSProxyAttributes(issuer_pem, chain_pem)
    if not attr or not dn:
        print('error: POST /proxies: DN or VOMS attribute extraction failure')
        return {'msg': 'Failed to extract DN or VOMS attributes'}, 400

    try:
        # load certificate string and check proxy
        issuer = x509.load_pem_x509_certificate(issuer_pem.encode("utf-8"), default_backend())
        if not checkRFCProxy(issuer):
            print('error: POST /proxies: issuer cert is not a valid proxy')
            return {'msg': 'Issuer cert is not a valid proxy'}, 400

        # generate private key for delegated proxy
        private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048,
            backend=default_backend()
        )

        # generate CSR
        csr = createProxyCSR(issuer, private_key)
        print(f'CSR generated: DN: {dn}, attr: {attr}, expiration: {exptime}')

        # put private key into string and store in db
        pem = private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption()
        ).decode('utf-8')
        proxyid = pmgr.actproxy.updateProxy(pem, dn, attr, exptime)
        if proxyid is None:
            print('error: POST /proxies: proxy insertion failure')
            return {'msg': 'Server error'}, 500

        # generate CSR string and auth token
        csr_pem = csr.public_bytes(serialization.Encoding.PEM).decode('utf-8')
        token = jwt.encode({'proxyid': proxyid, 'exp': exptime}, appconf.user.jwt_secret, algorithm='HS256')
    except Exception as e:
        print(f'error: POST /proxies: {e}')
        return {'msg': 'Server error'}, 500

    return {'token': token, 'csr': csr_pem}, 200


@app.route('/proxies', methods=['PUT'])
def uploadSignedProxy():
    try:
        token = getToken()
        jsonData = request.get_json()
    except RESTError as e:
        print(f'error: PUT /proxies: {e}')
        return {'msg': str(e)}, e.httpCode
    except (BadRequest, UnsupportedMediaType) as e:
        print(f'error: PUT /proxies: {e}')
        return {'msg': str(e)}, 400
    except Exception as e:
        print(f'error: PUT /proxies: {e}')
        return {'msg': 'Server error'}, 500

    if not jsonData:
        print('error: PUT /proxies: No JSON data')
        return {'msg': 'No JSON data'}, 400

    proxyid = token['proxyid']
    cert_pem = jsonData.get('cert', None)
    chain_pem = jsonData.get('chain', None)
    if cert_pem is None:
        print('error: PUT /proxies: No signed certificate')
        return {'msg': 'No signed certificate'}, 400
    if chain_pem is None:
        print('error: PUT /proxies: No cert chain')
        return {'msg': 'No cert chain'}, 400

    key_pem = pmgr.getProxyKeyPEM(proxyid)
    dn, exptime = pmgr.readProxyString(cert_pem)
    if datetime.utcnow() >= exptime:
        return {'msg': 'Given certificate is expired'}, 400
    attr = getVOMSProxyAttributes(cert_pem, chain_pem)
    if not attr or not dn:
        return {'msg': 'Failed to extract DN or VOMS attributes'}, 400
    proxy_pem = cert_pem + key_pem.decode('utf-8') + chain_pem

    try:
        proxy_obj = x509.load_pem_x509_certificate(proxy_pem.encode('utf-8'), backend=default_backend())
        if not checkRFCProxy(proxy_obj):
            return {'msg': 'cert is not a valid proxy'}, 400
        proxyid = pmgr.actproxy.updateProxy(proxy_pem, dn, attr, exptime)
        token = jwt.encode({'proxyid': proxyid, 'exp': exptime}, appconf.user.jwt_secret, algorithm='HS256')
    except Exception as e:
        print(f'error: PUT /proxies: {e}')
        return {'msg': 'Server error'}, 500

    print(f'Proxy submitted: DN: {dn}, attr: {attr}, expiration: {exptime}')
    return {'token': token}, 200


@app.route('/proxies', methods=['DELETE'])
def deleteProxy():
    try:
        token = getToken()
        pmgr.arcdb.deleteProxy(token['proxyid'])
    except RESTError as e:
        print(f'error: DELETE /proxies: {e}')
        return {'msg': str(e)}, e.httpCode
    except Exception as e:
        print(f'error: DELETE /proxies: {e}')
        return {'msg': 'Server error'}, 500
    return '', 204


@app.route('/jobs/<int:jobid>/data/<path:path>', methods=['PUT'])
def uploadFile(jobid, path):
    try:
        token = getToken()
    except RESTError as e:
        print(f'error: PUT /jobs/{jobid}/data/{path}: {e}')
        return {'msg': str(e)}, e.httpCode
    except Exception as e:
        print(f'error: PUT /jobs/{jobid}/data/{path}: {e}')
        return {'msg': 'Server error'}, 500
    proxyid = token['proxyid']

    try:
        jobid = jmgr.checkJobExists(proxyid, jobid)
        if not jobid:
            print(f'error: PUT /jobs/{jobid}/data/{path}: job ID does not exist')
            return {'msg': f'Client Job ID {jobid} does not exist'}, 400
    except Exception as e:
        print(f'error: PUT /jobs/{jobid}/data/{path}: {e}')
        return {'msg': 'Server error'}, 500

    try:
        jobDataDir = jmgr.getJobDataDir(jobid)
        filepath = os.path.join(jobDataDir, path)
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, 'wb') as f:
            while True:
                chunk = request.stream.read(STREAM_CHUNK_SIZE)
                if len(chunk) == 0:
                    break
                f.write(chunk)
    except Exception as e:
        print(f'error: PUT /jobs/{jobid}/data/{path}: {e}')
        return {'msg': 'Server error'}, 500

    return '', 204


@app.route('/info', methods=['GET'])
def info():
    try:
        getToken()

        json = {'clusters': appconf.user.clusters}

        c = jmgr.arcdb.db.getCursor()
        c.execute('SHOW COLUMNS FROM arcjobs')
        rows = c.fetchall()
        json['arc'] = [row['Field'] for row in rows]
        c.execute('SHOW COLUMNS FROM clientjobs')
        rows = c.fetchall()
        json['client'] = [row['Field'] for row in rows]
        c.close()

    except RESTError as e:
        print(f'error: GET /info: {e}')
        return {'msg': str(e)}, e.httpCode
    except Exception as e:
        print(f'error: GET /info: {e}')
        return {'msg': 'Server error'}, 500
    return jsonify(json)


def getIDs():
    '''
    Get IDs from current request context.

    IDs are taken from 'id' url parameter. Exceptions for getJobsFromList
    are handled by callers so they can generate appropriate responses.
    '''
    ids = request.args.get('id', default=[])
    if ids:
        try:
            return getIDsFromList(ids)
        except InvalidJobIDError as e:
            raise RESTError(f'Invalid job ID: {e.jobid}', 400)
        except InvalidJobRangeError as e:
            raise RESTError(f'Invalid job range: {e.jobRange}', 400)
    else:
        return []


def getToken():
    '''
    Raises:
        RESTError: Token is not present or is expired
    '''
    tokstr = request.headers.get('Authorization', None)
    if tokstr is None:
        raise RESTError('Auth token is missing', 401)
    try:
        # potential errors with tokstr, token decode, proxy manager ...
        tokstr = tokstr.split()[1]
        token = jwt.decode(tokstr, appconf.user.jwt_secret, algorithms=['HS256'])
        result = pmgr.checkProxyExists(token['proxyid'])
        if result is None:
            raise RESTError('Server error', 500)
        if result is False:
            raise RESTError('Proxy from token does not exist in database or is expired', 401)
    except jwt.ExpiredSignatureError:
        raise RESTError('Auth token is expired', 401)
    except jwt.InvalidSignatureError:
        raise RESTError('Invalid token signature', 401)
    else:
        return token


def checkClusters(clusterlist):
    clist = []
    for cluster in clusterlist:
        try:
            parts = urlparse(cluster, scheme="https")
        except Exception:
            raise UnknownClusterError(cluster)

        if parts.scheme != "https":
            raise UnknownClusterError(cluster)

        host = parts.hostname
        port = parts.port
        if port is None:
            port = 443

        url = f"https://{host}:{port}{parts.path}"

        if url not in clusters:
            raise UnknownClusterError(cluster)

        clist.append(url)
    return clist
