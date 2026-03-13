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
from flask import Flask, jsonify, request, send_file
from pyarcrest.arc import isLocalInputFile
from pyarcrest.x509 import (checkRFCProxy, createProxyCSR, csrToPEM,
                            generateKey, keyToPEM, pemToCert)
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


app = Flask(__name__)


def process_request(wrapperFunction):
    token = getToken()

    args = {
        "proxyid": token['proxyid'],
        "jobids": getIDs(),
        "name_filter": request.args.get('name'),
        "state_filter": request.args.get('state'),
        "clicols": request.args.get('client'),
        "arccols": request.args.get('arc'),
    }

    if args["clicols"]:
        args["clicols"] = args["clicols"].split(',')

    if args["arccols"]:
        args["arccols"] = args["arccols"].split(',')

    return wrapperFunction(**args)


@app.route('/jobs', methods=['GET']) # id name state
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
    try:
        jobdicts = process_request(jmgr.getJobStats)
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
        deleted = process_request(jmgr.cleanJobs)
    except RESTError as e:
        print(f'error: DELETE /jobs: {e}')
        return {'msg': str(e)}, e.httpCode
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
        action = request.args.get('action', None)
        if action is None:
            return {'msg': 'Request has no action parameter'}, 400
        elif action not in ('fetch', 'cancel', 'resubmit'):
            return {'msg': f'Invalid action "{action}"'}, 400
        
        if action == 'fetch':
            jobs = process_request(jmgr.fetchJobs)
        elif action == 'cancel':
            jobs = process_request(jmgr.killJobs)
        elif action == 'resubmit':
            jobs = process_request(jmgr.resubmitJobs)
    except BadRequest as e:
        print(f'error: PATCH /jobs: {e}')
        return {'msg': str(e)}, 400
    except RESTError as e:
        print(f'error: PATCH /jobs: {e}')
        return {'msg': str(e)}, e.httpCode
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
        if not jobs:
            return jsonify([])
        results = jmgr.createJobs(token['proxyid'], jobs, errpref)
    except RESTError as e:
        print(f'{errpref}{e}')
        return {'msg': str(e)}, e.httpCode
    except (BadRequest, UnsupportedMediaType) as e:  # raised for invalid JSON
        print(f'{errpref}{e}')
        return {'msg': str(e)}, 400
    except Exception as e:
        print(f'{errpref}{e}')
        return {'msg': 'Server error'}, 500

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
        if not submissions:
            return jsonify([])
        elif not isinstance(submissions, list):
            print(f'{errpref}Input JSON is not a list: {submissions}')
            return {'msg': 'Input JSON is not a list: {submissions}'}, 400
        jobs = jmgr.confirmJobs(proxyid, submissions, errpref)
    except (BadRequest, UnsupportedMediaType) as e:
        print(f'{errpref}{e}')
        return {'msg': str(e)}, 400
    except RESTError as e:
        print(f'{errpref}{e}')
        return {'msg': str(e)}, e.httpCode
    except Exception as e:
        print(f'{errpref}{e}')
        return {'msg': 'Server error'}, 500

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
        issuerPEM = jsonData.get('cert', None)
        if not issuerPEM:
            print('error: POST /proxies: missing issuer certificate')
            return {'msg': 'Missing issuer certificate'}, 400
        chainPEM = jsonData.get('chain', None)
        if not chainPEM:
            print('error: POST /proxies: missing certificate chain')
            return {'msg': 'Missing certificate chain'}, 400

    dn, exptime = pmgr.readProxyString(issuerPEM)
    if datetime.utcnow() >= exptime:
        print('error: POST /proxies: expired certificate')
        return {'msg': 'Given certificate is expired'}, 400
    attr = getVOMSProxyAttributes(issuerPEM, chainPEM)
    if not attr or not dn:
        print('error: POST /proxies: DN or VOMS attribute extraction failure')
        return {'msg': 'Failed to extract DN or VOMS attributes'}, 400

    try:
        # load proxy string and check validity
        issuer = pemToCert(issuerPEM)
        if not checkRFCProxy(issuer):
            print('error: POST /proxies: issuer cert is not a valid proxy')
            return {'msg': 'Issuer cert is not a valid proxy'}, 400

        # generate private key for delegated proxy
        key = generateKey()

        # generate CSR
        csr = createProxyCSR(issuer, key)
        print(f'CSR generated: DN: {dn}, attr: {attr}, expiration: {exptime}')

        # put private key into string and store in db
        proxyid = pmgr.clidb.updateProxy(keyToPEM(key), dn, attr, exptime)
        if proxyid is None:
            print('error: POST /proxies: proxy insertion failure')
            return {'msg': 'Server error'}, 500

        # generate CSR string and auth token
        token = jwt.encode({'proxyid': proxyid, 'exp': exptime}, appconf.user.jwt_secret, algorithm='HS256')
    except Exception as e:
        print(f'error: POST /proxies: {e}')
        return {'msg': 'Server error'}, 500

    return {'token': token, 'csr': csrToPEM(csr)}, 200


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
    certPEM = jsonData.get('cert', None)
    chainPEM = jsonData.get('chain', None)
    if certPEM is None:
        print('error: PUT /proxies: No signed certificate')
        return {'msg': 'No signed certificate'}, 400
    if chainPEM is None:
        print('error: PUT /proxies: No cert chain')
        return {'msg': 'No cert chain'}, 400

    keyPEM = pmgr.getProxyKeyPEM(proxyid)
    dn, exptime = pmgr.readProxyString(certPEM)
    if datetime.utcnow() >= exptime:
        return {'msg': 'Given certificate is expired'}, 400
    attr = getVOMSProxyAttributes(certPEM, chainPEM)
    if not attr or not dn:
        return {'msg': 'Failed to extract DN or VOMS attributes'}, 400
    proxyPEM = certPEM + keyPEM + chainPEM

    try:
        proxy = pemToCert(proxyPEM)
        if not checkRFCProxy(proxy):
            return {'msg': 'cert is not a valid proxy'}, 400
        proxyid = pmgr.actproxy.updateProxy(proxyPEM, dn, attr, exptime) # TODO maybe
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
