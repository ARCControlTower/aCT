import os
import shutil
import io
import jwt
import arc
import act.client.x509proxy as x509proxy
import yaml

from act.client.jobmgr import JobManager, checkJobDesc, getIDsFromList
from act.client.proxymgr import ProxyManager, getVOMSProxyAttributes
from act.client.errors import InvalidJobDescriptionError, InvalidJobIDError
from act.client.errors import UnknownClusterError, RESTError, InvalidJobRangeError, ConfigError
from act.common.aCTConfig import aCTConfigARC
from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from datetime import datetime, timedelta
from werkzeug.exceptions import BadRequest


# TODO: see if checkJobExists should be used anywhere else
# TODO: implement proper logging
# TODO: HTTP return codes
# TODO: consistently change relevant jobmgr API to return jobs as dicts
#       rather than list of IDs


JWT_SECRET = "aCT JWT secret"
STREAM_CHUNK_SIZE = 4096


from flask import Flask, request, send_file, jsonify
app = Flask(__name__)


@app.route('/test', methods=['GET'])
def test():
    return "Hello World!\n", 200


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
        jmgr = JobManager()
        proxyid = token['proxyid']
        jobids = getIDs()
        jobdicts = jmgr.getJobStats(proxyid, jobids, state_filter, name_filter, clicols, arccols)
    except RESTError as e:
        print('error: GET /jobs: {}'.format(e))
        return {'msg': str(e)}, e.httpCode
    except Exception:
        print('error: GET /jobs: {}'.format(e))
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
        print('error: DELETE /jobs: {}'.format(e))
        return {'msg': str(e)}, e.httpCode
    proxyid = token['proxyid']

    name_filter = request.args.get('name', default='')
    state_filter = request.args.get('state', default='')

    try:
        jmgr = JobManager()
        deleted = jmgr.cleanJobs(proxyid, jobids, state_filter, name_filter)
        for jobid in deleted:
            try:
                datadir = jmgr.getJobDataDir(jobid)
                shutil.rmtree(jmgr.getJobDataDir(datadir))
            except OSError as e:
                print('error: PATCH /jobs: deleting {}: {}'.format(datadir, e))
    except Exception as e:
        print('error: DELETE /jobs: {}'.format(e))
        return {'msg': 'Server error'}, 500

    return jsonify(deleted)


# expects JSON object with 'state' attribute:
# { "state": "tofetch|tocancel|toresubmit" }
@app.route('/jobs', methods=['PATCH'])
def patch():
    '''
    Set jobs' state based on request parameters.

    Parameter that defines operation is passed in body of request in
    JSON format. It is a JSON object with a single property "arcstate",
    that has to be one of possible settable states
    (for instance {"arcstate": "tofetch"}).

    Other parameters are passed in URL, they are:
        'id': a list of job IDs
        'name': a substring that has to be present in job names
        'state': state that jobs have to be in

    Returns:
        status 200: A string with a number of affected jobs.
        status 4**: A string with error message.
    '''
    try:
        token = getToken()
        jobids = getIDs()
        arcstate = request.get_json().get('arcstate', None)
    except BadRequest as e:
        print('error: PATCH /jobs: {}'.format(e))
        return {'msg': str(e)}, 400
    except RESTError as e:
        print('error: PATCH /jobs: {}'.format(e))
        return {'msg': str(e)}, e.httpCode
    proxyid = token['proxyid']

    name_filter = request.args.get('name', default='')
    state_filter = request.args.get('state', default='')

    if arcstate is None:
        return {'msg': 'Request data has no \'arcstate\' attribute'}, 400

    try:
        jmgr = JobManager()
        if arcstate == 'tofetch':
            jobids = jmgr.fetchJobs(proxyid, jobids, name_filter)
        elif arcstate == 'tocancel':
            # One state in which a job can be killed is before it is passed
            # to ARC. Such jobs have None as arcid. Data dirs for jobs are
            # otherwise cleaned by cleaning operation but this is one exception
            # where killing destroys the job immediately and has to remove the
            # data dir as well.
            jobids = jmgr.killJobs(proxyid, jobids, state_filter, name_filter)
            for job in jobids:
                if job['a_id'] is None or job['a_arcstate'] in ('tosubmit', 'submitting'):
                    try:
                        datadir = jmgr.getJobDataDir(job['c_id'])
                        shutil.rmtree(jmgr.getJobDataDir(datadir))
                    except OSError as e:
                        print('error: PATCH /jobs: deleting {}: {}'.format(datadir, e))
        elif arcstate == 'toresubmit':
            jobids = jmgr.resubmitJobs(proxyid, jobids, name_filter)
        else:
            return {'msg': '"arcstate" should be either "tofetch" or "tocancel" or "toresubmit"'}, 400
    except Exception as e:
        print('error: PATCH /jobs: {}'.format(e))
        return {'msg': 'Server error'}, 500
    return jsonify(jobids)


# expects a JSON list of job objects in the following form:
# [
#   {
#     "desc": "<xRSL or ADL>",
#     "site": "<site>"
#   },
#   {
#     "desc": "<xRSL or ADL>",
#     "site": "<site>"
#   },
#   ...
# ]
@app.route('/jobs', methods=['POST'])
def create_jobs():
    errpref = 'error: POST /jobs: '
    try:
        token = getToken()
        jobs = request.get_json()
        jmgr = JobManager()
    except RESTError as e:
        print('{}{}'.format(errpref, e))
        return {'msg': str(e)}, e.httpCode
    except BadRequest as e:  # raised for invalid JSON
        print('{}{}'.format(errpref, e))
        return {'msg': str(e)}, 400
    except Exception as e:
        print('{}{}'.format(errpref, e))
        return {'msg': 'Server error'}, 500

    results = []
    for job in jobs:
        result = {}

        # check job description
        if 'desc' not in job:
            print('{}No job description given'.format(errpref))
            result['msg'] = 'No job description given'
            results.append(result)
            continue
        jobdescs = arc.JobDescriptionList()
        if not arc.JobDescription_Parse(job['desc'], jobdescs):
            print('{}Invalid job description'.format(errpref))
            result['msg'] = 'Invalid job description'
            results.append(result)
            continue
        result['name'] = jobdescs[0].Identification.JobName

        try:
            # check clusters
            if 'clusterlist' not in job or not job['clusterlist']:
                print('{}No clusters given'.format(errpref))
                result['msg'] = 'No clusters given'
                results.append(result)
                continue
            checkClusters(job['clusterlist'])

            # insert job and create its data directory
            jobid = jmgr.clidb.insertJob(job['desc'], token['proxyid'], ','.join(job['clusterlist']))
            jobDataDir = jmgr.getJobDataDir(jobid)
            os.makedirs(jobDataDir)
        except UnknownClusterError as e:
            print('{}Unknown cluster {}'.format(errpref, e.name))
            result['msg'] = 'Unknown cluster {}'.format(e.name)
            results.append(result)
            continue
        except Exception as e:
            print('{}{}'.format(errpref, e))
            result['msg'] = 'Server error'
            results.append(result)
            continue

        result['id'] = jobid
        results.append(result)

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
        jobs = request.get_json()
        jmgr = JobManager()
    except BadRequest as e:
        print('{}{}'.format(errpref, e))
        return {'msg': str(e)}, 400
    except RESTError as e:
        print('{}{}'.format(errpref, e))
        return {'msg': str(e)}, e.httpCode
    except Exception as e:
        print('{}{}'.format(errpref, e))
        return {'msg': 'Server error'}, 500

    results = []
    for job in jobs:
        result = {}
        # check job description
        if 'desc' not in job:
            print('{}No job description given'.format(errpref))
            result['msg'] = 'No job description given'
            results.append(result)
            continue
        jobdescs = arc.JobDescriptionList()
        if not arc.JobDescription_Parse(job['desc'], jobdescs):
            print('{}Invalid job description'.format(errpref))
            result['msg'] = 'Invalid job description'
            results.append(result)
            continue
        result['name'] = jobdescs[0].Identification.JobName

        # check job ID
        if 'id' not in job:
            print('{}No job ID given')
            result['msg'] = 'No job ID given'
            results.append(result)
            continue
        result['id'] = job['id']
        jobid = jmgr.checkJobExists(proxyid, job['id'])
        if not jobid:
            print('{}Job ID {} does not exist'.format(errpref, jobid))
            result['msg'] = 'Job ID {} does not exist'.format(job['id'])
            results.append(result)
            continue

        # get job's data directory
        try:
            jobDataDir = jmgr.getJobDataDir(jobid)
        except ConfigError as e:
            print('{}{}'.format(errpref, e))
            result['msg'] = 'Server error'
            results.append(result)
            continue

        # InputFiles need to be accessed through index otherwise
        # the changes do not survive outside of for loop.
        for i in range(len(jobdescs[0].DataStaging.InputFiles)):
            filename = jobdescs[0].DataStaging.InputFiles[i].Name
            filepath = os.path.join(jobDataDir, filename)
            # TODO: handle the situation where file that should be
            #       present is not present (right now we asume all
            #       files are present and those who fail on this call
            #       are remote resources
            if not os.path.isfile(filepath):
                continue
            jobdescs[0].DataStaging.InputFiles[i].Sources[0].ChangeFullPath(os.path.abspath(filepath))

        # TODO: unparsing ADL at the time of writing didn't work
        # Unparse modified job description and check its validity
        desc = jobdescs[0].UnParse('', '')[1]
        jobdescs = arc.JobDescriptionList()
        if not arc.JobDescription_Parse(desc, jobdescs):
            print('{}Invalid modified job description'.format(errpref))
            result['msg'] = 'Server error'
            results.append(result)
            continue

        # insert modified job description and confirm job for submission
        try:
            # inserting job description ID makes job eligible for
            # pickup by client2arc
            descid = jmgr.clidb.insertDescription(desc)
            jmgr.clidb.updateJob(jobid, {'jobdesc': descid})
        except Exception as e:
            print('{}{}'.format(errpref, e))
            result['msg'] = 'Server error'
            results.append(result)
            continue

        results.append(result)

    return jsonify(results)


@app.route('/results', methods=['GET'])
def getResults():
    '''
    Return a .zip archive of job results folder.

    Request potentionaly accepts a list of IDs but only the first one is
    processed because it's easier to respond for one job.

    Returns:
        status 200: A .zip archive of job results.
        status 4** or 500: A string with error message.
    '''
    try:
        token = getToken()
        jobids = getIDs()
    except RESTError as e:
        return {'msg': str(e)}, e.httpCode
    proxyid = token['proxyid']

    if not jobids:
        return {'msg': 'No job ID given'}, 400
    elif len(jobids) > 1:
        return {'msg': 'Cannot fetch results of more than one job'}, 400
    jobids = jobids[:1] # only take first job

    try:
        # get job results
        jmgr = JobManager()
        results = jmgr.getJobs(proxyid, jobids)
        if not results.jobdicts:
            return {'msg': 'Results for job not found'}, 404
        resultDir = results.jobdicts[0]['dir']
        if not resultDir:
            return {'msg': 'No results to fetch'}, 204

        # create result archive in data dir
        jobDataDir = jmgr.getJobDataDir(jobids[0])
        path = os.path.join(jobDataDir, os.path.basename(resultDir))
        archivePath = shutil.make_archive(path, 'zip', resultDir)
    except Exception as e:
        print('error: GET /results: {}'.format(e))
        return {'msg': 'Server error'}, 500

    return send_file(archivePath)


@app.route('/proxies', methods=['POST'])
def getCSR():
    # get issuer cert string from request
    try:
        issuer_pem = request.get_json().get('cert', None)
        pmgr = ProxyManager()
    except BadRequest as e:
        print('error: POST /proxies: {}'.format(e))
        return {'msg': str(e)}, 400
    except Exception as e:
        print('error: POST /proxies: creating proxy manager: {}'.format(e))
        return {'msg': 'Server error'}, 500

    # check proxy
    if not issuer_pem:
        print('error: POST /proxies: missing issuer certificate')
        return {'msg': 'Missing issuer certificate'}, 400
    dn, exptime = pmgr.readProxyString(issuer_pem)
    if datetime.now() >= exptime:
        print('error: POST /proxies: expired certificate')
        return {'msg': 'Given certificate is expired'}, 400
    attr = getVOMSProxyAttributes(issuer_pem)
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
        csr = x509proxy.create_proxy_csr(issuer, private_key)
        print('CSR generated: DN: {}, attr: {}, expiration: {}'.format(dn, attr, exptime))

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
        token = jwt.encode({'proxyid': proxyid, 'exp': exptime}, JWT_SECRET, algorithm='HS256')
    except Exception as e:
        print('error: POST /proxies: {}'.format(e))
        return {'msg': 'Server error'}, 500

    return {'token': token, 'csr': csr_pem}, 200


@app.route('/proxies', methods=['PUT'])
def uploadSignedProxy():
    try:
        token = getToken()
        data = request.get_json()
        pmgr = ProxyManager()
    except RESTError as e:
        print('error: PUT /proxies: {}'.format(e))
        return {'msg': str(e)}, e.httpCode
    except BadRequest as e:
        print('error: PUT /proxies: {}'.format(e))
        return {'msg': str(e)}, 400
    except Exception as e:
        print('error: PUT /proxies: {}'.format(e))
        return {'msg': 'Server error'}, 500

    proxyid = token['proxyid']
    cert_pem = data.get('cert', None)
    chain_pem = data.get('chain', None)
    if cert_pem is None:
        print('error: PUT /proxies: no signed certificate')
        return {'msg': 'Signed certificate missing'}, 400
    if chain_pem is None:
        print('error: PUT /proxies: no cert chain')
        return {'msg': 'Cert chain missing'}, 400

    key_pem = pmgr.getProxyKeyPEM(proxyid)
    dn, exptime = pmgr.readProxyString(cert_pem)
    if datetime.now() >= exptime:
        return {'msg': 'Given certificate is expired'}, 400
    attr = getVOMSProxyAttributes(cert_pem)
    if not attr or not dn:
        return {'msg': 'Failed to extract DN or VOMS attributes'}, 400
    proxy_pem = cert_pem + key_pem.decode('utf-8') + chain_pem

    try:
        proxy_obj = x509.load_pem_x509_certificate(proxy_pem.encode('utf-8'), backend=default_backend())
        if not checkRFCProxy(proxy_obj):
            return {'msg': 'cert is not a valid proxy'}, 400
        proxyid = pmgr.actproxy.updateProxy(proxy_pem, dn, attr, exptime)
        token = jwt.encode({'proxyid': proxyid, 'exp': exptime}, JWT_SECRET, algorithm='HS256')
    except Exception as e:
        print('error: PUT /proxies: {}'.format(e))
        return {'msg': 'Server error'}, 500

    print('Proxy submitted: DN: {}, attr: {}, expiration: {}'.format(dn, attr, exptime))
    return {'token': token}, 200


@app.route('/proxies', methods=['DELETE'])
def deleteProxy():
    try:
        token = getToken()
        pmgr = ProxyManager()
        pmgr.arcdb.deleteProxy(token['proxyid'])
    except RESTError as e:
        print('error: DELETE /proxies: {}'.format(e))
        return {'msg': str(e)}, e.httpCode
    except Exception as e:
        print('error: DELETE /proxies: {}'.format(e))
        return {'msg': 'Server error'}, 500
    return jsonify({'msg': 'Proxy deletion successful'}), 204


@app.route('/data', methods=['PUT'])
def uploadFile():
    try:
        token = getToken()
        jobids = getIDs()
    except RESTError as e:
        print('error: PUT /data: {}'.format(e))
        return {'msg': str(e)}, e.httpCode
    except Exception as e:
        print('error: PUT /data: {}'.format(e))
        return {'msg': 'Server error'}, 500
    proxyid = token['proxyid']

    if not jobids:
        print('error: PUT /data: no job ID given')
        return {'msg': 'No job ID given'}, 400
    elif len(jobids) > 1:
        print('error: PUT /data: more than one job ID given')
        return {'msg': 'More than one job ID given'}, 400

    try:
        jmgr = JobManager()
        jobid = jmgr.checkJobExists(proxyid, jobids[0])
        if not jobid:
            print('error: PUT /data: job ID does not exist')
            return {'msg': 'Client Job ID {} does not exist'.format(jobids[0])}, 400
    except Exception as e:
        print('error: PUT /data: {}'.format(e))
        return {'msg': 'Server error'}, 500


    try:
        ## this block fires exception if client doesn't complete upload
        #datafile = request.files.get('file', None)
        #if not datafile:
        #    print('error: PUT /data: file not sent')
        #    return {'msg': 'No file sent'}, 400

        #jobDataDir = jmgr.getJobDataDir(jobid)
        ## TODO: werkzeug.safe_filename does not work because we need relative
        ##       path that can go deep
        #datafile.save(os.path.join(jobDataDir, datafile.filename))

        filename = request.args.get('filename', None)
        if filename is None:
            print('error: PUT /data: file name not given')
            return {'msg': 'File name not given'}, 400

        jobDataDir = jmgr.getJobDataDir(jobid)
        filepath = os.path.join(jobDataDir, filename)
        with open(filepath, 'wb') as f:
            while True:
                chunk = request.stream.read(STREAM_CHUNK_SIZE)
                if len(chunk) == 0:
                    break
                f.write(chunk)
    except Exception as e:
        print('error: PUT /data: {}'.format(e))
        return {'msg': 'Server error'}, 500

    return {'msg': 'OK'}, 200


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
            raise RESTError('Invalid job ID: {}'.format(e.jobid), 400)
        except InvalidJobRangeError as e:
            raise RESTError('Invalid job range: {}'.format(e.jobRange), 400)
    else:
        return []


def getToken():
    '''
    Raises:
        RESTError: Token is not present or is expired
    '''
    tokstr = request.headers.get('Authorization', None)
    if tokstr is None: raise RESTError('Auth token is missing', 401)
    try:
        # potential errors with tokstr, token decode, proxy manager ...
        tokstr = tokstr.split()[1]
        token = jwt.decode(tokstr, JWT_SECRET, algorithms=['HS256'])
        pmgr = ProxyManager()
        result = pmgr.checkProxyExists(token['proxyid'])
        if result is None:
            raise RESTError('Server error', 500)
        if result is False:
            raise RESTError('Proxy from token does not exist in database or is expired; use actproxy', 401)
    except jwt.ExpiredSignatureError:
        raise RESTError('Auth token is expired', 401)
    else:
        return token


def checkRFCProxy(proxy):
    for ext in proxy.extensions:
        if ext.oid.dotted_string == "1.3.6.1.5.5.7.1.14":
            return True
    return False


# TODO: hardcoded
# TODO: for now this is called in try except block so we don't handle exceptions
def checkClusters(clusterlist):
    with open('/etc/act/clusters.yaml', 'r') as f:
        yamlstr = f.read()
    conf = yaml.safe_load(yamlstr)
    if 'clusters' not in conf:
        raise ConfigError('/etc/act/clusters.yaml: clusters')
    for cluster in conf['clusters']:
        if cluster not in conf['clusters']:
            raise UnknownClusterError(cluster)
