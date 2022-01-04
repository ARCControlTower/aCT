import json
import os
import shutil
import io
import jwt
import arc
import act.client.x509proxy as x509proxy

from act.client.jobmgr import JobManager, checkJobDesc, checkSite, getIDsFromList
from act.client.proxymgr import ProxyManager, getVOMSProxyAttributes
from act.client.errors import InvalidJobDescriptionError, InvalidJobIDError
from act.client.errors import NoSuchSiteError, RESTError, InvalidJobRangeError
from act.common.aCTConfig import aCTConfigARC
from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from datetime import datetime, timedelta


# TODO: see if checkJobExists should be used anywhere else
# TODO: implement proper logging
# TODO: HTTP return codes


JWT_SECRET = "aCT JWT secret"


from flask import Flask, request, send_file
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
        jmgr = JobManager()
        token = getToken()
        proxyid = token['proxyid']
        jobids = getIDs()
        jobdicts = jmgr.getJobStats(proxyid, jobids, state_filter, name_filter, clicols, arccols)
    except RESTError as e:
        return {'msg': str(e)}, e.httpCode
    except Exception:
        # TODO: log properly
        return {'msg': 'Server error'}, 500
    else:
        return json.dumps(jobdicts)


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
        return {'msg': str(e)}, e.httpCode
    proxyid = token['proxyid']

    name_filter = request.args.get('name', default='')
    state_filter = request.args.get('state', default='')

    jmgr = JobManager()
    deleted = jmgr.cleanJobs(proxyid, jobids, state_filter, name_filter)

    for jobid in deleted:
        shutil.rmtree(jmgr.getJobDataDir(jobid))

    return json.dumps(deleted)


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
    except RESTError as e:
        return {'msg': str(e)}, e.httpCode
    proxyid = token['proxyid']

    name_filter = request.args.get('name', default='')
    state_filter = request.args.get('state', default='')

    arcstate = request.get_json().get('arcstate', None)
    if arcstate is None:
        return {'msg': 'Request data has no \'arcstate\' attribute'}, 400

    jmgr = JobManager()

    if arcstate == 'tofetch':
        jobids = jmgr.fetchJobs(proxyid, jobids, name_filter)
    elif arcstate == 'tocancel':
        jobids = jmgr.killJobs(proxyid, jobids, state_filter, name_filter)
    elif arcstate == 'toresubmit':
        jobids = jmgr.resubmitJobs(proxyid, jobids, name_filter)
    else:
        return {'msg': '"arcstate" should be either "tofetch" or "tocancel" or "toresubmit"'}, 400
    return json.dumps(jobids)


# TODO: figure out if this can be done for multiple jobs
# expects a JSON object of the following form:
# {
#   "desc": "<xRSL or ADL>",
#   "site": "<site>"
# }
@app.route('/jobs', methods=['POST'])
def submit():
    job = request.get_json()
    if 'desc' not in job:
        return {'msg': 'No job description file given'}, 400
    if 'site' not in job:
        return {'msg': 'No site given'}, 400

    try:
        jmgr = JobManager()
        token = getToken()
        checkJobDesc(job['desc'])
        checkSite(job['site'])
        # TODO: more systematic error handling for insertion and data dir
        jobid = jmgr.clidb.insertJob(job['desc'], token['proxyid'], job['site'])
        jobDataDir = jmgr.getJobDataDir(jobid)
        os.makedirs(jobDataDir)
    except RESTError as e:
        return {'msg': str(e)}, e.httpCode
    except InvalidJobDescriptionError:
        return {'msg': 'Invalid job description'}, 400
    except NoSuchSiteError:
        return {'msg': 'Invalid site'}, 400
    except Exception as e:
        # TODO: log error
        return {'msg': 'Server error'}, 500

    return {'id': jobid}, 200


# TODO: figure out if this can be done for multiple jobs
# expects a JSON object of the following form:
# {
#   "id": <jobid>,
#   "desc": "<xRSL or ADL>",
#   "site": "<site>"
# }
@app.route('/jobs', methods=['PUT'])
def submitWithData():
    job = request.get_json()
    if 'id' not in job:
        return {'msg': 'No jobid parameter'}, 400
    if 'desc' not in job:
        return {'msg': 'No job description parameter'}, 400

    jobdescs = arc.JobDescriptionList()
    if not arc.JobDescription_Parse(job['desc'], jobdescs):
        return {'msg': 'Invald job description'}, 400

    try:
        token = getToken()
    except RESTError as e:
        return {'msg': str(e)}, e.httpCode
    proxyid = token['proxyid']

    # this is run to check if jobid even exists
    jmgr = JobManager()
    jobid = jmgr.checkJobExists(proxyid, job['id'])
    if not jobid:
        return {'msg': 'Client job ID {} does not exist'.format(job['id'])}, 400

    jobDataDir = jmgr.getJobDataDir(jobid)
    filenames = []
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

    # TODO: unparsing of ADL at the time of writing particular point didn't work
    desc = jobdescs[0].UnParse('', '')[1]
    jobdescs = arc.JobDescriptionList()
    if not arc.JobDescription_Parse(desc, jobdescs):
        # TODO: log error
        return {'msg': 'Server error'}, 500

    try:
        descid = jmgr.clidb.insertDescription(desc)
        # inserting job description ID makes job eligible for
        # pickup by client2arc
        jmgr.clidb.updateJob(jobid, {'jobdesc': descid})
    except Exception as e:
        # TODO: log error
        return {'msg': 'Server error'}, 500

    return {'id': jobid}, 200


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

    # get job results
    jmgr = JobManager()
    results = jmgr.getJobs(proxyid, jobids)
    if not results.jobdicts:
        return {'msg': 'Results for job not found'}, 404
    resultDir = results.jobdicts[0]['dir']

    # TODO: should probably split this for more systematic error handling
    try:
        jobDataDir = jmgr.getJobDataDir(jobids[0])
        path = os.path.join(jobDataDir, os.path.basename(resultDir))
        archivePath = shutil.make_archive(path, 'zip', resultDir)
    except Exception as e:
        return 'Server error', 500

    return send_file(archivePath)


@app.route('/proxies', methods=['POST'])
def getCSR():
    issuer_pem = request.form.get('cert')
    if not issuer_pem:
        return {'msg': 'Missing issuer certificate'}, 400
    issuer = x509.load_pem_x509_certificate(issuer_pem.encode("utf-8"), default_backend())

    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=1024,
        backend=default_backend()
    )

    csr = x509proxy.create_proxy_csr(issuer, private_key)

    # put private key into string and store in db
    pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption()
    ).decode('utf-8')
    pmgr = ProxyManager()
    dn, exptime = pmgr.readProxyString(issuer_pem)
    attr = getVOMSProxyAttributes(issuer_pem)
    proxyid = pmgr.actproxy.updateProxy(pem, dn, attr, exptime)
    if proxyid is None:
        return {'msg': 'Server error'}, 500

    csr_pem = csr.public_bytes(serialization.Encoding.PEM).decode('utf-8')
    token = jwt.encode({'proxyid': proxyid, 'exp': exptime}, JWT_SECRET, algorithm='HS256')
    return {'token': token, 'csr': csr_pem}, 200


@app.route('/proxies', methods=['PUT'])
def uploadSignedProxy():
    data = request.get_json()
    cert_pem = data.get('cert', None)
    chain_pem = data.get('chain', None)
    if cert_pem is None:  return {'msg': 'Signed certificate missing'}, 400
    if chain_pem is None: return {'msg': 'Cert chain missing'}, 400

    try:
        token = getToken()
    except RESTError as e:
        return {'msg': str(e)}, e.httpCode
    proxyid = token['proxyid']

    pmgr = ProxyManager()
    key_pem = pmgr.getProxyKeyPEM(proxyid)
    dn, exptime = pmgr.readProxyString(cert_pem)
    attr = getVOMSProxyAttributes(cert_pem)
    proxy_pem = cert_pem + key_pem.decode('utf-8') + chain_pem
    proxyid = pmgr.actproxy.updateProxy(proxy_pem, dn, attr, exptime)
    token = jwt.encode({'proxyid': proxyid, 'exp': exptime}, JWT_SECRET, algorithm='HS256')
    return {'token': token}, 200


@app.route('/data', methods=['PUT'])
def uploadFile():
    try:
        token = getToken()
    except RESTError as e:
        return {'msg': str(e)}, e.httpCode
    proxyid = token['proxyid']

    try:
        jobids = getIDs()
    except Exception:
        return {'msg': 'Invalid id parameter'}, 400
    if not jobids:
        return {'msg': 'No job ID given'}, 400
    elif len(jobids) > 1:
        return {'msg': 'More than one job ID given'}, 400
    # TODO: is there any duplication with submitWithData?
    jmgr = JobManager()
    jobid = jmgr.checkJobExists(proxyid, jobids[0])
    if not jobid:
        return {'msg': 'Client Job ID {} does not exist'.format(jobids[0])}, 400

    if not 'file' in request.files:
        return {'msg': 'No file sent'}, 400

    datafile = request.files["file"]

    # TODO: check for error or use current exceptions?
    jobDataDir = jmgr.getJobDataDir(jobid)
    # TODO: werkzeug.safe_filename does not work because we need relative
    #       path that can go deep
    datafile.save(os.path.join(jobDataDir, datafile.filename))
    return 'OK', 200


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
    tokstr = request.args.get('token', None)
    if tokstr is None: raise RESTError('Auth token is missing', 401)
    try:
        token = jwt.decode(tokstr, JWT_SECRET, algorithms=['HS256'])
        pmgr = ProxyManager()
        result = pmgr.checkProxyExists(token['proxyid'])
        if result is None:
            raise RESTError('Server error', 500)
        if result is False:
            raise RESTError('Proxy from token does not exist in database', 401)
    except jwt.ExpiredSignatureError:
        raise RESTError('Auth token is expired', 401)
    else:
        return token


