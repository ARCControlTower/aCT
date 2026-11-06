import json
import urllib.parse, socket, http.client
import ssl
from act.common import aCTConfig

class aCTPanda:


    def __init__(self,logger, proxyfile):
        self.conf = aCTConfig.aCTConfigAPP()
        server = self.conf.panda.server
        u = urllib.parse.urlparse(server)
        self.hostport = u.netloc
        self.topdir = u.path
        proxypath = proxyfile
        self.log = logger
        # timeout in seconds
        self.timeout = self.conf.panda.timeout
        socket.setdefaulttimeout(self.timeout)

        # TLS_CLIENT negotiates the best TLS version and also checks the hostname
        self.context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        self.context.load_cert_chain(proxypath, keyfile=proxypath)
        self.context.verify_mode = ssl.CERT_REQUIRED
        self.context.load_verify_locations('/etc/pki/tls/certs/CERN-bundle.pem')

    def __HTTPConnect__(self, mode, node):
        urldata = None
        try:
            conn = http.client.HTTPSConnection(self.hostport, context=self.context)
            rdata = urllib.parse.urlencode(node)
            conn.request("POST", self.topdir+mode, rdata)
            resp = conn.getresponse()
            urldata = resp.read().decode()
            conn.close()
        except Exception as x:
            self.log.error(f"error in connection: {x}")
        return urldata

    def updateStatus(self,pandaId,state,desc={}):
        node={}
        node['jobId']=pandaId
        node['state']=state
        node['schedulerID']=self.conf.panda.schedulerid
        if desc:
            for key in desc.keys():
                node[key]=desc[key]
        # protection against bad pickles
        if 'jobId' not in node or not node['jobId']:
            node['jobId'] = pandaId
        if 'state' not in node or not node['state']:
            node['state'] = state
        urldesc=None
        urldata=self.__HTTPConnect__('updateJob',node)
        #self.log.debug('panda returned %s' % str(urldata))
        try:
            urldesc = urllib.parse.parse_qs(urldata)
        except Exception as x:
            self.log.error(x)
            return None
        return urldesc

    def updateStatuses(self, jobs):
        # Caller must make sure jobId and state are defined for each job
        jobdata = []
        for job in jobs:
            node = job
            node['schedulerID'] = self.conf.panda.schedulerid
            jobdata.append(node)
        urldata=self.__HTTPConnect__('updateJobsInBulk', {'jobList': json.dumps(jobdata)})
        try:
            urldesc = json.loads(urldata)
        except Exception as x:
            self.log.error(x)
            return {}
        return urldesc
