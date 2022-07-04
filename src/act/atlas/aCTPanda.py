import cgi
import json
import urllib.parse, socket, http.client
import os
import pickle
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

        self.context = ssl.SSLContext(ssl.PROTOCOL_TLSv1)
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
            self.log.error("error in connection: %s" %x)
        return urldata

    def getQueueStatus(self, queue=None):
        node = {}
        if queue:
            node = {'site': queue}
        self.log.debug('Getting queue info')
        urldata = self.__HTTPConnect__('getJobStatisticsWithLabel', node)
        if not urldata:
            self.log.warning('No queue info returned by panda')
            return None

        try:
            data = pickle.loads(urldata.encode())
        except Exception as e:
            self.log.error('Could not load panda response: %s' % urldata)
            return None

        return data

    def getJob(self,siteName,prodSourceLabel=None,getEventRanges=True):
        node={}
        node['siteName']=siteName
        if prodSourceLabel is not None:
            node['prodSourceLabel']=prodSourceLabel
        pid = None
        urldesc=None
        eventranges=None
        self.log.debug('Fetching jobs for %s %s' % ( siteName, prodSourceLabel) )
        urldata=self.__HTTPConnect__('getJob',node)
        if not urldata:
            self.log.info('No job from panda')
            return (None,None,None,None)
        try:
            urldesc = urllib.parse.parse_qs(urldata)
        except Exception as x:
            self.log.error(x)
            return (None,None,None,None)

        self.log.info('panda returned %s' % urldesc)
        status = urldesc['StatusCode'][0]
        if status == '20':
            self.log.debug('No Panda activated jobs available')
            return (-1,None,None,None)
        elif status == '0':
            pid = urldesc['PandaID'][0]
            self.log.info('New Panda job with ID %s' % pid)
            prodSourceLabel = urldesc['prodSourceLabel'][0]
            if getEventRanges and 'eventService' in urldesc and urldesc['eventService'][0] == 'True':
                node = {}
                node['pandaID'] = urldesc['PandaID'][0]
                node['jobsetID'] = urldesc['jobsetID'][0]
                node['taskID'] = urldesc['taskID'][0]
                node['nRanges'] = 500 # TODO: configurable?
                if siteName == 'BOINC-ES':
                    node['nRanges'] = 100
                eventranges = self.getEventRanges(node)
        elif status == '60':
            self.log.error('Failed to contact Panda, proxy may have expired')
        else:
            self.log.error('Check out what this Panda rc means %s' % status)
        self.log.debug("%s %s" % (pid,urldesc))
        return (pid,urldata,eventranges,prodSourceLabel)

    def getEventRanges(self, node):
        self.log.debug('%s: Fetching event ranges' % node['pandaID'])
        urldata=self.__HTTPConnect__('getEventRanges', node)
        if not urldata:
            self.log.info('%s: Could not get event ranges from panda' % node['pandaID'])
            return None
        try:
            urldesc = cgi.parse_qs(urldata)
        except Exception as x:
            self.log.error(x)
            return None
        self.log.debug('%s: Panda returned %s' % (node['pandaID'], urldesc))
        status = urldesc['StatusCode'][0]
        if status == '0':
            return urldesc['eventRanges'][0]
        if status == '60':
            self.log.error('Failed to contact Panda, proxy may have expired')
        else:
            self.log.error('Check out what this Panda rc means %s' % status)
        return None

    def updateEventRange(self, node):
        self.log.debug('Updating event range %s: %s' % (node['eventRangeID'], str(node)))
        urldata=self.__HTTPConnect__('updateEventRange', node)
        self.log.debug('panda returned %s' % str(urldata))
        if not urldata:
            self.log.info('Could not update event ranges in panda')
            return None
        try:
            urldesc = cgi.parse_qs(urldata)
        except Exception as x:
            self.log.error(x)
            return None
        return urldesc

    def updateEventRanges(self, node):
        urldata=self.__HTTPConnect__('updateEventRanges', node)
        self.log.debug('panda returned %s' % str(urldata))
        if not urldata:
            self.log.info('Could not update event ranges in panda')
            return None
        try:
            urldesc = cgi.parse_qs(urldata)
        except Exception as x:
            self.log.error(x)
            return None
        return urldesc

    def getStatus(self,pandaId):
        self.log.info("entry %d" % pandaId)
        node={}
        node['ids']=pandaId
        urldesc=None
        urldata=self.__HTTPConnect__('getStatus',node)
        try:
            urldesc = cgi.parse_qs(urldata)
        except Exception as x:
            self.log.error(x)
            return None
        return urldesc


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
            urldesc = cgi.parse_qs(urldata)
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


    def queryJobInfo(self, cloud='ND'):
        node={}
        node['cloud']=cloud
        node['schedulerID']=self.conf.panda.schedulerid
        try:
            urldata=self.__HTTPConnect__('queryJobInfoPerCloud',node)
        except:
            return []
        try:
            return pickle.loads(urldata)
        except:
            return []


if __name__ == '__main__':

    from act.common.aCTLogger import aCTLogger
    logger = aCTLogger('test')
    log = logger()
    p = aCTPanda(log, os.environ['X509_USER_PROXY'])
    print(p.getQueueStatus('UIO'))
