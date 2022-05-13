import datetime
import json
import requests
from act.common.aCTLogger import aCTLogger

class aCTAPFMon:

    def __init__(self, conf, log=None):

        if log:
            self.log = log
        else:
            self.logger = aCTLogger(self.__class__.__name__, arclog=False)
            self.log = self.logger()

        self.apfmonurl = conf.monitor.apfmon
        self.sendupdates = conf.monitor.update
        self.acturl = conf.joblog.urlprefix
        self.factory = conf.panda.schedulerid

    def registerFactory(self):
        '''Register this instance of aCT'''

        if not self.apfmonurl or not self.sendupdates:
            self.log.info('APFmon updates disabled')
            return

        factoryregister = '%s/factories/%s' % (self.apfmonurl, self.factory)
        factory = {"url":     self.acturl,
                   "email":   "act-dev@cern.ch",
                   "version": "1.0.0"}
        payload = json.dumps(factory)

        self.log.debug("Registering %s as new factory at %s" % (self.factory, factoryregister))
        r = requests.put(factoryregister, data=payload, timeout=10)
        self.log.debug("APFmon returned %d: %s" % (r.status_code, r.text))

    def registerLabels(self, sites):
        '''Register labels (panda queues)'''

        if not self.apfmonurl or not sites or not self.sendupdates:
            return

        labelregister = '%s/labels' % self.apfmonurl
        labels = [{"name": "%s" % site, "factory": "%s" % self.factory} for site in sites]
        payload = json.dumps(labels)

        self.log.debug("Registering new labels: %s" % ','.join([s for s in sites]))
        r = requests.put(labelregister, data=payload, timeout=10)
        self.log.debug("APFmon returned %d: %s" % (r.status_code, r.text))

    def registerJobs(self, pandaids, site):
        '''Register new jobs'''

        if not self.apfmonurl or not pandaids or not self.sendupdates:
            return

        logdir = '%s/%s/%s' % (self.acturl, datetime.date.today().isoformat(), site)
        jobregister = '%s/jobs' % self.apfmonurl
        jobs = [{'cid'        : '%s' % rowid,
                 'factory'    : self.factory,
                 'label'      : site,
                 'stdouturl'  : '%s/%s.out' % (logdir, pid),
                 'stderrurl'  : '%s/%s.err' % (logdir, pid),
                 'logurl'     : '%s/%s.log' % (logdir, pid),
                 'jdlurl'     : '%s/%s.jdl' % (logdir, pid)} for (rowid, pid) in pandaids]
        payload = json.dumps(jobs)

        self.log.debug("sending to %s: %s" % (jobregister, jobs))
        r = requests.put(jobregister, data=payload, timeout=10)
        self.log.debug("APFmon returned %d: %s" % (r.status_code, r.text))

    def updateJob(self, pandaid, status, exitcode=None):
        '''Update job status. Exit code is required for "exiting" state'''

        if not self.apfmonurl or not self.sendupdates:
            return

        jobupdate = '%s/jobs/%s:%s' % (self.apfmonurl, self.factory, pandaid)
        payload = {"state": "%s" % status}
        if exitcode is not None:
            payload['rc'] = exitcode

        self.log.debug("sending to %s: %s" % (jobupdate, payload))
        r = requests.post(jobupdate, data=payload, timeout=10)
        self.log.debug("APFmon returned %d: %s" % (r.status_code, r.text))

def test():
    from act.common.aCTConfig import aCTConfigAPP
    conf = aCTConfigAPP()
    apf = aCTAPFMon(conf)
    apf.registerFactory()
    apf.registerLabels(['ARC-TEST', 'BOINC_MCORE'])
    apf.registerJobs(['123456', '234567'], 'ARC-TEST')
    apf.updateJob('123456', 'running')

if __name__ == '__main__':
    test()

