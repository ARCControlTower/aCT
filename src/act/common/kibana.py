#!/usr/bin/env python3
# Generate xml and send to Kibana
#
# Call this in a cron with arguments service_id webpage_url

import os
import subprocess
import sys
import time
import json
import requests
from datetime import datetime
from act.arc.aCTDBArc import aCTDBArc
from act.atlas.aCTDBPanda import aCTDBPanda
from act.common.aCTLogger import aCTLogger
from act.common.aCTConfig import aCTConfigARC

try:
    service_id, webpage_url = sys.argv[1:3]
except:
    print('Usage: kibana.py service_id webpage_url')
    sys.exit(1)

logger = aCTLogger('kibana probe')
log = logger()
arcdb = aCTDBArc(log)
pandadb = aCTDBPanda(log)
config = aCTConfigARC()

def getARCJobs():
    return str(arcdb.getNArcJobs('TRUE'))

def getARCSlots():
    jobs=arcdb.getArcJobsInfo("state='Running'",['RequestedSlots'])
    slots=0
    for j in jobs:
        slots += j['RequestedSlots']
    return str(slots)

def getPandaNotStarted():
    return str(pandadb.getNJobs("actpandastatus='sent' and created<NOW()-interval 12 hour"))

def getArcQueuedLong():
    jobs=arcdb.getArcJobsInfo("state='Queuing' and created<NOW()-interval 12 hour",['id'])
    return str(len(jobs))

def getPandaDone():
    return str(pandadb.getNJobs("actpandastatus='done'"))

def getPandaDoneFailed():
    return str(pandadb.getNJobs("actpandastatus='donefailed'"))

def getAvailability():

    # Check autopilot is running
    logdir = config.logger.logdir
    try:
        mtime = os.stat('%s/aCTAutopilot.log' % logdir).st_mtime
    except:
        # Check previous log (in case it was just rotated)
        try:
            mtime = os.stat('%s/aCTAutopilot.log-%s' % (logdir, datetime.now().strftime('%Y%m%d'))).st_mtime
        except:
            return 'degraded', 'Autopilot log not available'
    if time.time() - mtime > 900:
        return 'degraded', 'Autopilot log not updated in %d seconds' % (time.time() - mtime)

    # Check heartbeats are being updated
    timelimit = 3600
    select = "sendhb=1 and " \
         "pandastatus in ('sent', 'starting', 'running', 'transferring') and " \
         "theartbeat != 0 and " + pandadb.timeStampLessThan("theartbeat", timelimit)
    columns = ['pandaid']
    jobs = pandadb.getJobs(select, columns)
    if len(jobs) > 100:
        return 'degraded', '%d jobs with outdated heartbeat. JUST A TEST PLEASE IGNORE!' % len(jobs)

    # All ok
    return 'available', 'all ok'


def send(document):
    return requests.post('http://monit-metrics.cern.ch:10012/', data=json.dumps(document), headers={ "Content-Type": "application/json; charset=UTF-8"})

def send_and_check(document, should_fail=False):
    response = send(document)
    assert( (response.status_code in [200]) != should_fail), 'With document: {0}. Status code: {1}. Message: {2}'.format(document, response.status_code, response.text)


availability, desc = getAvailability()

i=[]
info={}
info['producer'] = 'atlasact'
info['type'] = "availability"
info['availabilityinfo'] = desc
info['service_status'] = availability
info['availabilitydesc'] = 'Check whether aCT is functioning correctly'
info['serviceid'] = service_id
info['timestamp'] =  int(time.time()*1000)
info['contact'] = 'atlas-adc-act-support@cern.ch'
info['webpage'] = webpage_url

infom={}
infom['producer'] = 'atlasact'
infom['type'] = "metric"
infom['timestamp'] =  int(time.time()*1000)
infom['arcjobs'] = int(getARCJobs())
infom['arcslots'] = int(getARCSlots())
infom['pandasent12h'] = int(getPandaNotStarted())
infom['arcqueued12h'] = int(getArcQueuedLong())
infom['pandadone'] = int(getPandaDone())
infom['pandafailed'] = int(getPandaDoneFailed())
infom['serviceid'] = service_id
infom["idb_tags"] = ["serviceid"]

i.append(info)
i.append(infom)

send_and_check(i)
#print(json.dumps(i))
