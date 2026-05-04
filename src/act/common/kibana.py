# Generate xml and send to Kibana
#
# Call this in a cron with arguments service_id webpage_url

import json
import os
import sys
import time
from datetime import datetime, timedelta

import requests
from act.atlas.aCTDBPanda import aCTDBPanda
from act.common.aCTConfig import aCTConfigARC
from act.common.aCTLogger import aCTLogger
from act.atlas.dbModels import PandaJob
from act.arc.dbModels import ArcJob

from sqlalchemy import select, func


def getAvailability(config, db: aCTDBPanda):

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
    with db.Session() as session:
        timelimit = 3600
        jobs = session.execute(select(PandaJob.pandaid).where(PandaJob.sendhb==1, PandaJob.pandastatus.in_(['sent', 'starting', 'running', 'transferring'],
                                                                PandaJob.theartbeat!=0, db.timeStampLessThan(PandaJob.theartbeat, timelimit)))).all()
    if len(jobs) > 100:
        return 'degraded', '%d jobs with outdated heartbeat. JUST A TEST PLEASE IGNORE!' % len(jobs)

    # All ok
    return 'available', 'all ok'


def send(document):
    return requests.post('http://monit-metrics.cern.ch:10012/', data=json.dumps(document), headers={ "Content-Type": "application/json; charset=UTF-8"})


def send_and_check(document, should_fail=False):
    response = send(document)
    assert( (response.status_code in [200]) != should_fail), 'With document: {0}. Status code: {1}. Message: {2}'.format(document, response.status_code, response.text)


def main():
    try:
        service_id, webpage_url = sys.argv[1:3]
    except:
        print('Usage: kibana service_id webpage_url')
        sys.exit(1)

    logger = aCTLogger('kibana probe')
    log = logger()
    db = aCTDBPanda(log)
    config = aCTConfigARC()

    availability, desc = getAvailability(config, db)

    i = []
    info = {}
    info['producer'] = 'atlasact'
    info['type'] = "availability"
    info['availabilityinfo'] = desc
    info['service_status'] = availability
    info['availabilitydesc'] = 'Check whether aCT is functioning correctly'
    info['serviceid'] = service_id
    info['timestamp'] = int(time.time()*1000)
    info['contact'] = 'atlas-adc-act-support@cern.ch'
    info['webpage'] = webpage_url

    with db.Session() as session:
        infom = {}
        infom['producer'] = 'atlasact'
        infom['type'] = "metric"
        infom['timestamp'] = int(time.time()*1000)
        infom['arcjobs'] = session.scalar(select(func.count()).select_from(ArcJob))
        infom['arcslots'] = session.scalar(select(func.sum(ArcJob.RequestedSlots)).where(ArcJob.State == 'Running')) or 0
        infom['pandasent12h'] = session.scalar(select(func.count()).where(PandaJob.actpandastatus=='sent', db.timeStampLessThan(PandaJob.created, 43200)))
        infom['arcqueued12h'] = session.scalar(select(func.count()).where(ArcJob.State=='Queuing', db.timeStampLessThan(ArcJob.created, 43200)))
        infom['pandadone'] = session.scalar(select(func.count()).where(PandaJob.actpandastatus=='done'))
        infom['pandafailed'] = session.scalar(select(func.count()).where(PandaJob.actpandastatus=='donefailed'))
        infom['serviceid'] = service_id
        infom["idb_tags"] = ["serviceid"]

    i.append(info)
    i.append(infom)

    send_and_check(i)
    #print(json.dumps(i))
