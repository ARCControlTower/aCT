# Tool for updating heartbeats when the main process has failed.
from act.common.aCTLogger import aCTLogger
from act.atlas.aCTDBPanda import aCTDBPanda
from act.atlas.aCTPanda import aCTPanda
from act.atlas.dbModels import PandaJob
from sqlalchemy import select, update
import sys
import time

def main():
    if len(sys.argv) != 2:
        print("Usage: python aCTHeartbeatWatchdog.py timelimit")
        sys.exit(1)

    timelimit = int(sys.argv[1])

    # logger
    logger = aCTLogger('aCTHeartbeatWatchdog')
    log = logger()
    # database
    db = aCTDBPanda(log)

    # Query for running jobs with theartbeat longer than timelimit seconds ago,
    # and the proxy paths needed to talk to Panda for them
    with db.Session() as session:
        jobs = session.execute(select(PandaJob.pandaid, PandaJob.pandastatus, PandaJob.proxyid, PandaJob.siteName, PandaJob.theartbeat) \
                               .where(PandaJob.sendhb==1, PandaJob.pandastatus.in_(['sent', 'starting', 'running', 'transferring']),
                                      PandaJob.theartbeat!=0, db.timeStampLessThan(PandaJob.theartbeat, timelimit))).all()
        proxypaths = {proxyid: db.getProxyPath(session, proxyid) for proxyid in {job.proxyid for job in jobs}}

    if not jobs:
        return

    print('Found %d jobs with outdated heartbeat (older than %d seconds):\n' % (len(jobs), timelimit))
    print('\t'.join(['pandaid', 'site', 'status', 'theartbeat', 'Panda response']))

    # send heartbeats to Panda (one Panda client per proxy), no DB session open
    pandas = {proxyid: aCTPanda(log, path) for proxyid, path in proxypaths.items()}
    for job in jobs:
        response = pandas[job.proxyid].updateStatus(job.pandaid, job.pandastatus)
        print('\t'.join([str(job.pandaid), job.siteName, job.pandastatus, str(job.theartbeat), str(response)]))

    # update heartbeat time in the DB for all of them in one short transaction
    with db.Session.begin() as session:
        session.execute(update(PandaJob).where(PandaJob.pandaid.in_([job.pandaid for job in jobs]))
                        .values(theartbeat=db.getTimeStamp(time.time()+1)))


if __name__ == '__main__':
    main()
