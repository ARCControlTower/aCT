import os
import sys

from act.common.aCTLogger import aCTLogger
from act.atlas.aCTDBPanda import aCTDBPanda

def report(actconfs):
    actlogger = aCTLogger('aCTReport')
    logger = actlogger()
    rep = {}
    rtot = {}
    log = ''
    states = ["sent", "starting", "running", "slots", "transferring", "tovalidate", "toresubmit",
              "toclean", "finished", "done", "failed", "donefailed",
              "tobekilled", "cancelled", "donecancelled"]

    for conf in actconfs:
        if conf:
            os.environ['ACTCONFIGARC'] = conf

        db = aCTDBPanda(logger)
        c = db.db.conn.cursor()
        c.execute("select sitename, actpandastatus, corecount from pandajobs")
        rows = c.fetchall()
        for r in rows:

            site, state = (str(r[0]), str(r[1]))
            if r[2] is None:
                corecount = 1
            else:
                corecount = int(r[2])

            try:
                rep[site][state] += 1
                if state == "running":
                    rep[site]["slots"] += corecount
            except:
                try:
                    rep[site][state] = 1
                    if state == "running":
                        try:
                            rep[site]["slots"] += corecount
                        except:
                            rep[site]["slots"] = corecount
                except:
                    rep[site] = {}
                    rep[site][state] = 1
                    if state == "running":
                        rep[site]["slots"] = corecount
            try:
                rtot[state] += 1
                if state == "running":
                    rtot["slots"] += corecount
            except:
                rtot[state] = 1
                if state == "running":
                    rtot["slots"] = corecount

    log += f"All Panda jobs: {sum([v for k,v in rtot.items() if k != 'slots'])}\n"
    log += f"{'':29} {' '.join([f'{s:>9}' for s in states])}\n"

    for k in sorted(rep.keys()):
        log += f"{k:>28.28}:"
        for s in states:
            try:
                log += f'{rep[k][s]:>10}'
            except KeyError:
                log += f'{"-":>10}'
        log += '\n'

    log += f'{"Totals":>28}:'
    for s in states:
        try:
            log += f'{rtot[s]:>10}'
        except:
            log += f'{"-":>10}'
    log += '\n\n'
    if len(actconfs) == 1:
        log += HarvesterReport()
    return log

def HarvesterReport():
    log = ''
    try:
        from distutils.sysconfig import get_python_lib # pylint: disable=import-error
        sys.path.append(get_python_lib()+'/pandacommon')

        os.environ['PANDA_HOME']=os.environ['VIRTUAL_ENV']

        from collections import defaultdict # pylint: disable=import-error
        from pandaharvester.harvestercore.db_proxy_pool import DBProxyPool as DBProxy # pylint: disable=import-error

        dbProxy = DBProxy()

        workers = dbProxy.get_worker_stats_bulk(None)
        rep = defaultdict(dict)

        rtot = defaultdict(int)

        for site, prodsourcelabels in workers.items():
            for prodsourcelabel, resources in prodsourcelabels.items():
                for resource, jobs in resources.items():
                    rep[f'{site}-{resource}'][prodsourcelabel or 'empty'] = jobs
                    for state, count in jobs.items():
                        rtot[state] += count
        log = f"All Harvester jobs: {sum(rtot.values())}       prodSourceLabel: submitted/running\n"
        for k in sorted(rep.keys()):
            log += f"{k:>28.28}:"
            for psl, jobs in rep[k].items():
                log += f"{psl:>10}: {jobs['submitted']}/{jobs['running']}"
            log += '\n'
        log += f"{'Totals':>28}:  submitted: {rtot['submitted']}  running: {rtot['running']}\n\n"
    except:
        log += "Harvester db lookup error"

    return log
