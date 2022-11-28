from collections import defaultdict
from act.common.aCTLogger import aCTLogger
from act.ldmx.aCTDBLDMX import aCTDBLDMX

def report(actconfs):

    # Get current jobs
    actlogger = aCTLogger('aCTReport')
    logger = actlogger()
    rep = defaultdict(lambda: defaultdict(int))
    rtot = defaultdict(int)
    log = ''
    states = ["new", "waiting", "queueing", "running", "finishing", "registering", "toresubmit",
              "toclean", "finished", "failed", "tocancel", "cancelling", "cancelled"]

    db = aCTDBLDMX(logger)
    rows = db.getGroupedJobs('batchname, ldmxstatus')
    for r in rows:
        count, state, site = (r['count(*)'], r['ldmxstatus'], r['batchname'] or 'None')
        rep[site][state] += count
        rtot[state] += count

    # figure out batchid column length (min 10)
    maxbatchlen = max([len(k) for k in rep]+[10]) + 1

    log += f"Active LDMX job batches: {len(rep)}\n"
    log += f"{'':{maxbatchlen+1}} {' '.join([f'{s:>9}' for s in states])}   Total\n"

    for k in sorted(rep.keys(), key=lambda x: x != None):
        log += f"{k:>{maxbatchlen}.{maxbatchlen}}:"
        log += ''.join([f'{(rep[k][s] or "-"):>10}' for s in states])
        log += f"{sum(rep[k].values()):>10}"
        log += '\n'

    log += f'{"Totals":>{maxbatchlen}}:'
    log += ''.join([f'{(rtot[s] or "-"):>10}' for s in states])
    log += f"{sum(rtot.values()):>10}"
    log += '\n\n'

    rep = defaultdict(lambda: defaultdict(int))
    rtot = defaultdict(int)

    rows = db.getJobs('True', ['sitename', 'ldmxstatus'])
    for r in rows:

        site, state = (r['sitename'] or 'None', r['ldmxstatus'])
        rep[site][state] += 1
        rtot[state] += 1

    log += f"Active LDMX jobs by site: {sum(rtot.values())}\n"
    log += f"{'':{maxbatchlen+1}} {' '.join([f'{s:>9}' for s in states])}\n"

    for k in sorted(rep.keys(), key=lambda x: x != None):
        log += '{:>{width}.{width}}:'.format(k, width=maxbatchlen)
        log += ''.join([f'{(rep[k][s] or "-"):>10}' for s in states])
        log += '\n'

    log += f'{"Totals":>{maxbatchlen}}:'
    log += ''.join([f'{(rtot[s] or "-"):>10}' for s in states])
    log += '\n\n'

    # Summary from archive
    states = ['finished', 'failed', 'cancelled']
    rep = defaultdict(lambda: defaultdict(int))
    rtot = defaultdict(int)
    # Show only batches from last month
    rows = db.getGroupedArchiveJobs('batchid, ldmxstatus', limit=60*60*24*30)
    for r in rows:
        count, state, batch = (r['count(*)'], r['ldmxstatus'], r['batchid'] or 'None')
        rep[batch][state] += count
        rtot[state] += count

    # figure out batchid column length (min 10)
    maxbatchlen = max([len(k) for k in rep]+[10]) + 1

    log += f"Completed LDMX batches: {len(rep)}\n"
    log += f"{'':{maxbatchlen+1}} {' '.join([f'{s:>9}' for s in states+['Total']])}\n"

    for k in sorted(rep.keys(), key=lambda x: x != None):
        log += f"{k:>{maxbatchlen}.{maxbatchlen}}:"
        log += ''.join([f'{(rep[k][s] or "-"):>10}' for s in states])
        log += f"{sum(rep[k].values()):>10}"
        log += '\n'

    log += f'{"Totals":>{maxbatchlen}}:'
    log += ''.join([f'{(rtot[s] or "-"):>10}' for s in states])
    log += f"{sum(rtot.values()):>10}"

    return log+'\n\n'
