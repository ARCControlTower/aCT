from collections import defaultdict
from act.common.aCTLogger import aCTLogger
from act.ldmx.aCTDBLDMX import aCTDBLDMX

def report( printLog ):

    # Get current jobs
    actlogger = aCTLogger('aCTReport', stderr=True)
    logger = actlogger()
    rep = defaultdict(lambda: defaultdict(int))
    rtot = defaultdict(int)
    log = ''
    states = ["new", "waiting", "queueing", "running", "finishing", "registering", "toresubmit",
              "toclean" ] #, "finished", "failed", "tocancel", "cancelling", "cancelled"]

    db = aCTDBLDMX(logger)
    #rows = db.getGroupedJobs('batchid, ldmxstatus')
    rows = db.getGroupedJobs('batchname, ldmxstatus')
    for r in rows:
        count, state, site = (r['count(*)'], r['ldmxstatus'], r['batchname'] or 'None')
        #count, state, site = (r['count(*)'], r['ldmxstatus'], r['batchid'] or 'None')
        rep[site][state] += count
        rtot[state] += count

    # figure out batchid column length (min 10)
    maxbatchlen = max([len(k) for k in rep]+[10]) + 1

    log += f"Active LDMX job batches: {len(rep)}\n"
    log += f"{'':{maxbatchlen+1}} {' '.join([f'{s:>9}' for s in states])}   Total\n"

    totStillActive=0
    for k in sorted(rep.keys(), key=lambda x: x != None):
        log += f"{k:>{maxbatchlen}.{maxbatchlen}}:"
        log += ''.join([f'{(rep[k][s] or "-"):>10}' for s in states])
        log += f"{sum(rep[k].values()):>10}"
        log += '\n'
        for s in states :
            totStillActive += (rep[k][s] or 0) 

    log += f'{"Totals":>{maxbatchlen}}:'
    log += ''.join([f'{(rtot[s] or "-"):>10}' for s in states])
    log += f"{sum(rtot.values()):>10}"
    log += '\n\n'

    # only for debugging 
    if printLog :
        print( log )
    return totStillActive


if __name__ == '__main__':
    printOutLog=False
    n=report(printOutLog)
    print(n)
