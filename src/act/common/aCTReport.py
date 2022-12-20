import argparse
import importlib
import logging
import os
import re
import subprocess
import sys
import time

from act.arc import aCTDBArc
from act.common import aCTLogger
from act.common.aCTConfig import aCTConfigAPP


class aCTReport:
    '''Print summary info on jobs in DB. Use --web to print html that is
    automatically refreshed. Add filenames to query more than one aCT DB'''

    def __init__(self, args):
        self.output = ""
        self.outfile = args.web
        self.actconfs = args.conffiles or [''] # empty string for default behaviour

        self.logger=aCTLogger.aCTLogger("aCTReport", stderr=True)
        self.actlog=self.logger()
        self.actlog.logger.setLevel(logging.INFO)

        if self.outfile:
            self.log('<META HTTP-EQUIV="refresh" CONTENT="60"><pre>')
            self.log(time.asctime() + '\n')

        self.db=aCTDBArc.aCTDBArc(self.actlog)

    def log(self, message=''):
        self.output += message + '\n'

    def AppReport(self):

        appconf = aCTConfigAPP()
        modules = appconf.modules
        for module in modules:
            try:
                ap = importlib.import_module(f'{module}.aCTReport').report
                self.log(ap(self.actconfs))
            except ModuleNotFoundError:
                self.actlog.info(f'No report in module {module}')
            except AttributeError:
                self.actlog.info(f'aCTReport.report() not found in {module}')
            except Exception as e:
                self.actlog.error(f'Exception running {module}.aCTReport.report: {e}')

    def ProcessReport(self):
        if self.actconfs != ['']:
            return # don't print processes for combined report

        # check if aCT is running
        actprocscmd = 'ps ax -o pid,ppid,args'
        try:
            out = subprocess.run(actprocscmd.split(), check=True, encoding='utf-8', stdout=subprocess.PIPE).stdout
        except subprocess.CalledProcessError as e:
            self.log(f'Error: could not run ps command: {e.stderr}')
            return

        # find aCTMain to see if aCT is running and get its PID and get lines
        # of act processes
        mainpid = None
        actlines = []
        for line in out.splitlines():
            parts = line.split()
            if parts[2] == 'aCTMain':
                mainpid = int(parts[0])
            elif parts[2].startswith('aCT'):
                actlines.append(parts)
        if mainpid is None:
            return

        cluster_procs = {}
        for parts in actlines:
            # skip as not aCT process (ppid != mainpid)
            if int(parts[1]) != mainpid:
                continue
            if len(parts) > 3:  # cluster process
                cluster_procs.setdefault(parts[3], []).append(parts[2])
            else:
                cluster_procs.setdefault('(no cluster defined)', []).append(parts[2])

        self.log('Active processes per cluster:')
        for cluster in sorted(cluster_procs):
            procs = cluster_procs[cluster]
            procs.sort()
            self.log(f'{cluster:>38.38}: {" ".join(procs)}')

    def ArcJobReport(self):
        rep={}
        rtot={}
        states = ["Undefined", "Accepting", "Accepted", "Preparing", "Submitting",
                 "Queuing", "Running", "Finishing", "Finished", "Hold", "Killed",
                 "Failed", "Deleted", "Other"]

        for conf in self.actconfs:
            if conf:
                os.environ['ACTCONFIGARC'] = conf

            db=aCTDBArc.aCTDBArc(self.actlog)
            c=db.db.conn.cursor()
            c.execute("select jobid,state from arcjobs")
            rows=c.fetchall()
            for r in rows:

                reg=re.search('.+//([^:]+)',str(r[0]))
                cl=""
                try:
                    cl=reg.group(1)
                except:
                    cl='WaitingSubmission'

                jid=str(r[1])
                if jid == 'None':
                    jid="Other"

                try:
                    rep[cl][jid]+=1
                except:
                    try:
                        rep[cl][jid]=1
                    except:
                        rep[cl]={}
                        rep[cl][jid]=1
                try:
                    rtot[jid]+=1
                except:
                    rtot[jid]=1

        if sum(rtot.values()) == 0:
            return
        self.log(f"All ARC jobs: {sum(rtot.values())}")
        self.log(f"{'':39} {' '.join([f'{s:>9}' for s in states])}")

        for k in sorted(rep, key=lambda x: x.split('.')[-1]):
            log=f"{k:>38.38}:"
            for s in states:
                try:
                    log += f'{rep[k][s]:>10}'
                except KeyError:
                    log += f'{"-":>10}'
            self.log(log)
        log = f"{'Totals':>38}:"
        for s in states:
            try:
                log += f'{rtot[s]:>10}'
            except:
                log += f'{"-":>10}'
        self.log(log+'\n\n')

    def CondorJobReport(self):

        rep = {}
        rtot = {}
        condorjobstatemap = ['Undefined', # used before real state is known
                             'Idle',
                             'Running',
                             'Removed',
                             'Completed',
                             'Held',
                             'Transferring',
                             'Suspended']

        for conf in self.actconfs:
            if conf:
                os.environ['ACTCONFIGARC'] = conf

            db=aCTDBArc.aCTDBArc(self.actlog)
            c = db.db.conn.cursor()
            c.execute("select cluster, JobStatus from condorjobs")
            rows = c.fetchall()

            for r in rows:

                cl = str(r[0])
                if not cl:
                    cl = 'WaitingSubmission'

                jid = r[1]

                try:
                    rep[cl][jid]+=1
                except:
                    try:
                        rep[cl][jid]=1
                    except:
                        rep[cl]={}
                        rep[cl][jid]=1
                try:
                    rtot[jid]+=1
                except:
                    rtot[jid]=1

        if sum(rtot.values()) == 0:
            return
        self.log(f"All Condor jobs: {sum(rtot.values())}")
        self.log(f"{'':39} {' '.join([f'{s:>9}' for s in condorjobstatemap])}")
        for k in sorted(rep, key=lambda x: x.split('.')[-1]):
            log=f"{k:>38.38}:"
            for s in range(8):
                try:
                    log += f'{rep[k][s]:>10}'
                except KeyError:
                    log += f'{"-":>10}'
            self.log(log)
        log = f"{'Totals':>38}:"
        for s in range(8):
            try:
                log += f'{rtot[s]:>10}'
            except:
                log += f'{"-":>10}'
        self.log(log+'\n\n')


    def StuckReport(self):

        # Query for lost jobs older than lostlimit
        lostlimit = 86400
        select = "(arcstate='submitted' or arcstate='running') and " \
                 + self.db.timeStampLessThan("tarcstate", lostlimit) + \
                 " order by tarcstate"
        columns = ['cluster']
        jobs = self.db.getArcJobsInfo(select, columns)

        if jobs:
            self.log('Found %d jobs not updated in over %d seconds:\n' % (len(jobs), lostlimit))

            clustercount = {}
            for job in jobs:
                try:
                    host = re.search('.+//([^:]+)', job['cluster']).group(1)
                except:
                    host = None
                if host in clustercount:
                    clustercount[host] += 1
                else:
                    clustercount[host] = 1

            for cluster, count in clustercount.items():
                self.log(f'{count} {cluster}')
            self.log()


    def end(self):
        if self.outfile:
            self.log('</pre>')


def main():
    parser = argparse.ArgumentParser(description='Report table of aCT jobs.')
    parser.add_argument('conffiles', nargs='*', help='list of configuration files')
    parser.add_argument('--web', help='Output suitable for web page')
    parser.add_argument('--harvester', action='store_true', help='Dummy arg for backwards compatibility')
    args = parser.parse_args(sys.argv[1:])

    acts = aCTReport(args)
    acts.AppReport()
    acts.ArcJobReport()
    acts.CondorJobReport()
    acts.StuckReport()
    acts.ProcessReport()
    acts.end()
    if acts.outfile is None:
        sys.stdout.write(acts.output)
    else:
        f=open(acts.outfile,"w")
        f.write(acts.output)
        f.close()

if __name__ == '__main__':
    main()
