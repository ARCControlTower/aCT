# Tool for detecting critical errors in log in the last hour. Can be run as a cron
# to send emails.
# Critical log lines look like
# [2015-07-28 11:26:43,780] [aCTProcess.py:89] [CRITICAL] [arc01.lcg.cscs.ch/atlas] - Traceback (most recent call last):
#

from act.common.aCTConfig import aCTConfigARC
from datetime import datetime, timedelta
import re

import psutil

def main():

    conf = aCTConfigARC()
    criticallog = f'{conf.logger.logdir}/aCTCritical.log'
    criticalerrors = 0
    lastcritical = ''
    now = datetime.now()

    with open(criticallog) as f:
        for line in f:
            t = re.match('\[(\d\d\d\d\-\d\d\-\d\d\s\d\d:\d\d:\d\d,\d\d\d)\].*\[CRITICAL\]', line)
            if t:
                if abs(now-datetime.strptime(t.group(1), '%Y-%m-%d %H:%M:%S,%f')) < timedelta(hours=1):
                    criticalerrors += 1
                    lastcritical = line
            else:
                lastcritical += line

    if criticalerrors:
        print('%d critical errors in the last hour\n' % criticalerrors)
        print('Last critical error:\n%s' % lastcritical)

    for p in psutil.process_iter(['pid','ppid','name','username']):
        if p.ppid() == 1 and p.name() == 'arc-dmcgridftp':
          print("arc-dmcgridftp killed: ",p.pid)
          p.kill()

if __name__ == '__main__':
    main()
