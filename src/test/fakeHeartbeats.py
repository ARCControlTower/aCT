#!/usr/bin/python
#
# Get the list of jobs from panda for the configured scheduler ID and send a
# heartbeat for starting and running jobs.
#

from act.atlas.aCTPanda import aCTPanda

import logging
import sys

if len(sys.argv) != 2:
    print "Usage: fakeHeartbeats.py <path to proxy>"
    sys.exit(0)

logging.basicConfig(level=logging.DEBUG)
p = aCTPanda(logging.getLogger('test'), sys.argv[1])

for cloud in ['ND', 'CA', 'CERN', 'DE', 'ES', 'FR', 'IT', 'NL', 'TW', 'UK', 'US']:

    print (f'Cloud {cloud}')
    jobs = p.queryJobInfo(cloud=cloud)
    for j in jobs:
        if j['jobStatus'] == 'starting' or j['jobStatus'] == 'running':
            print j
            p.updateStatus(j['PandaID'], j['jobStatus'])
