#!/usr/bin/python

import classad
import logging
from act.arc.aCTDBArc import aCTDBArc
from act.condor.aCTDBCondor import aCTDBCondor
from act.common.aCTProxy import aCTProxy
from act.common.aCTLogger import aCTLogger

logger = aCTLogger('acttest', cluster='test')
log = logger()

db = aCTDBArc(log)
dbcondor = aCTDBCondor(log)

xrsl = '''&(executable=/bin/sleep)
           (arguments=100)
           (stdout=stdout)
           (rerun=2)
           (gmlog=gmlog)
           (inputfiles = ("runpilot2-wrapper.sh" "/cvmfs/atlas.cern.ch/repo/sw/PandaPilotWrapper/latest/runpilot2-wrapper.sh"))
           (runtimeenvironment = "ENV/PROXY")
           '''

cad = classad.ClassAd('''\n
    [\n
            UserLog = "/tmp/log/test.$(ClusterId).log"; \n
            Out = "/tmp/output/test.$(ClusterId).out"; \n
            Err = "/tmp/error/test.$(ClusterId).err"; \n
            JobUniverse = 9; \n
            X509UserProxy = "/tmp/x509up_u100780"; \n
            Arguments = "-c 1 -t 60"; \n
            Cmd = "/usr/bin/stress"; \n
            JobPrio = 50; \n
    ]
    ''')

cad = {
    'UserLog': "/tmp/log/test.$(ClusterId).log",
    'Output': "/tmp/output/test.$(ClusterId).out",
    'Error': "/tmp/error/test.$(ClusterId).err",
    'JobUniverse': '9',
    'X509UserProxy': "/tmp/x509up_u100780",
    'Arguments': "60",
    'Cmd': "/usr/bin/sleep",
    'JobPrio': '50'
    }

p=aCTProxy(logging.getLogger(), 1)
voms="atlas"
attribute="" # e.g. attribute="/atlas/Role=production"
proxypath=p.conf.voms.proxypath
validHours=5
proxyid=1 # p.createVOMSAttribute(voms, attribute, proxypath, validHours)

db.insertArcJobDescription(xrsl, clusterlist='gsiftp://pcoslo5.dyndns.cern.ch/fork', proxyid=1, maxattempts=5)
#dbcondor.insertCondorJobDescription(cad, clusterlist='nordugrid pcoslo5.cern.ch', proxyid=proxyid, maxattempts=5)
#dbcondor.insertCondorJobDescription(cad, clusterlist='condor ce503.cern.ch ce503.cern.ch:9619', proxyid=proxyid, maxattempts=5)
#dbcondor.insertCondorJobDescription(cad, clusterlist='condor gridgk01.racf.bnl.gov gridgk01.racf.bnl.gov:9619', proxyid=proxyid, maxattempts=5)
