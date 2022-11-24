from act.ldmx.aCTLDMX2Arc import aCTLDMX2Arc
from act.ldmx.aCTLDMXGetJobs import aCTLDMXGetJobs
from act.ldmx.aCTLDMXRegister import aCTLDMXRegister
from act.ldmx.aCTLDMXStatus import aCTLDMXStatus
from act.ldmx.aCTRucioMonitor import aCTRucioMonitor

processes = {
    'single': [
        aCTLDMX2Arc,
        aCTLDMXGetJobs,
        aCTLDMXRegister,
        aCTLDMXStatus,
        aCTRucioMonitor,
    ]
}
