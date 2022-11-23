from act.atlas.aCTCRICFetcher import aCTCRICFetcher
from act.atlas.aCTATLASStatus import aCTATLASStatus
from act.atlas.aCTATLASStatusCondor import aCTATLASStatusCondor
from act.atlas.aCTAutopilot import aCTAutopilot
from act.atlas.aCTAutopilotSent import aCTAutopilotSent
from act.atlas.aCTPanda2Arc import aCTPanda2Arc
from act.atlas.aCTPanda2Condor import aCTPanda2Condor
from act.atlas.aCTValidator import aCTValidator
from act.atlas.aCTValidatorCondor import aCTValidatorCondor

processes = {
    'single': [
        aCTCRICFetcher,
        aCTATLASStatus,
        aCTATLASStatusCondor,
        aCTAutopilot,
        aCTAutopilotSent,
        aCTPanda2Arc,
        aCTPanda2Condor,
        aCTValidator,
        aCTValidatorCondor,
    ]
}
