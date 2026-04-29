from act.atlas.aCTATLASStatus import aCTATLASStatus
from act.atlas.aCTAutopilot import aCTAutopilot
from act.atlas.aCTAutopilotSent import aCTAutopilotSent
from act.atlas.aCTCRICFetcher import aCTCRICFetcher
from act.atlas.aCTPanda2Arc import aCTPanda2Arc
from act.atlas.aCTValidator import aCTValidator

processes = {
    'single': [
        aCTCRICFetcher,
        aCTATLASStatus,
        aCTAutopilot,
        aCTAutopilotSent,
        aCTPanda2Arc,
        aCTValidator,
    ]
}
