from act.condor.aCTCleaner import aCTCleaner
from act.condor.aCTFetcher import aCTFetcher
from act.condor.aCTStatus import aCTStatus
from act.condor.aCTSubmitter import aCTSubmitter

processes = {
    'submitter': [aCTSubmitter],
    'cluster': [aCTCleaner, aCTFetcher, aCTStatus]
}
