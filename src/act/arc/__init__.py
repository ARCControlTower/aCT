from act.arc.aCTSubmitter import aCTSubmitter
from act.arc.aCTCleaner import aCTCleaner
from act.arc.aCTFetcher import aCTFetcher
from act.arc.aCTStatus import aCTStatus
from act.common.aCTProxyHandler import aCTProxyHandler
from act.common.aCTMonitor import aCTMonitor


processes = {
    'submitter': {
        'aCTSubmitter': aCTSubmitter
    },
    'cluster': {
        'aCTCleaner': aCTCleaner,
        'aCTFetcher': aCTFetcher,
        'aCTStatus': aCTStatus
    },
    'single': {
        'aCTProxyHandler': aCTProxyHandler,
        'aCTMonitor': aCTMonitor
    }
}
