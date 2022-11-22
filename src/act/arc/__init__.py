from act.arc.aCTCleaner import aCTCleaner
from act.arc.aCTFetcher import aCTFetcher
from act.arc.aCTStatus import aCTStatus
from act.arc.aCTSubmitter import aCTSubmitter
from act.common.aCTMonitor import aCTMonitor
from act.common.aCTProxyHandler import aCTProxyHandler

processes = {
    'submitter': [aCTSubmitter],
    'cluster': [aCTCleaner, aCTFetcher, aCTStatus],
    'single': [aCTProxyHandler, aCTMonitor]
}
