# Handler for fetching site info from CRIC. Fetches data and stores it in json file.

import os
import signal
import threading
import urllib.error
from datetime import datetime, timedelta

from act.atlas.aCTATLASProcess import aCTATLASProcess
from pyarcrest.http import HTTPClient


class aCTCRICFetcher(aCTATLASProcess):

    def setup(self):
        super().setup()
        self.queues = self.conf.cric.server
        self.queuesfile = self.conf.cric.jsonfilename

        # TODO: aCTCRICFetcher is the only process that sleeps a very long
        # period of time, unacceptable for the service stop mechanism.
        # This is currently a quick hack. What should be a more generic API
        # for such functionality? (Especially, how could the configuration
        # of arbitrary waiting period be exposed to the developer in the most
        # elegant way?)
        self.terminate = threading.Event()
        signal.signal(signal.SIGTERM, self.exitHandler)

    def exitHandler(self, signum, frame):
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        self.mustExit = True
        self.terminate.set()

    def wait(self):
        # avoid too much cric fetching
        self.terminate.wait(600)

    def fetchFromCRIC(self, url, filename):
        self.log.debug(f"Downloading from {url}")
        try:
            client = HTTPClient(url)
            response = client.request("GET", url)
            urldata = response.read().decode()
            if response.status != 200:
                raise Exception(f"Invalid response status for URL {url}: {response.status}")
            self.log.debug(f"Fetched {url}")
            return urldata
        except Exception as e:
            self.log.warning("Failed to contact CRIC: %s" % str(e))
            # Check if the cached data is getting old, if so raise a critical error
            try:
                mtime = os.stat(filename).st_mtime
                if datetime.fromtimestamp(mtime) < datetime.now() - timedelta(hours=1):
                    self.log.critical("CRIC info has not been updated since more than 1 hour ago")
                    self.criticallog.critical("CRIC info has not been updated since more than 1 hour ago")
            except:
                # file may not have been created yet
                pass
            return ''

    def storeToFile(self, cricjson, filename):
        if not cricjson:
            return
        tmpfile=filename+'_'
        os.makedirs(tmpfile[:tmpfile.rfind('/')], 0o755, exist_ok=True)
        with open(tmpfile, 'w') as f:
            f.write(cricjson)

        os.rename(tmpfile, filename)
        self.log.debug("Wrote "+filename)

    def process(self):
        """
        Main loop
        """
        self.log.info("Running")
        # todo: check if cric.json exists and return if too new
        # fetch data from CRIC
        queuesjson = self.fetchFromCRIC(self.queues, self.queuesfile)
        # store data to file
        self.storeToFile(queuesjson, self.queuesfile)
