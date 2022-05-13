# Handler for fetching site info from CRIC. Fetches data and stores it in json file.

from datetime import datetime, timedelta
import os
import time
import urllib.request, urllib.error

from act.atlas.aCTATLASProcess import aCTATLASProcess

class aCTCRICFetcher(aCTATLASProcess):

    def __init__(self):
        aCTATLASProcess.__init__(self)
        self.queues = self.conf.cric.server
        self.oses = self.conf.cric.objectstores
        self.queuesfile = self.conf.cric.jsonfilename
        self.osesfile = self.conf.cric.osfilename


    def fetchFromCRIC(self, url, filename):
        try:
            self.log.debug("Downloading from %s" % url)
            response = urllib.request.urlopen(url, timeout=60)
        except urllib.error.URLError as e:
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

        urldata = response.read().decode('utf-8')
        self.log.debug("Fetched %s" % url)
        return urldata

    def storeToFile(self, cricjson, filename):
        if not cricjson:
            return
        tmpfile=filename+'_'
        try:
            with open(tmpfile, 'w') as f:
                f.write(cricjson)
        except:
            os.makedirs(tmpfile[:tmpfile.rfind('/')], 0o755)
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
        osesjson = self.fetchFromCRIC(self.oses, self.osesfile)
        # store data to file
        self.storeToFile(queuesjson, self.queuesfile)
        self.storeToFile(osesjson, self.osesfile)
        # temporary hack to avoid too much cric fetching
        time.sleep(600)

if __name__ == '__main__':
    acf=aCTCRICFetcher()
    acf.run()
