from pyarcrest.arc import ARCJob


class ACTJob:

    def __init__(self):
        self.arcjob = ARCJob()
        self.arcid = None
        self.appid = None
        self.proxyid = None
        self.state = None
        self.tstate = None
        self.tarcstate = None
        self.tcreated = None
        self.attemptsleft = None
        self.clusterList = None

    def loadARCDBJob(self, dbdict):
        if "IDFromEndpoint" in dbdict:
            self.arcjob.id = dbdict["IDFromEndpoint"]

        if "id" in dbdict:
            self.arcid = dbdict["id"]

        if "appjobid" in dbdict:
            self.appid = dbdict["appjobid"]

        if "proxyid" in dbdict:
            self.proxyid = dbdict["proxyid"]

        if "State" in dbdict:
            self.arcjob.state = dbdict["State"]

        if "tstate" in dbdict:
            self.tstate = dbdict["tstate"]

        if "arcstate" in dbdict:
            self.state = dbdict["arcstate"]

        if "tarcstate" in dbdict:
            self.tarcstate = dbdict["tarcstate"]

        if "created" in dbdict:
            self.tcreated = dbdict["created"]

        if "downloadfiles" in dbdict:
            self.arcjob.downloadFiles = dbdict["downloadfiles"].split(";")

        if "attemptsleft" in dbdict:
            self.attemptsleft = dbdict["attemptsleft"]

        if "clusterlist" in dbdict:
            self.clusterList = dbdict["clusterlist"].split(",")
