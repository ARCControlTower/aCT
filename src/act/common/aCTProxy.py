import logging
from .aCTConfig import aCTConfigARC
import datetime, time
import arc
import subprocess
from act.arc.aCTDBArcNEW import aCTDBArc
from act.arc.dbModels import Proxy
from sqlalchemy import select, update

class aCTProxy:

    def __init__(self, logger, Interval=3600, db=None):
        self.interval = Interval
        self.conf = aCTConfigARC()
        self.db: aCTDBArc = db
        self.log = logger
        cred_type=arc.initializeCredentialsType(arc.initializeCredentialsType.SkipCredentials)
        self.uc=arc.UserConfig(cred_type)
        self.uc.CACertificatesDirectory(self.conf.voms.cacertdir)
        self.voms_proxies = {}

    def _timediffSeconds(self, t1, t2):
        '''
        Helper function. Takes datetime.datetime t1 and t2, returns t1-t2 in seconds
        '''
        return time.mktime(t1.timetuple())-time.mktime(t2.timetuple())

    def _readProxyFromFile(self, path):
        f = open(path)
        proxy = f.read()
        f.close()
        self.uc.CredentialString(str(proxy))
        cred=arc.Credential(self.uc)
        dn = cred.GetIdentityName()
        expirytime=datetime.datetime.strptime(cred.GetEndTime().str(arc.UTCTime),"%Y-%m-%dT%H:%M:%SZ")
        return proxy, dn, expirytime

    def _createVomsProxyFromFile(self, oldproxypath, newproxypath, validTime, voms, attribute=''):
        '''
        Helper function to create proxy under newproxypath from proxy under oldproxypath
        with given voms and attribute (if given), using arcproxy.
        '''
        cmd=[self.conf.voms.bindir+"/arcproxy"]
        cmd.extend(["--constraint=validityPeriod="+str(validTime)+"S"])
        cmd.extend(["--constraint=vomsACvalidityPeriod="+str(validTime)+"S"])
        if voms:
            cmd.extend(["--voms="+voms])
            if attribute:
                cmd[-1]+=":"+attribute
        cmd.extend(["--cert="+oldproxypath])
        cmd.extend(["--key="+oldproxypath])
        cmd.extend(["--proxy="+newproxypath])

        p = subprocess.Popen(cmd, stderr=subprocess.STDOUT, stdout=subprocess.PIPE)
        self.log.info('arcproxy returned:\n%s' % p.communicate()[0])
        return p

    def createVOMSAttribute(self, voms, attribute, proxypath="", validTime=345600, proxyid=None):
        '''
        Function to create proxy with voms extensions from proxy.
        Example: To add production attribute to atlas voms, set voms="atlas" and
        attribute="/atlas/Role=production". The proxy file under proxypath will
        be used to generate/update the proxy.
        If proxyid is None, a new proxy entry will be created.
        After a call to this function, the new proxy will be automatically renewed
        with a call to the renew() function.
        '''
        with self.db.Session.begin() as session:
            if not proxypath:
                proxypath=self.conf.voms.proxypath
            _, dn, expirytime = self._readProxyFromFile(proxypath)
            # if not given, try to get proxyid using dn and attribute first
            if not proxyid:
                proxyid = session.execute(select(Proxy.id).where(Proxy.dn==dn, Proxy.attribute==attribute)).scalar_one_or_none()
            # if still no proxyid, a new proxies table entry must be created
            if not proxyid:
                proxyid = self.db.insertProxy("", session, dn, expirytime, attribute)
            dbproxypath = session.execute(select(Proxy.proxypath).where(Proxy.id==proxyid)).scalar_one()
            retries = 3
            while self._createVomsProxyFromFile(proxypath, dbproxypath, validTime, voms, attribute).returncode:
                # todo: check that attribute is actually set in the new proxy.
                retries -= 1
                if retries == 0:
                    self.log.warning("Got errors when creating VOMS proxy from file %s", proxypath)
                    break
                #give arcproxy a bit of time before retrying
                time.sleep(1)
            proxy, _, expirytime = self._readProxyFromFile(dbproxypath)
            desc={"proxy":proxy, "expirytime":expirytime}
            session.execute(update(Proxy).where(Proxy.id==proxyid).values(**desc))
            self.voms_proxies[(dn, attribute)] = (voms, attribute, proxypath, validTime, proxyid)
        return proxyid

    def renew(self):
        "renews proxies in db. renews all proxies created with createVOMSRole."
        with self.db.Session() as session:
            for (dn, attribute), args in list(self.voms_proxies.items()):
                tleft = self.timeleft(dn, attribute, session)
                if tleft <= self.conf.voms.minlifetime:
                    self.createVOMSAttribute(*args)
                    tleft = self.timeleft(dn, attribute, session)
                    if tleft <= 0:
                        self.log.error("VOMS proxy not extended")

    def timeleft(self, dn, attribute, session):
        expirytime = session.execute(select(Proxy.expirytime).where(Proxy.dn==dn, Proxy.attribute==attribute)).scalar_one_or_none()
        if expirytime:
            total_seconds = self._timediffSeconds(expirytime, datetime.datetime.utcnow())
            return total_seconds
        else:
            return 0

def test_aCTProxy():
    p=aCTProxy(logging.getLogger(), 1)
    voms="atlas"
    attribute="/atlas/Role=pilot"
    proxypath=p.conf.voms.proxypath
    validTime=43200
    proxyid = p.createVOMSAttribute(voms, "/atlas/Role=pilot", proxypath, validTime)
    proxyid = p.createVOMSAttribute(voms, "/atlas/Role=production", proxypath, validTime)
    dn = p.db.getProxiesInfo("id="+str(proxyid), ["dn"], expect_one=True)["dn"]
    print("dn=", dn)
    print("path from dn,attribute lookup matches path from proxyid lookup:", end=' ')
    time.sleep(30)
    p.renew()

if __name__ == '__main__':
    test_aCTProxy()

