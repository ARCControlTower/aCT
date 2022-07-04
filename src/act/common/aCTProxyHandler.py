# aCTProxyHandler.py
#
# Handles proxy updates in proxies table
#

from act.common.aCTConfig import aCTConfigARC
from act.common.aCTProcess import aCTProcess
from act.common.aCTProxy import aCTProxy

import datetime

class aCTProxyHandler(aCTProcess):

    def __init__(self):
        aCTProcess.__init__(self)
        self.conf = aCTConfigARC()
        self.pm = aCTProxy(self.log)
        self.tstamp = datetime.datetime.utcnow()-datetime.timedelta(0,self.pm.interval)
        if self._updateLocalProxies() == 0:
            # no local proxies in proxies table yet, better populate it
            self._updateRolesFromConfig()
        self._updateMyProxies()

    def _checkProxyLifetime(self, proxylifetime):
        # enforcing max limit of 96 hours since this is the maximum lifetime of voms attrs
        if proxylifetime > 345600:
            self.log.warning("voms proxylifetime was higher than the allowed max time of 96 hours. Reducing to 96 hours.")
            return 345600
        else:
            return proxylifetime

    def _updateRolesFromConfig(self):
        vo = self.conf.voms.vo
        validTime = self._checkProxyLifetime(self.conf.voms.proxylifetime)
        proxypath = self.conf.voms.proxypath
        # TODO: roles should be taken from AGIIS
        roles = self.conf.voms.roles
        if not roles:
            self.pm.createVOMSAttribute(vo, '', proxypath, validTime)
        else:
            for role in roles:
                attribute = "/"+vo+"/Role="+role
                self.pm.createVOMSAttribute(vo, attribute, proxypath, validTime)

    def _updateLocalProxies(self):
        """
        Function to get local proxies to be updated in proxies table.
        """
        select = "proxytype='local'"
        columns = ["dn","attribute","proxypath","id"]
        ret_columns = self.pm.db.getProxiesInfo(select, columns)
        vo = self.conf.voms.vo
        validTime = self._checkProxyLifetime(self.conf.voms.proxylifetime)
        for row in ret_columns:
            dn = row["dn"]
            attribute = row["attribute"]
            proxypath = self.conf.voms.proxypath
            proxyid = row["id"]
            self.pm.voms_proxies[(dn, attribute)] = (vo, attribute, proxypath, validTime, proxyid)
        return len(ret_columns)

    def _updateMyProxies(self):
        return None

    def renewProxies(self):
        self.log.info("renewing proxies")
        self.pm.renew()

    def process(self):
        # renew proxies
        t=datetime.datetime.utcnow()
        if self.pm._timediffSeconds(t, self.tstamp) >= self.pm.interval:
            self.renewProxies()
            self.tstamp = t

if __name__ == '__main__':
    st=aCTProxyHandler()
    st.run()
    st.finish()
