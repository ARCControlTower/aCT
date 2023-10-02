"""
This is a module that provides proxy management functionality.
"""

import logging
import os
import arc
import datetime
import re

from act.common.aCTProxy import aCTProxy
from act.arc.aCTDBArc import aCTDBArc
from act.client.errors import NoSuchProxyError, NoProxyFileError
from act.client.errors import ProxyFileExpiredError, ProxyDBExpiredError

DEFAULT_PROXY_PATH = '/tmp/x509up_u'


class ProxyManager(object):
    """
    Object for managing proxies with aCT functions.

    This object tries to provide more convenient interface for proxy
    certificate management. Proxy certificates are stored by aCT in a
    dedicated table in database.

    MySQL errors that might happen are ignored unless stated otherwise where
    they are not.

    Attributes:
        logger: An object for logging.
        actproxy: An :class:~`act.common.aCTProxy.aCTProxy` object that
            provides interface to proxies table in aCT.
        arcdb: An object that is interface to ARC engine's table.
    """

    def __init__(self, db=None):
        """Initialize object."""
        self.log = logging.getLogger(__name__)
        self.actproxy = aCTProxy(self.log, db=db)
        self.arcdb = aCTDBArc(self.log, db=db)

    def getProxyInfo(self, dn, attribute='', columns=[]):
        """
        Return proxy information from database.

        Args:
            dn: A string with DN of proxy.
            attribute: A string with proxy attributes of proxy.
            columns: A list of string names of table columns.

        Returns:
            A dictionary with column_name: value entries.

        Raises:
            NoSuchProxyError: Searched for proxy is not in database.
        """
        try:
            proxyInfo =  self.actproxy.getProxyInfo(dn, attribute, columns)
        except Exception as exc:
            self.log.error(f'Error getting info for proxy dn={dn} attribute={attribute}: {exc}')
            raise
        else:
            if not proxyInfo:
                self.log.error(f'No proxy with dn={dn} and attribute={attribute}')
                raise NoSuchProxyError(dn, attribute)
            else:
                return proxyInfo

    def readProxyFile(self, proxyPath):
        """
        Read proxy info from file.

        Args:
            proxyPath: A string with path to proxy file.

        Returns:
            A tuple with proxy string, dn and expiry time.

        Raises:
            NoProxyFileError: Proxy was not found in a given file.
            ProxyFileExpiredError: Proxy has expired.
        """
        if not os.path.isfile(proxyPath):
            raise NoProxyFileError(proxyPath)
        try:
            proxystr, dn, expirytime = self.actproxy._readProxyFromFile(proxyPath)
        except Exception as exc:
            self.log.error(f'Error reading proxy file {proxyPath}: {exc}')
            raise
        if expirytime < datetime.datetime.utcnow():
            raise ProxyFileExpiredError()
        return proxystr, dn, expirytime

    def readProxyString(self, proxyStr):
        """
        Read proxy info from a string.

        Extracting the proxy information is the same as in
        :meth:~`act.common.aCTProxy.aCTProxy._readProxyFromFile`.

        Args:
            proxyStr: A string with proxy content.

        Returns:
            A tuple with string DN and string expiry time.
        """
        cred_type = arc.initializeCredentialsType(arc.initializeCredentialsType.SkipCredentials)
        userconf = arc.UserConfig(cred_type)
        userconf.CredentialString(str(proxyStr))
        cred = arc.Credential(userconf)
        dn = cred.GetIdentityName()
        expirytime = datetime.datetime.strptime(
                cred.GetEndTime().str(arc.UTCTime),
                "%Y-%m-%dT%H:%M:%SZ")
        return dn, expirytime

    def updateProxy(self, proxyPath):
        """
        Update or insert given proxy, return proxyid.

        Args:
            proxyPath: A string with path to proxy file.

        Returns:
            ID of proxy in database.
        """
        proxystr, dn, exptime = self.readProxyFile(proxyPath)
        return self.actproxy.updateProxy(proxystr, dn, '', exptime)

    def getProxyIdForProxyFile(self, path=None):
        """
        Get proxy id for proxy in given file.

        If no path is given, the default location for generated proxies is
        used, which is /tmp/x509up_u<user id>.

        Args:
            path: A string with path to proxy file.

        Returns:
            Proxy ID from database.

        Raises:
            ProxyDBExpiredError: Proxy in DB has expired.
        """
        if not path:
            path = DEFAULT_PROXY_PATH + str(os.getuid())

        _, dn, expirytime = self.readProxyFile(path)
        proxyinfo = self.getProxyInfo(dn, '', ['id', 'expirytime'])
        if expirytime != proxyinfo["expirytime"]:
            raise ProxyDBExpiredError()
        return proxyinfo["id"]

    def getProxiesWithDN(self, dn, columns=[]):
        """
        Get info for proxies with given dn.

        Args:
            dn: A string with DN.
            columns: A list of string names of table columns.

        Returns:
            A list of dictionaries with column name:value entries for proxies.
        """
        return self.arcdb.getProxiesInfo(f" dn = '{dn}' ", columns)

    def getProxyKeyPEM(self, proxyid):
        c = self.arcdb.db.getCursor()
        try:
            c.execute('SELECT proxy FROM proxies WHERE id = %s', (proxyid,))
        except Exception as exc:
            self.log.error(f'Error retrieving private key PEM from database: {exc}')
            return None
        else:
            row = c.fetchone()
            return row['proxy'].decode()
        finally:
            c.close()

    def checkProxyExists(self, proxyid):
        try:
            c = self.arcdb.db.getCursor()
            c.execute('SELECT id,expirytime FROM proxies WHERE id = %s LIMIT 1', (proxyid,))
        except Exception as exc:
            self.log.error(f'Error checking existence of proxy: {exc}')
            return None
        else:
            proxy = c.fetchone()
            if proxy is not None:
                if proxy['expirytime'] > datetime.datetime.utcnow():
                    return True
            return False
        finally:
            c.close()


# We basically want to get the value of the first 'attribute:' line from
# 'arcproxy -I' output.
#
# We achieve this by replicating what arcproxy does. The relevant part is:
# https://source.coderefinery.org/nordugrid/arc/-/blob/master/src/clients/credentials/arcproxy.cpp#L606-744
def getVOMSProxyAttributes(certPEM, chainPEM):
    certList = [certPEM]
    certList.extend(splitChainPEMs(chainPEM))

    for cert in certList:
        uc = arc.UserConfig()
        uc.CredentialString(cert)
        cr = arc.Credential(uc)
        if not cr.GetCert():
            continue
        trustList = arc.VOMSTrustList()
        trustList.AddRegex(".*")
        acList = arc.VOMSACInfoVector()
        if not arc.parseVOMSAC(cr, uc.CACertificatesDirectory(), "", "/etc/grid-security/vomsdir", trustList, acList):
            continue
        # These loops go over values of interest. They mimic this code snippet:
        # https://source.coderefinery.org/nordugrid/arc/-/blob/master/src/clients/credentials/arcproxy.cpp#L684-724
        for ac in acList:
            for attr in ac.attributes:
                if 'hostname=' not in attr:
                    return attr
    return None


def splitChainPEMs(pem):
    """Return a list of cert PEMs from combined string of chain PEMs."""
    return re.findall(
        "-----BEGIN.*?-----.*?-----END.*?-----",
        pem,
        flags=re.DOTALL
    )
