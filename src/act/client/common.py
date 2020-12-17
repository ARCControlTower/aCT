"""
This module defines all functionality that is common to CLI programs.
"""

import sys

import act.client.proxymgr as proxymgr
from act.client.errors import NoSuchProxyError
from act.client.errors import NoProxyFileError
from act.client.errors import ProxyFileExpiredError
from act.client.errors import ProxyDBExpiredError


def getProxyIdFromProxy(proxyPath):
    """
    Returns ID of proxy at the given path.

    Args:
        proxyPath: A string with path to the proxy.

    Raises:
        NoSuchProxyError: Proxy with DN and attributes of the proxy given
            in proxy path is not in the database.
        NoProxyFileError: No proxy on given path.
    """
    manager = proxymgr.ProxyManager()
    try:
        return manager.getProxyIdForProxyFile(proxyPath)
    except NoSuchProxyError as e:
        print("error: no proxy for DN=\"{}\" and attributes=\"{}\" "\
                "found in database; use actproxy".format(e.dn, e.attribute))
        sys.exit(1)

    except NoProxyFileError as e:
        print("error: no proxy file \"{}\"; create proxy first".format(e.path))
        sys.exit(2)

    except ProxyFileExpiredError:
        print("error: proxy has expired; create new proxy")
        sys.exit(3)

    except ProxyDBExpiredError:
        print("error: proxy entry in DB has expired; run actproxy")
        sys.exit(4)


def showHelpOnCommandOnly(argparser):
    """Show help if command is called without parameters."""
    if len(sys.argv) == 1:
        argparser.print_help()
        sys.exit(0)


