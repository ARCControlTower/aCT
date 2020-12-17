#!/usr/bin/env python3

"""
Insert proxy certificate into aCT.

Returns:
    8: Error inserting or updating proxy.
    9: Proxy has expired.
"""


import argparse
import os
import sys
import logging

from act.client.proxymgr import ProxyManager
from act.client.errors import NoProxyFileError, ProxyFileExpiredError


def printProxyInfo(proxyInfo):
    """Print proxy info from aCT table."""
    for key, value in proxyInfo.items():
        print('{:<12}: {}'.format(key, value))


def main():
    # parse arguments
    parser = argparse.ArgumentParser(description = 'aCT proxies utility')
    parser.add_argument('-p', '--proxy', default = None, help = 'custom path to proxy')
    parser.add_argument('-v', '--verbose', action='store_true',
            help='show more information')
    args = parser.parse_args()

    # logging
    logFormat = "[%(asctime)s] [%(filename)s:%(lineno)d] [%(levelname)s] - %(message)s"
    if args.verbose:
        logging.basicConfig(format=logFormat, level=logging.DEBUG, stream=sys.stdout)
    else:
        logging.basicConfig(format=logFormat, level=logging.DEBUG, filename=os.devnull)

    # determine proxy file path from args
    if not args.proxy: # default proxy path is /tmp/x509_u<user id>
        proxyPath = '/tmp/x509up_u' + str(os.getuid())
    else:
        proxyPath = args.proxy

    manager = ProxyManager()
    try:
        manager.updateProxy(proxyPath)
    except NoProxyFileError as e:
        print("error: no proxy file \"{}\"; create proxy first".format(e.path))
    except ProxyFileExpiredError:
        print("error: proxy has expired; create new proxy")
        sys.exit(9)
    except Exception as e:
        print('error: {}'.format(str(e)))
        sys.exit(8)


if __name__ == '__main__':
    main()


