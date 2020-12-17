#!/usr/bin/env python3

"""
Submit xRSL job to aCT.

Returns:
    1: No proxy found.
    4: Site is not configured.
    6: Invalid job description.
    7: Error reading xRSL file.
"""


import os
import argparse
import sys
import logging

from act.client.clientdb import ClientDB
from act.client.common import showHelpOnCommandOnly, getProxyIdFromProxy
from act.client.jobmgr import checkSite, checkJobDesc
from act.common.aCTConfig import aCTConfigARC
from act.client.errors import NoSuchSiteError, InvalidJobDescriptionError


def readXRSL(filepath):
    """Return contents of given file."""
    with open(filepath, 'r') as xrsl:
        return xrsl.read()


def main():
    # parse arguments
    parser = argparse.ArgumentParser(description='Submit xRSL job to aCT')
    parser.add_argument('-p', '--proxy', default=None,
            help='custom path to proxy certificate')
    parser.add_argument('-s', '--site', default='default',
            help='specific site to submit job to')
    parser.add_argument('-v', '--verbose', action='store_true',
            help='show more information')
    parser.add_argument('xRSL', help='path to xRSL file')

    showHelpOnCommandOnly(parser)

    args = parser.parse_args()

    # logging
    logFormat = "[%(asctime)s] [%(filename)s:%(lineno)d] [%(levelname)s] - %(message)s"
    if args.verbose:
        logging.basicConfig(format=logFormat, level=logging.DEBUG, stream=sys.stdout)
    else:
        logging.basicConfig(format=logFormat, level=logging.DEBUG, filename=os.devnull)

    # get ID given proxy
    proxyid = getProxyIdFromProxy(args.proxy)

    # check site
    try:
        checkSite(args.site) # use default path for sites.json
    except NoSuchSiteError as e:
        print('error: site \'{}\' is not configured'.format(args.site))
        sys.exit(4)
    except Exception as e:
        print('error: could not read site config: {}'.format(str(e)))
        sys.exit(11) # TODO: refactor error handling

    # get and check job description
    try:
        jobdesc = readXRSL(args.xRSL)
        checkJobDesc(jobdesc)
    except InvalidJobDescriptionError:
        print('error: invalid job description')
        sys.exit(6)
    except IOError:
        print('error: could not read xRSL file')
        sys.exit(7)

    # insert job
    arcconf = aCTConfigARC()
    clidb = ClientDB()
    jobid = clidb.insertJobAndDescription(jobdesc, proxyid, args.site)
    print('Successfully inserted job with id {}'.format(jobid))


if __name__ == '__main__':
    main()


