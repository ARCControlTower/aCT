#!/usr/bin/env python3

# TODO: Add CLI switch for no header for simpler processing

"""
Get job information from aCT.

Returns:
    1: No proxy found.
    2: One of the elements in job list is not a range.
    3: One of the elements in job list is not a valid ID.
    9: Error getting job stats (likely because of wrong column names).
"""

import argparse
import sys
import logging
import os

from act.client.jobmgr import getIDsFromList, JobManager
from act.client.common import showHelpOnCommandOnly, getProxyIdFromProxy
from act.client.errors import InvalidJobRangeError, InvalidJobIDError


def main():
    # parse arguments
    parser = argparse.ArgumentParser(description='Get job info from aCT')
    parser.add_argument('-a', '--all', action='store_true',
            help='all jobs that match other criteria')
    parser.add_argument('-j', '--jobs', default='',
            help='ID/range(id1-id2;id1<id2)/comma separated list of IDs/ranges')
    parser.add_argument('-f', '--find', default='',
            help='get only jobs with matching (sub)string in their name')
    parser.add_argument('-s', '--state', default='',
            help='get only jobs with certain state')
    parser.add_argument('-v', '--verbose', action='store_true',
            help='show more information')
    parser.add_argument('-p', '--proxy', default=None,
            help='custom path to proxy certificate')
    parser.add_argument('--arc-cols', default='JobID,State,arcstate',
            help='columns from ARC table that should be fetched')
    parser.add_argument('--client-cols', default='id,jobname',
            help='columns from client table that should be fetched')
    parser.add_argument('--get-cols', action='store_true',
            help='print all available column names')

    showHelpOnCommandOnly(parser)

    args = parser.parse_args()

    # logging
    logFormat = "[%(asctime)s] [%(filename)s:%(lineno)d] [%(levelname)s] - %(message)s"
    if args.verbose:
        logging.basicConfig(format=logFormat, level=logging.DEBUG, stream=sys.stdout)
    else:
        logging.basicConfig(format=logFormat, level=logging.DEBUG, filename=os.devnull)

    # get column names from database
    manager = JobManager()
    if args.get_cols:
        clientCols = manager.getClientColumns()
        arcCols = manager.getArcColumns()
        print('client cols:', end=' ')
        for col in clientCols:
            print(col, end=' ')
        print()
        print('arc cols:', end=' ')
        for col in arcCols:
            print(col, end=' ')
        print()
        sys.exit(0)

    # create a list of jobs to work on
    if args.all:
        jobs = [] # empty means all jobs
    elif args.jobs:
        try:
            jobs = getIDsFromList(args.jobs)
        except InvalidJobRangeError as e:
            print("error: range '{}' is not a valid range".format(e.jobRange))
            sys.exit(2)
        except InvalidJobIDError as e:
            print("error: ID '{}' is not a valid ID".format(e.jobid))
            sys.exit(3)
    else:
        print("error: no jobs specified (use -a or -j)")
        sys.exit(10)

    # create column lists
    if not args.client_cols:
        clicols = []
    else:
        clicols = args.client_cols.split(',')
    if not args.arc_cols:
        arccols = []
    else:
        arccols = args.arc_cols.split(',')

    # get proxy ID given proxy
    proxyid = getProxyIdFromProxy(args.proxy)

    # get information
    try:
        jobdicts = manager.getJobStats(
                proxyid,
                jobs,
                args.state,
                args.find,
                clicols=clicols,
                arccols=arccols)
    except Exception as e:
        print('error: {}'.format(str(e)))
        sys.exit(9)

    if not jobdicts: # no jobs so just exit
        sys.exit(0)

    # For each column, determine biggest sized value so that output can
    # be nicely formatted.
    colsizes = {}
    for job in jobdicts:
        for key, value in job.items():
            # All keys have a letter and underscore prepended, which is not
            # used when printing
            colsize = max(len(str(key[2:])), len(str(value)))
            try:
                if colsize > colsizes[key]:
                    colsizes[key] = colsize
            except KeyError:
                colsizes[key] = colsize

    # Print table header
    for col in clicols:
        print('{:<{width}}'.format(col, width=colsizes['c_' + col]), end=' ')
    for col in arccols:
        print('{:<{width}}'.format(col, width=colsizes['a_' + col]), end=' ')
    print()
    line = ''
    for value in colsizes.values():
        line += '-' * value
    line += '-' * (len(colsizes) - 1)
    print(line)

    # Print jobs
    for job in jobdicts:
        for col in clicols:
            fullKey = 'c_' + col
            txt = job.get(fullKey)
            # just in case the value is a bunch of whitespace
            # TODO: This (str(txt)) might not be a general fix; it is a direct 
            #       fix for the problem encountered with
            #       datetime.datetime object for 'created' field that
            #       has to be converted to a string.
            #       The same fix is used for arccols below.
            # TODO: This fix assumes that all job fields are properly
            #       convertible to string. Is that really so?
            if not txt or str(txt).strip() == '': # short circuit important!
                txt = "''"
            print('{:<{width}}'.format(txt, width=colsizes[fullKey]), end=' ')
        for col in arccols:
            fullKey = 'a_' + col
            txt = job.get(fullKey)
            # just in case the value is a bunch of whitespace
            if not txt or str(txt).strip() == '': # short circuit important!
                txt = "''"
            print('{:<{width}}'.format(str(txt), width=colsizes[fullKey]), end=' ')
        print()


if __name__ == '__main__':
    main()


