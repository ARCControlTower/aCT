#!/usr/bin/env python

"""
Kill jobs.

Returns:
    1: No proxy found.
    2: One of the elements in job list is not a range.
    3: One of the elements in job list is not a valid ID.
"""


import argparse
import sys
import logging
import os

import act.client.jobmgr as jobmgr
import act.client.proxymgr as proxymgr
import act.client.clicommon as clicommon
from act.client.errors import InvalidJobRangeError
from act.client.errors import InvalidJobIDError


def main():
    # parse arguments
    parser = argparse.ArgumentParser(description='Kill jobs')
    parser.add_argument('-a', '--all', action='store_true',
            help='all jobs that match other criteria')
    parser.add_argument('-j', '--jobs', default='',
            help='comma separated list of job IDs or ranges')
    parser.add_argument('-f', '--find', default='',
            help='get only jobs with matching (sub)string in their name')
    parser.add_argument('-s', '--state', default='',
            help='get only jobs with certain state')
    parser.add_argument('-v', '--verbose', action='store_true',
            help='show more information')
    parser.add_argument('-p', '--proxy', default=None,
            help='custom path to proxy certificate')
    args = parser.parse_args()

    # logging
    logFormat = "[%(asctime)s] [%(filename)s:%(lineno)d] [%(levelname)s] - %(message)s"
    if args.verbose:
        logging.basicConfig(format=logFormat, level=logging.DEBUG, stream=sys.stdout)
    else:
        logging.basicConfig(format=logFormat, level=logging.DEBUG, filename=os.devnull)

    # create a list of jobs to work on
    if args.all:
        jobs = [] # empty means all jobs
    elif args.jobs:
        try:
            jobs = jobmgr.getIDsFromList(args.jobs)
        except InvalidJobRangeError as e:
            print "error: range '{}' is not a valid range".format(e.jobRange)
            sys.exit(2)
        except InvalidJobIDError as e:
            print "error: ID '{}' is not a valid ID".format(e.jobid)
            sys.exit(3)
    else:
        print "error: no jobs specified (use -a or -j)"
        sys.exit(10)

    # get proxy ID given proxy
    proxyid = clicommon.getProxyIdFromProxy(args.proxy)

    # kill jobs
    manager = jobmgr.JobManager()
    numKilled = manager.killJobs(proxyid, jobs, args.state, args.find)
    print 'Jobs killed: {}'.format(numKilled)


if __name__ == '__main__':
    main()


