#!/usr/bin/env python3

#TODO: consider adding state argument because of refetch operation

"""
Fetch failed jobs from ARC.

Returns:
    1: No proxy found.
    2: One of the elements in job list is not a range.
    3: One of the elements in job list is not a valid ID.
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
    parser = argparse.ArgumentParser(description='Fetch failed jobs')
    parser.add_argument('-a', '--all', action='store_true',
            help='all jobs that match other criteria')
    parser.add_argument('-j', '--jobs', default='',
            help='comma separated list of IDs or ranges')
    parser.add_argument('-f', '--find',
            help='get only jobs with matching (sub)string in their name')
    parser.add_argument('-v', '--verbose', action='store_true',
            help='show more information')
    parser.add_argument('-p', '--proxy', default=None,
            help='custom path to proxy certificate')
    parser.add_argument('-r', '--refetch', action='store_true',
            help='refetch packages')

    showHelpOnCommandOnly(parser)

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

    # get proxy ID given proxy
    proxyid = getProxyIdFromProxy(args.proxy)

    # fetch jobs
    manager = JobManager()
    if args.refetch:
        numFetching = len(manager.refetchJobs(proxyid, jobs, args.find))
        print('Will refetch {} jobs'.format(numFetching))
    else:
        numFetching = len(manager.fetchJobs(proxyid, jobs, args.find))
        print('Will fetch {} jobs'.format(numFetching))


if __name__ == '__main__':
    main()


