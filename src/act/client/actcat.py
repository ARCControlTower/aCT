#!/usr/bin/env python3

"""
Print stdout of filtered jobs from the database.

Returns:
    1: No proxy found.
    2: One of the elements in job list is not a range.
    3: One of the elements in job list is not a valid ID.
    9: Error getting job stats
"""

import argparse
import sys
import logging
import os
import subprocess

from act.client.jobmgr import JobManager, getIDsFromList
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
    parser.add_argument('-n', '--name', default='',
            help='get only jobs with given name')
    parser.add_argument('-v', '--verbose', action='store_true',
            help='show more information')
    parser.add_argument('-p', '--proxy', default=None,
            help='custom path to proxy certificate')

    # arguments passed directly to arccat
    parser.add_argument('-o', '--stdout', action='store_true',
            help='show the stdout of the job (default)', default=True)
    parser.add_argument('-e', '--stderr', action='store_true',
            help='show the stderr of the job')
    #parser.add_argument('-l', '--joblog', action='store_true',
    #        help='show A-REX\'s error log of the job')
    #parser.add_argument('-P', '--listplugins', action='store_true',
    #        help='list the available plugins')
    #parser.add_argument('-t', '--timeout', type=int, nargs=1,
    #        help='timeout in seconds (default 20)', default=20)

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

    proxyid = getProxyIdFromProxy(args.proxy)

    # get ARC job IDs of jobs that match filters
    try:
        jobdicts = manager.getJobStats(
                proxyid,
                jobs,
                args.state,
                args.find,
                clicols=[],
                arccols=["JobID", "StdOut", "StdErr"],
                jobname=args.name)
    except Exception as e:
        print('error: {}'.format(str(e)))
        sys.exit(9)

    if not jobdicts: # no jobs so just exit
        print('no jobs found that fit given filters')
        sys.exit(0)

    for job in jobdicts:
        url = job["a_JobID"] + "/"
        if args.stderr:
            url += job["a_StdErr"]
        elif args.stdout:
            url += job["a_StdOut"]
        subprocess.run(["arccp", url, "-"])


if __name__ == '__main__':
    main()


