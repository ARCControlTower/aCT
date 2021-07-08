import argparse
import logging
import os
import re
import shutil
import sys

from act.ldmx import aCTDBLDMX
from act.common.aCTConfig import aCTConfigAPP

logger = logging.getLogger()
logger.setLevel(logging.INFO)
hdlr = logging.StreamHandler()
logger.addHandler(hdlr)

def submit(args):

    try:
        with open(args.conffile) as f:
            try:
                config = {l.split('=')[0]: l.split('=')[1].strip() for l in f if '=' in l}
            except IndexError:
                logger.error(f"Error: Badly formed line in {args.conffile}")
                return 1
    except OSError as e:
        logger.error(f"Error: Failed to open job configuration file {args.conffile}: {str(e)}")
        return 1

    # Check for mandatory parameters
    if 'InputDataset' not in config:
        for param in ('JobTemplate', 'RandomSeed1SequenceStart', 'RandomSeed2SequenceStart', 'NumberofJobs'):
            if param not in config:
                logger.error(f"Error: {param} not defined in {args.conffile}")
                return 1

    actconf = aCTConfigAPP()
    bufferdir = actconf.get(['jobs', 'bufferdir'])
    if not bufferdir:
        logger.error(f"Error: bufferdir not found in aCT configuration")
        return 1

    template_file = os.path.join(bufferdir, 'templates', config['JobTemplate'])
    if not os.path.exists(template_file):
        logger.error(f"Error: template not found at {template_file}")
        return 1

    # only if we explicitly ask to keep logs, we'll submit with the keepsuccessful option set to 1
    if str(actconf.get(['joblog', 'keepsuccessful'])) == '1' and not args.keepLogs :
        logger.info(f"Successful job logs will be kept! This could fill up the disk, for large batches. \n-->Continue submission now with this log setting? [y/N]")
        response=str(input())
        if response != "y" :
            logger.info(f"Interrupting submission on request by user. Modify log keeping settings in aCTConfigAPP.xml.")
            return 1

    # Everything looks ok, so submit the job
    try:
        shutil.copy(args.conffile, os.path.join(bufferdir, 'configs'))
    except Exception as e:
        logger.error(f"Failed to copy {args.conffile} to {os.path.join(bufferdir, 'configs')}: {str(e)}")
        return 1

    logger.info(f"Submitted job configuration at {args.conffile} to create {config.get('NumberofJobs', '')} jobs")
    return 0

def cancel(args):

    if not (args.batchid or args.site):
        logger.error("BatchID or site must be specified")
        return 1

    constraints = []
    if args.batchid:
        if not sanitise(args.batchid):
            logger.error(f"Illegal batchID: {args.batchid}")
            return 1
        constraints.append(f"batchid='{args.batchid}'")
    if args.site:
        if not sanitise(args.batchid):
            logger.error(f"Illegal site name: {args.site}")
            return 1
        constraints.append(f"sitename='{args.site}'")

    dbldmx = aCTDBLDMX.aCTDBLDMX(logger)
    jobs = dbldmx.getNJobs(f"ldmxstatus in {job_not_final_states()} AND {' AND '.join(constraints)}")

    if not jobs:
        logger.error('No matching jobs found')
        return 0

    answer = input(f'This will cancel {jobs} jobs, are you sure? (y/n) ')
    if answer != 'y':
        logger.info('Aborting..')
        return 0

    dbldmx.updateJobs(f"ldmxstatus in {job_not_final_states()} AND {' AND '.join(constraints)}",
                      {'ldmxstatus': 'tocancel'})
    logger.info(f'Cancelled {jobs} jobs')
    return 0

def resubmit(args):

    if not (args.batchid or args.site):
        logger.error("BatchID or site must be specified")
        return 1

    constraints = []
    if args.batchid:
        if not sanitise(args.batchid):
            logger.error(f"Illegal batchID: {args.batchid}")
            return 1
        constraints.append(f"batchid='{args.batchid}'")
    if args.site:
        if not sanitise(args.site):
            logger.error(f"Illegal site name: {args.site}")
            return 1
        constraints.append(f"sitename='{args.site}'")

    dbldmx = aCTDBLDMX.aCTDBLDMX(logger)
    jobs = dbldmx.getNJobs(f"ldmxstatus in {job_not_final_states()} AND {' AND '.join(constraints)}")

    if not jobs:
        logger.error('No matching jobs found')
        return 0

    answer = input(f'This will resubmit {jobs} jobs, are you sure? (y/n) ')
    if answer != 'y':
        logger.info('Aborting..')
        return 0

    dbldmx.updateJobs(f"ldmxstatus in {job_not_final_states()} AND {' AND '.join(constraints)}",
                      {'ldmxstatus': 'toresubmit'})
    logger.info(f'Resubmitted {jobs} jobs')
    return 0

def job_not_final_states():
    """
    Return db states which are not final
    """
    return "('new', 'waiting', 'queueing', 'running', 'finishing', 'registering')"

def sanitise(query_string):
    """
    Return False if query_string contains bad characters
    """
    return re.match('^[a-zA-Z0-9_\-\.]+$', query_string)

def get_parser():
    """
    Returns the argparse parser.
    """
    oparser = argparse.ArgumentParser(prog=os.path.basename(sys.argv[0]), add_help=True)
    oparser.add_argument('-v', '--verbose', default=False, action='store_true', help="Print more verbose output")

    subparsers = oparser.add_subparsers()

    submit_parser = subparsers.add_parser('submit', help='Submit jobs')
    submit_parser.set_defaults(function=submit)
    submit_parser.add_argument('-c', '--config', dest='conffile', action='store', help='Job configuration file')
    submit_parser.add_argument('-k', '--keepLogs', dest='keepLogs', default=False, action='store_true', help='Assertion that we want to keep succesful job logs')

    cancel_parser = subparsers.add_parser('cancel', help='Cancel jobs')
    cancel_parser.set_defaults(function=cancel)
    cancel_parser.add_argument('--batchid', dest='batchid', action='store', help='Batch ID')
    cancel_parser.add_argument('--site', dest='site', action='store', help='Site name')

    resubmit_parser = subparsers.add_parser('resubmit', help='Resubmit jobs')
    resubmit_parser.set_defaults(function=resubmit)
    resubmit_parser.add_argument('--batchid', dest='batchid', action='store', help='Batch ID')
    resubmit_parser.add_argument('--site', dest='site', action='store', help='Site name')

    return oparser

def main():
    oparser = get_parser()

    if len(sys.argv) == 1:
        oparser.print_help()
        sys.exit(1)

    args = oparser.parse_args(sys.argv[1:])

    if not hasattr(args, 'function'):
        oparser.print_help()
        sys.exit(1)

    try:
        if args.verbose:
            logger.setLevel(logging.DEBUG)
        result = args.function(args)
        sys.exit(result)
    except Exception as error:
        logger.error("Strange error: {0}".format(error))
        sys.exit(1)

if __name__ == '__main__':
    main()
