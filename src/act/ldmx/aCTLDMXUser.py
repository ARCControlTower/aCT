import argparse
import logging
import os
import pwd
import re
import shutil
import sys
import time
from pathlib import Path

from act.ldmx import aCTDBLDMX
from act.common.aCTConfig import aCTConfigAPP

logger = logging.getLogger()
logger.setLevel(logging.INFO)
hdlr = logging.StreamHandler()
logger.addHandler(hdlr)

def submit(args):

    try:
        config_file = os.path.abspath(args.conffile)
        with open(config_file) as f:
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
        for param in ('JobTemplate', 'NumberofJobs'):
            if param not in config:
                logger.error(f"Error: {param} not defined in {args.conffile}")
                return 1

        if 'IsImage' not in config or config['IsImage'] == "No":
            if 'RandomSeed1SequenceStart' not in config :  #required for unique run numbering in simulation jobs
                logger.error(f"Error: 'RandomSeed1SequenceStart' not defined in {args.conffile}")
                return 1

    dbldmx = aCTDBLDMX.aCTDBLDMX(logger)
    # If user is not admin, and they try to use a scope that isn't user.ruciouser, throw an error
    if 'Scope' in config:
        user = dbldmx.getUser(args.uid)
        if user['role'] != 'admin' and config['Scope'] != f"user.{user['ruciouser']}":
            logger.error(f"Error: user {user['username']} cannot use scope {config['Scope']}")
            return 1

    actconf = aCTConfigAPP()

    template_file = os.path.abspath(os.path.join(args.templatedir, config['JobTemplate']))
    if not os.path.exists(template_file):
        logger.error(f"Error: template not found at {template_file}")
        return 1

    # only if we explicitly ask to keep logs, we'll submit with the keepsuccessful option set to True (YAML)
    if actconf.joblog.keepsuccessful and not args.keepLogs:
        logger.info(f"Successful job logs will be kept! This could fill up the disk, for large batches. \n-->Continue submission now with this log setting? [y/N]")
        response=str(input())
        if response != "y" :
            logger.info(f"Interrupting submission on request by user. Modify log keeping settings in aCTConfigAPP.xml.")
            return 1

    # Get batch name or set default
    batchname = config.get('BatchID', f'Batch-{time.strftime("%Y-%m-%dT%H%M%S")}')

    # Everything looks ok, so submit the batch
    try:
        dbldmx.insertBatch(config_file, template_file, batchname, args.uid)
    except Exception as e:
        logger.error(f"Failed to submit {args.conffile}: {str(e)}")
        return 1

    logger.info(f"Submitted job configuration at {args.conffile} to create {config.get('NumberofJobs', '')} jobs")
    return 0

def cancel(args):

    if not (args.batchid or args.site):
        logger.error("BatchID or site must be specified")
        return 1

    constraints = []
    dbldmx = aCTDBLDMX.aCTDBLDMX(logger)
    if args.batchid:
        if not sanitise(args.batchid):
            logger.error(f"Illegal batchID: {args.batchid}")
            return 1
        batches = dbldmx.getBatches(f"batchname='{args.batchid}'", columns=['id'])
        if not batches:
            logger.error(f"No such batchID found")
            return 1
        constraints.append(f"batchid={batches[0]['id']}")
    if args.site:
        if not sanitise(args.batchid):
            logger.error(f"Illegal site name: {args.site}")
            return 1
        constraints.append(f"sitename='{args.site}'")

    # Check if there are jobs to cancel and if they are all owned by the current user
    jobs = dbldmx.getJobs(f"ldmxstatus in {job_not_final_states()} AND {' AND '.join(constraints)}",
                           columns=['ldmxjobs.userid'])

    if not jobs:
        logger.error('No matching jobs found')
        return 0

    if [j for j in jobs if j['userid'] != args.uid]:
        logger.error('Found jobs owned by another user which cannot be cancelled')
        return 1

    answer = input(f'This will cancel {len(jobs)} jobs, are you sure? (y/n) ')
    if answer != 'y':
        logger.info('Aborting..')
        return 0

    dbldmx.updateJobs(f"ldmxstatus in {job_not_final_states()} AND {' AND '.join(constraints)}",
                      {'ldmxstatus': 'tocancel'})
    logger.info(f'Cancelled {len(jobs)} jobs')
    return 0

def resubmit(args):

    if not (args.batchid or args.site):
        logger.error("BatchID or site must be specified")
        return 1

    constraints = []
    dbldmx = aCTDBLDMX.aCTDBLDMX(logger)
    if args.batchid:
        if not sanitise(args.batchid):
            logger.error(f"Illegal batchID: {args.batchid}")
            return 1
        batches = dbldmx.getBatches(f"batchname='{args.batchid}'", columns=['id'])
        if not batches:
            logger.error(f"No such batchID found")
            return 1
        constraints.append(f"batchid={batches[0]['id']}")
    if args.site:
        if not sanitise(args.site):
            logger.error(f"Illegal site name: {args.site}")
            return 1
        constraints.append(f"sitename='{args.site}'")

    # Check if there are jobs to resubmit and if they are all owned by the current user
    jobs = dbldmx.getJobs(f"ldmxstatus in {job_not_final_states()} AND {' AND '.join(constraints)}",
                          columns=['ldmxjobs.userid'])

    if not jobs:
        logger.error('No matching jobs found')
        return 0

    if [j for j in jobs if j['userid'] != args.uid]:
        logger.error('Found jobs owned by another user which cannot be resubmitted')
        return 1

    answer = input(f'This will resubmit {len(jobs)} jobs, are you sure? (y/n) ')
    if answer != 'y':
        logger.info('Aborting..')
        return 0

    dbldmx.updateJobs(f"ldmxstatus in {job_not_final_states()} AND {' AND '.join(constraints)}",
                      {'ldmxstatus': 'toresubmit'})
    logger.info(f'Resubmitted {len(jobs)} jobs')
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
    return re.match(r'^[a-zA-Z0-9_\-\.]+$', query_string)

def is_user_admin():
    """
    Returns true if user calling command is an admin
    """

    dbldmx = aCTDBLDMX.aCTDBLDMX(logger)

    # Check if this user has admin rights
    ldmxuser = dbldmx.getUser(os.getuid())
    return (not ldmxuser) or (ldmxuser['role'] != 'admin')

def is_user_allowed(uid, username):
    """
    Check if the current user is allowed to run a command on behalf of another
    """
    dbldmx = aCTDBLDMX.aCTDBLDMX(logger)

    # Check if this user has admin rights
    if not is_user_admin():
        print("You do not have privileges to run commands on behalf of other users")
        return False

    # Check if the other user exists
    ldmxuser = dbldmx.getUser(uid)
    if not ldmxuser:
        print(f"No such user {username}")
        return False

    return True

def get_parser():
    """
    Returns the argparse parser.
    """
    oparser = argparse.ArgumentParser(prog=os.path.basename(sys.argv[0]), add_help=True)
    oparser.add_argument('-v', '--verbose', default=False, action='store_true', help="Print more verbose output")
    oparser.add_argument('-u', '--user', dest='username', action='store', help='Username to run command on behalf of')

    subparsers = oparser.add_subparsers()

    submit_parser = subparsers.add_parser('submit', help='Submit jobs')
    submit_parser.set_defaults(function=submit)
    submit_parser.add_argument('-c', '--config', dest='conffile', action='store', help='Job configuration file')
    submit_parser.add_argument('-k', '--keepLogs', dest='keepLogs', default=False, action='store_true', help='Assertion that we want to keep succesful job logs')
    submit_parser.add_argument('-t', '--templatedir', dest='templatedir', default=os.path.join(str(Path.home()), 'templates'), action='store', help='Template directory (default $HOME/templates)')

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

    if args.username:
        try:
            args.uid = pwd.getpwnam(args.username)[2]
        except:
            logger.error(f'User {args.username} does not exist')
            sys.exit(1)
        if not is_user_allowed(args.uid, args.username):
            sys.exit(1)
    else:
        args.uid = os.getuid()

    if args.verbose:
        logger.setLevel(logging.DEBUG)

    try:
        result = args.function(args)
        sys.exit(result)
    except Exception as error:
        logger.error("Strange error: {0}".format(error))
        sys.exit(1)

if __name__ == '__main__':
    main()
