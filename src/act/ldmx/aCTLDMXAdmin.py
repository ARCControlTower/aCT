import argparse
import logging
import os
import pwd
import re
import shutil
import sys
from tabulate import tabulate

from act.ldmx import aCTDBLDMX
from act.common.aCTConfig import aCTConfigAPP

logger = logging.getLogger()
logger.setLevel(logging.INFO)
hdlr = logging.StreamHandler()
logger.addHandler(hdlr)

def adduser(args):

    try:
        uid = pwd.getpwnam(args.username)[2]
    except:
        logger.error(f'Could not find uid for user {args.username}')
        return 1

    dbldmx = aCTDBLDMX.aCTDBLDMX(logger)
    try:
        dbldmx.insertUser(uid, args.username, args.role, name=args.name, ruciouser=args.ruciouser)
    except Exception as x:
        logger.error(f'Failed to insert new user: {x}')
        return 1
    return 0

def showusers(args):

    dbldmx = aCTDBLDMX.aCTDBLDMX(logger)
    if args.username:
        try:
            uid = pwd.getpwnam(args.username)[2]
        except:
            logger.error(f'Could not find uid for user {args.username}')
            return 1
        users = [dbldmx.getUser(uid)]
    else:
        users = dbldmx.getUsers()

    print(tabulate(users, headers='keys'))
    return 0

def deleteuser(args):

    dbldmx = aCTDBLDMX.aCTDBLDMX(logger)
    try:
        uid = pwd.getpwnam(args.username)[2]
    except:
        logger.error(f'Could not find uid for user {args.username}')
        return 1
    user = dbldmx.getUser(uid)
    if not user:
        logger.error(f'No such user {args.username} registered in aCT')
        return 1

    # Check if the user still has active jobs
    njobs = dbldmx.getNJobs(f"userid = {uid}")
    if njobs:
        logger.warn(f"User {args.username} still has {njobs} active jobs, please wait until they are archived")
        return 1

    response=str(input(f"Are you sure you want to delete the account {args.username}? [y/N] > "))
    if response != "y" :
        logger.info("Backing out!")
        return 0

    try:
        dbldmx.deleteUser(uid)
    except Exception as exc:
        logger.error(f'Error deleting user {args.username}: {exc}')
        return 1

    logger.info(f"User {args.username} deleted")
    return 0

def checkAdmin():
    dbldmx = aCTDBLDMX.aCTDBLDMX(logger)
    userinfo = dbldmx.getUser(os.getuid())
    if not userinfo or userinfo['role'] != 'admin':
        logger.error('Only admin users can run this command')
        return False
    return True

def get_parser():
    """
    Returns the argparse parser.
    """
    oparser = argparse.ArgumentParser(prog=os.path.basename(sys.argv[0]), add_help=True)
    oparser.add_argument('-v', '--verbose', default=False, action='store_true', help="Print more verbose output")

    subparsers = oparser.add_subparsers()

    add_user_parser = subparsers.add_parser('adduser', help='Add a new user')
    add_user_parser.set_defaults(function=adduser)
    add_user_parser.add_argument('--role', dest='role', default='user', action='store', help='User role (admin or user, default user)')
    add_user_parser.add_argument('--name', dest='name', action='store', help='Real name of user (default login)')
    add_user_parser.add_argument('--ruciouser', dest='ruciouser', action='store', help='Rucio account (default login)')
    add_user_parser.add_argument('username')

    show_users_parser = subparsers.add_parser('showusers', help='Show user information')
    show_users_parser.set_defaults(function=showusers)
    show_users_parser.add_argument('username', nargs='?', help='Query specific username')

    delete_users_parser = subparsers.add_parser('deleteuser', help='Remove a user')
    delete_users_parser.set_defaults(function=deleteuser)
    delete_users_parser.add_argument('username')

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

    if not checkAdmin():
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
