import sys


def addCommonArgs(parser):
    parser.add_argument('--proxy', default=None, type=str,
            help='path to proxy file')
    parser.add_argument('--server', default=None, type=str,
            help='URL to aCT server')
    parser.add_argument('--port', default=None, type=int,
            help='port on aCT server')
    parser.add_argument('--conf', default=None, type=str,
            help='path to configuration file')


def checkJobParams(args):
    if not args.all and not args.id:
        print("error: no job IDs given (use -a/--all or --id)")
        sys.exit(1)
    elif args.id and not isCorrectIDString(args.id):
        sys.exit(1)


def addCommonJobFilterArgs(parser):
    parser.add_argument('-a', '--all', action='store_true',
            help='all jobs that match other criteria')
    parser.add_argument('--id', default=None,
            help='a list of IDs of jobs that should be queried')
    parser.add_argument('--name', default=None,
            help='substring that jobs should have in name')


# duplicated from act.client.proxymgr
# Since ARC doesn't seem to complain about non certificate files, should we
# check if given file is actual certificate here?
def readProxyFile(filename):
    try:
        with open(filename, 'r') as f:
            return f.read()
    except Exception as e:
        print('error: read proxy: {}'.format(str(e)))


# duplicated from act.client.clicommon
def showHelpOnCommandOnly(argparser):
    if len(sys.argv) == 1:
        argparser.print_help()
        sys.exit(0)


# modified from act.client.jobmgr.getIDsFromList
# return boolean that tells whether the id string is OK
def isCorrectIDString(listStr):
    groups = listStr.split(',')
    for group in groups:
        try:
            group.index('-')
        except ValueError:
            isRange = False
        else:
            isRange = True

        if isRange:
            try:
                firstIx, lastIx = group.split('-')
            except ValueError: # if there is more than one dash
                print('error: invalid ID range: {}'.format(group))
                return False
            try:
                _ = int(firstIx)
            except ValueError:
                print('error: ID range start: {}'.format(firstIx))
                return False
            try:
                _ = int(lastIx)
            except ValueError:
                print('error: ID range end: {}'.format(firstIx))
                return False
        else:
            try:
                _ = int(group)
            except ValueError:
                print('error: invalid ID: {}'.format(group))
                return False
    return True


