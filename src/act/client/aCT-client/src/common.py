

# Since ARC doesn't seem to complain about non certificate files, should we
# check if given file is actual certificate here?
def readProxyFile(filename):
    try:
        with open(filename, 'r') as f:
            return f.read()
    except Exception as e:
        print('error: read proxy: {}'.format(str(e)))


def addCommonArguments(parser):
    parser.add_argument('--proxy', default=None, type=str,
            help='path to proxy file')
    parser.add_argument('--server', default=None, type=str,
            help='URL to aCT server')
    parser.add_argument('--port', default=None, type=int,
            help='port on aCT server')
    parser.add_argument('--conf', default=None, type=str,
            help='path to configuration file')


