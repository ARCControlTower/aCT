"""
A set of functions and variables for handling configuration using ConfigParser.

Every program gets configuration in 4 steps:
    1. as parameters for program
    2. remaining not provided parameters are read from config file given
    as parameter
    3. remaining not provided parameters are read from default config file
    4. remaining not provided parameters are given hardcoded values

If parameter is not available in any of first 3 steps, it remains None and is
given some hardcoded value in 4th step.
"""

import configparser
import io
import os
import sys

# program parameters that are paths and might have to be expanded
# (~) as home folder
PATH_PARAMS = ('proxy', )#'cert', 'key', 'cadir')

# defaults that are used if no parameters or config is available
DEFAULT_CONF = os.path.join(os.path.expanduser('~'), '.aCT-client', 'aCT-client.conf')
DEFAULTS = {
    'proxy':    '/tmp/x509up_u' + str(os.getuid()),
    'server':   'http://localhost',
    'port':     8000,
    ##'cert':     os.path.join(os.path.expanduser('~'), '.globus', 'usercert.pem'),
    ##'key':      os.path.join(os.path.expanduser('~'), '.globus', 'userkey.pem'),
    ##'cadir':    '/etc/grid-security/certificates',
}

def parseConfFile(confFile, confDict):
    """
    Parse configuration file into given dictionary.

    All dictionary keys with None values will be taken from config file.
    If they are not present in config file, they remain None.
    """
    # insert dummy section, as config parser requires it
    confStr = '[dummy]\n' + confFile.read()
    confFp = io.StringIO(confStr)
    confParser = configparser.RawConfigParser()
    confParser.readfp(confFp)
    config = dict(confParser.items('dummy'))

    for key, value in confDict.items():
        if value == None:
            confDict[key] = config.get(key, None)


def setDefaults(confDict):
    """Set all None parameters to default values in config dictionary."""
    for key, value in confDict.items():
        if value is None:
            confDict[key] = DEFAULTS.get(key, None)


def expandPaths(confDict):
    """Expand home directories (~) in path parameters of config dictionary."""
    for param in PATH_PARAMS:
        if param in confDict:
            confDict[param] = os.path.expanduser(confDict[param])


def parseParamConf(confName, confDict):
    """
    Parse config file to dictionary and exit on error.

    If there are any errors, user should be notified and fix the problem.
    """
    try:
        confFile = open(confName, 'r')
    except Exception as e:
        print('error: {}'.format(str(e)))
        sys.exit(2)
    else:
        parseConfFile(confFile, confDict)
        confFile.close()


def parseDefaultConf(confDict):
    """
    Parse default config file to dictionary and don't exit on error.

    Since default config files are not mandatory, the problem with the
    existence of file can be ignored.
    """
    try:
        confFile = open(DEFAULT_CONF, 'r')
    except Exception as e:
        # Program doesn't complain when default conf is missing because that
        # might be intent of a user.
        pass
    else:
        parseConfFile(confFile, confDict)
        confFile.close()


def parseNonParamConf(confDict, confArg):
    """Parse from config files and use defaults for any not given parameter."""
    # get remaining config from conf file parameter
    if None in confDict.values() and confArg:
        parseParamConf(confArg, confDict)

    # get remaining config from default conf file
    if None in confDict.values():
        parseDefaultConf(confDict)

    # use defaults for all remaining not given parameters
    setDefaults(confDict)

    # if any path has '~' character, expand it
    expandPaths(confDict)


