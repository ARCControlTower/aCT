"""
A set of functions and variables for handling "layered configuration".

Every program gets configuration from a combination of different sources:
    - command line parameters
    - config file (default or provided as CLI argument)
    - some settings should have hardcoded values

Some parameters are paths and we want to allow users to use environment
variables and tilda (PATH_KEYS).
"""

import os

import yaml

from act_client.common import ACTClientError

# program parameters that are paths have to be expanded (env vars, tilda)
PATH_KEYS = ('proxy', 'token', )
DEFAULT_KEYS = ('proxy', 'token', )

# construct default paths for config and token files
DIRNAME = 'act-client' # name of directories for configuration and data
CONF_HOME = os.path.expandvars('$XDG_CONFIG_HOME')
DATA_HOME = os.path.expandvars('$XDG_DATA_HOME')
CONF_NAME = 'config.yaml'
TOKEN_NAME = 'token'

# XDG Base Directory specification use
if CONF_HOME == '$XDG_CONFIG_HOME':
    CONF_BASE = os.path.join(os.path.expandvars('$HOME'), '.config', DIRNAME)
else:
    CONF_BASE = os.path.join(CONF_HOME, DIRNAME)

if DATA_HOME == '$XDG_DATA_HOME':
    DATA_BASE = os.path.join(os.path.expandvars('$HOME'), '.local', 'share', DIRNAME)
else:
    DATA_BASE = os.path.join(DATA_HOME, DIRNAME)

# it is convenient to have hardcoded defaults for some settings
DEFAULT_CONF = {
    'proxy': f'/tmp/x509up_u{os.getuid()}',
    'token': os.path.join(DATA_BASE, TOKEN_NAME),
}

# default configuration path is not addressed by key and not needed from outside
# this module so it is a separate constant
DEFAULT_CONF_PATH = os.path.join(CONF_BASE, CONF_NAME)


def loadConf(**kwargs):
    # load config from file
    path = kwargs.get('path', '')
    if not path:
        path = DEFAULT_CONF_PATH
    try:
        with open(path, 'r') as confFile:
            yamlstr = confFile.read()
        config = yaml.safe_load(yamlstr)
    except Exception as e:
        raise ACTClientError(str(e))

    # add missing keys that have hardcoded defaults
    for key in DEFAULT_KEYS:
        if key not in config:
            config[key] = DEFAULT_CONF[key]

    return config


def expandPaths(conf):
    for param in PATH_KEYS:
        if param in conf:
            conf[param] = os.path.expanduser(conf[param])
            conf[param] = os.path.expandvars(conf[param])


def checkConf(config, keyList):
    for key in keyList:
        if key not in config:
            raise ACTClientError(f'Config key {key} not configured')
