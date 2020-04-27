import importlib
import sys
import os
from act.common.aCTLogger import aCTLogger
from act.common.aCTConfig import aCTConfigARC, aCTConfigAPP
from act.arc.aCTDBArc import aCTDBArc
from act.condor.aCTDBCondor import aCTDBCondor

def bootstrap_conf():
    '''Check config is ok'''
    try:
        arcconf = aCTConfigARC()
    except Exception as e:
        print('Error processing ARC config file: %s' % str(e))
        sys.exit(1)

    try:
        atlasconf = aCTConfigAPP()
    except Exception as e:
        print('Error processing APP config file: %s' % str(e))
        sys.exit(1)

def bootstrap_dirs():
    '''Make necessary directories'''
    arcconf = aCTConfigARC()
    os.makedirs(arcconf.get(['tmp', 'dir']), mode=0o755, exist_ok=True)
    os.makedirs(arcconf.get(['logger', 'logdir']), mode=0o755, exist_ok=True)

def bootstrap_db():
    '''Set up the ARC and Condor DB tables'''
    logger = aCTLogger('aCTBootstrap')
    log = logger()
    dbarc = aCTDBArc(log)
    dbcondor = aCTDBCondor(log)
    print('Setting up ARC tables...')
    if not dbarc.createTables():
        print('Error creating arc tables, see aCTBootstrap.log for details')
    print('Setting up Condor tables...')
    if not dbcondor.createTables():
        print('Error creating condor tables, see aCTBootstrap.log for details')

def bootstrap_app():
    '''Set up app-specific things'''
    appconf = aCTConfigAPP()
    apps = appconf.getList(["modules", "app"])
    for app in apps:
        print(f'Setting up app from {app}...')
        try:
            ap = importlib.import_module(f'{app}.aCTBootstrap').bootstrap
            ap()
        except ModuleNotFoundError as e:
            print(f'No bootstrap in module {app}')
        except AttributeError:
            print(f'aCTBootstrap.bootstrap() not found in {app}')
        except Exception as e:
            print(f'Exception running {app}.aCTBootstrap.bootstrap(): {e}')


def main():

    bootstrap_conf()
    bootstrap_dirs()
    bootstrap_db()
    bootstrap_app()


if __name__ == '__main__':
    main()
