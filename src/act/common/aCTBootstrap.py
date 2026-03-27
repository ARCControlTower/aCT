import importlib
import sys
import os
from act.common.aCTLogger import aCTLogger
from act.common.aCTConfig import aCTConfigARC, aCTConfigAPP
from act.arc.aCTDBArcNEW import aCTDBArc
from act.condor.aCTDBCondor import aCTDBCondor
from act.arc.dbModels import Base
from sqlalchemy import inspect


def bootstrap_conf():
    '''Check config is ok'''
    try:
        arcconf = aCTConfigARC()
    except Exception as e:
        print('Error processing ARC config file: %s' % str(e))
        sys.exit(1)

    try:
        appconf = aCTConfigAPP()
    except Exception as e:
        print('Error processing APP config file: %s' % str(e))
        sys.exit(1)


def bootstrap_dirs():
    '''Make necessary directories'''
    arcconf = aCTConfigARC()
    os.makedirs(arcconf.tmp.dir, mode=0o755, exist_ok=True)
    os.makedirs(arcconf.logger.logdir, mode=0o755, exist_ok=True)

def bootstrap_db():
    '''Set up the aCT and app tables'''
    logger = aCTLogger('aCTBootstrap')
    log = logger()

    print('Setting up ARC tables...')
    appconf = aCTConfigAPP()

    # Import models dynamically
    for app in appconf.modules:
        print(f'Setting up db from {app}...')
        try:
            importlib.import_module(f'{app}.dbModels')
        except ModuleNotFoundError:
            print(f'No dbModels in module {app}')
        except AttributeError:
            print(f'dbModels not found in {app}')
        except Exception as e:
            print(f'Exception running {app}.dbModels: {e}')
    db = aCTDBArc(log)
    inspector = inspect(db.engine)

    # Only check OUR tables
    our_tables = set(Base.metadata.tables.keys())
    existing_tables = set(inspector.get_table_names())
    overlap = our_tables & existing_tables

    if overlap:
        print(f"These tables already exist: {list(overlap)}")
        answer = input("Drop and recreate them? (y/n) ").strip().lower()
        if answer not in ('y', 'yes'):
            print("Aborting table creation.")
            return

    # Create tables
    print("Creating tables:", Base.metadata.tables.keys())
    Base.metadata.drop_all(db.engine)
    Base.metadata.create_all(db.engine)
    print("Tables created!")


def bootstrap_app():
    '''Set up app-specific things'''
    appconf = aCTConfigAPP()
    for app in appconf.modules:
        print(f'Setting up app from {app}...')
        try:
            ap = importlib.import_module(f'{app}.aCTBootstrap').bootstrap
            ap()
        except ModuleNotFoundError:
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
