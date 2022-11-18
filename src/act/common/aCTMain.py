import os
import subprocess
import sys
import tempfile
import traceback

from act.common import aCTUtils
from act.common.aCTConfig import aCTConfigAPP, aCTConfigARC
from act.common.aCTLogger import aCTLogger
from act.common.aCTProcessManager import aCTProcessManager
from act.common.aCTSignal import aCTSignal


class aCTMain:
    '''Main class to run aCT.'''

    def __init__(self):
        # Check we have the right ARC version
        self.checkARC()

        # xml config file
        self.conf = aCTConfigARC()
        self.appconf = aCTConfigAPP()

        # Create required directories
        tmpdir = self.conf.tmp.dir
        os.makedirs(os.path.join(tmpdir, 'inputfiles'), mode=0o755, exist_ok=True)
        os.makedirs(os.path.join(tmpdir, 'failedlogs'), mode=0o755, exist_ok=True)
        os.makedirs(self.conf.voms.proxystoredir, mode=0o700, exist_ok=True)
        os.makedirs(self.conf.logger.logdir, mode=0o755, exist_ok=True)

        # logger
        self.logger = aCTLogger('aCTMain')
        self.log = self.logger()

        # set up signal handlers
        self.signal = aCTSignal(self.log)

        # change to aCT working dir
        os.chdir(self.conf.actlocation.dir)

        # process manager
        try:
            self.procmanager = aCTProcessManager(self.log, self.conf, self.appconf)
        except Exception as e:
            self.log.critical('*** Unexpected exception! ***')
            self.log.critical(traceback.format_exc())
            self.log.critical('*** Process exiting ***')
            raise e

    def checkARC(self):
        '''Check ARC can be used and is correct version.'''
        try:
            import arc
        except ImportError:
            print('Error: failed to import ARC. Are ARC python bindings installed?')
            sys.exit(1)

        if arc.ARC_VERSION_MAJOR < 4:
            print(f'Error: Found ARC version {arc.ARC_VERSION}. aCT requires 4.0.0 or higher')
            sys.exit(1)

    def logrotate(self):
        '''Run logrotate to rotate all logs.'''
        # double braces are escaped braces
        logrotateconf = f'''
            {self.conf.logger.logdir}/*.log {{
                daily
                dateext
                missingok
                rotate {self.conf.logger.rotate}
                maxsize 100M
                nocreate
                nocompress
            }}'''
        logrotatestatus = os.path.join(self.conf.tmp.dir, 'logrotate.status')

        # Make a temp file with conf and call logrotate
        with tempfile.NamedTemporaryFile() as temp:
            temp.write(logrotateconf.encode('utf-8'))
            temp.flush()
            command = ['logrotate', '-s', logrotatestatus, temp.name]
            try:
                subprocess.run(command, check=True)
            except (FileNotFoundError, subprocess.CalledProcessError) as e:
                self.log.warning(f'Failed to run logrotate: {e}')

    def run(self):
        '''Run main loop.'''
        self.log.info('Running')
        while 1:
            try:
                # Rotate logs
                self.logrotate()
                # (re)start new processes as necessary
                self.procmanager.checkARCClusters()
                self.procmanager.checkCondorClusters()
                # sleep
                aCTUtils.sleep(10)

                if self.signal.isInterrupted():
                    self.log.info('*** Exiting on exit interrupt ***')
                    break

            except:
                self.log.critical('*** Unexpected exception! ***')
                self.log.critical(traceback.format_exc())
                # Reconnect database, in case there was a DB interruption
                try:
                    self.procmanager.reconnectDB()
                except:
                    self.log.critical(traceback.format_exc())
                aCTUtils.sleep(10)

    def finish(self):
        '''Do cleanup on normal exit by signal.'''
        self.log.info('Cleanup')


def main():
    am = aCTMain()
    am.run()
    am.finish()


if __name__ == '__main__':
    main()
