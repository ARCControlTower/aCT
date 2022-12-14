import os
import subprocess
import sys
import tempfile
import time
import signal

from act.common.aCTConfig import aCTConfigAPP, aCTConfigARC
from act.common.aCTProcessManager import aCTProcessManager
from act.common.aCTProcess import aCTProcess, exitHandler
from act.common.aCTSignal import aCTSignalDeferrer


class aCTMain(aCTProcess):
    '''Main class to run aCT.'''

    def loadConf(self):
        self.conf = aCTConfigARC()
        self.appconf = aCTConfigAPP()

    def setup(self):
        # check we have the right ARC version
        self.checkARC()

        super().setup()

        # override handlers for SIGINT and SIGTERM to support running the main
        # process from terminal
        signal.signal(signal.SIGINT, mainExitHandler)
        signal.signal(signal.SIGTERM, mainExitHandler)
        self.sigdefer = aCTSignalDeferrer(self.log, signal.SIGINT, signal.SIGTERM)

        self.loadConf()

        # create required directories
        tmpdir = self.conf.tmp.dir
        os.makedirs(os.path.join(tmpdir, 'inputfiles'), mode=0o755, exist_ok=True)
        os.makedirs(os.path.join(tmpdir, 'failedlogs'), mode=0o755, exist_ok=True)
        os.makedirs(self.conf.voms.proxystoredir, mode=0o700, exist_ok=True)
        os.makedirs(self.conf.logger.logdir, mode=0o755, exist_ok=True)

        self.procmanager = aCTProcessManager(self.log)

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

    def wait(self):
        time.sleep(10)

    def process(self):
        self.log.debug("Rotating logs ...")
        self.logrotate()

        self.log.debug("Updating running processes ...")
        self.procmanager.update()

    def finish(self):
        '''Do cleanup on normal exit by signal.'''
        self.log.info('Stopping all processes ...')
        self.procmanager.stopAllProcesses()


def mainExitHandler(signum, frame):
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    exitHandler(signum, frame)


def main():
    am = aCTMain()
    am.run()
