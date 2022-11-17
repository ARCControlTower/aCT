#!/usr/bin/env python3

import errno
import os
import signal
import subprocess
import sys
import tempfile
import traceback
from act.common.aCTConfig import aCTConfigAPP, aCTConfigARC
from act.common.aCTLogger import aCTLogger
from act.common.aCTSignal import aCTSignal
from act.common import aCTUtils
from act.common.aCTProcessManager import aCTProcessManager


class aCTMain:
    '''Main class to run aCT.'''

    def __init__(self, args):
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

        # Check if we should run
        self.shouldrun = not os.path.exists(os.path.join(self.conf.actlocation.dir, 'act.stop'))
        if not self.shouldrun:
            self.log.warning('Detected act.stop file, won\'t start child processes')

        # daemon operations
        if len(args) >= 2:
            self.daemon(args[1])

        # process manager
        try:
            if self.shouldrun:
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

    def start(self):
        '''Start daemon.'''
        pidfile = self.conf.actlocation.pidfile
        try:
            with open(pidfile) as f:
                pid = f.read()
                if pid:
                    print(f'aCT already running (pid {pid})')
                    sys.exit(1)
        except IOError:
            pass

        print('Starting aCT... ')
        # do double fork
        try:
            pid = os.fork()
            if pid > 0:
                # exit first parent
                sys.exit(0)
        except OSError as e:
            print(f'fork #1 failed: {e.errno} ({e.strerror})')
            sys.exit(1)

        # decouple from parent environment
        os.setsid()
        os.umask(0)

        # do second fork
        try:
            pid = os.fork()
            if pid > 0:
                # exit from second parent
                sys.exit(0)
        except OSError as e:
            print(f'fork #2 failed: {e.errno} ({e.strerror})')
            sys.exit(1)

        # redirect standard file descriptors
        sys.stdout.flush()
        sys.stderr.flush()
        si = open('/dev/null', 'r')
        so = open('/dev/null', 'a+')
        se = open('/dev/null', 'a+')
        os.dup2(si.fileno(), sys.stdin.fileno())
        os.dup2(so.fileno(), sys.stdout.fileno())
        os.dup2(se.fileno(), sys.stderr.fileno())

        # change to aCT working dir
        os.chdir(self.conf.actlocation.dir)

        # write pidfile
        with open(pidfile,'w+') as f:
            f.write(str(os.getpid()))

    def stop(self):
        '''Stop daemon.'''
        pidfile = self.conf.actlocation.pidfile
        pid = None
        try:
            with open(pidfile) as f:
                pid = f.read()
        except IOError:
            pass

        if not pid:
            print('aCT already stopped')
            return 1

        try:
            os.kill(int(pid), signal.SIGTERM)
        except OSError: # already stopped
            pass

        os.remove(pidfile)
        print('Stopping aCT... ', end=' ')
        sys.stdout.flush()
        while True:
            try:
                aCTUtils.sleep(1)
                os.kill(int(pid), 0)
            except OSError as err:
                if err.errno == errno.ESRCH:
                    break
        print('stopped')
        return 0


    def daemon(self, operation):
        '''Start or stop process.'''

        if operation == 'start':
            self.start()
        elif operation == 'stop':
            sys.exit(self.stop())
        elif operation == 'restart':
            self.stop()
            self.start()
        else:
            print('Usage: python aCTMain.py [start|stop|restart]')
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
                if self.shouldrun:
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
    am = aCTMain(sys.argv)
    am.run()
    am.finish()


if __name__ == '__main__':
    main()
