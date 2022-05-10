#!/usr/bin/env python3

import errno
import os
import signal
import subprocess
import sys
import tempfile
import traceback
from act.common import aCTConfig
from act.common import aCTLogger
from act.common.aCTSignal import aCTSignal
from act.common import aCTUtils
from act.common import aCTProcessManager

class aCTMain:
    """
    Main class to run aCT.
    """

    def __init__(self, args):
        # set up signal handlers
        self.signal = aCTSignal()

        # Check we have the right ARC version
        self.checkARC()

        # xml config file
        self.conf = aCTConfig.aCTConfigARC()
        self.appconf = aCTConfig.aCTConfigAPP()

        # Create required directories
        tmpdir = self.conf.get(["tmp", "dir"])
        self.makeDirs(tmpdir)
        self.makeDirs(os.path.join(tmpdir, 'inputfiles'))
        self.makeDirs(os.path.join(tmpdir, 'eventranges'))
        self.makeDirs(os.path.join(tmpdir, 'failedlogs'))
        self.makeDirs(self.conf.get(["voms","proxystoredir"]), 0o700)
        self.makeDirs(self.conf.get(["logger", "logdir"]))

        # logger
        self.logger = aCTLogger.aCTLogger("aCTMain")
        self.log = self.logger()

        # Check if we should run
        self.shouldrun = not os.path.exists(os.path.join(self.conf.get(["actlocation","dir"]), "act.stop"))
        if not self.shouldrun:
            self.log.warning("Detected act.stop file, won't start child processes")

        # daemon operations
        if len(args) >= 2:
            self.daemon(args[1])

        # process manager
        try:
            if self.shouldrun:
                self.procmanager = aCTProcessManager.aCTProcessManager(self.log, self.conf, self.appconf)
        except Exception as e:
            self.log.critical("*** Unexpected exception! ***")
            self.log.critical(traceback.format_exc())
            self.log.critical("*** Process exiting ***")
            raise e


    def checkARC(self):
        """
        Check ARC can be used and is correct version
        """
        try:
            import arc
        except ImportError:
            print('Error: failed to import ARC. Are ARC python bindings installed?')
            sys.exit(1)

        if arc.ARC_VERSION_MAJOR < 4:
            print('Error: Found ARC version %s. aCT requires 4.0.0 or higher' % arc.ARC_VERSION)
            sys.exit(1)

    def makeDirs(self, dir, mode=0o755):
        """
        Make a directory if it doesn't exist already
        """
        try:
            os.makedirs(dir, mode)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise

    def start(self):
        """
        Start daemon
        """
        pidfile = self.conf.get(['actlocation', 'pidfile'])
        try:
            with open(pidfile) as f:
                pid = f.read()
                if pid:
                    print("aCT already running (pid %s)" % pid)
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
            print("fork #1 failed: %d (%s)" % (e.errno, e.strerror))
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
            print("fork #2 failed: %d (%s)" % (e.errno, e.strerror))
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
        os.chdir(self.conf.get(['actlocation', 'dir']))

        # write pidfile
        with open(pidfile,'w+') as f:
            f.write(str(os.getpid()))


    def stop(self):
        """
        Stop daemon
        """
        pidfile = self.conf.get(['actlocation', 'pidfile'])
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
        """
        Start or stop process
        """

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
        """
        Run logrotate to rotate all logs
        """

        logrotateconf = '''
            %s/*.log {
                daily
                dateext
                missingok
                rotate %s
                maxsize 100M
                nocreate
                nocompress
            }''' % (self.conf.get(["logger", "logdir"]),
                    self.conf.get(["logger", "rotate"]))
        logrotatestatus = os.path.join(self.conf.get(["tmp", "dir"]), "logrotate.status")

        # Make a temp file with conf and call logrotate
        with tempfile.NamedTemporaryFile() as temp:
            temp.write(logrotateconf.encode('utf-8'))
            temp.flush()
            command = ['logrotate', '-s', logrotatestatus, temp.name]
            try:
                subprocess.run(command, check=True)
            except (FileNotFoundError, subprocess.CalledProcessError) as e:
                self.log.warning("Failed to run logrotate: %s" % str(e))


    def run(self):
        """
        Main loop
        """
        self.log.info("Running")
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
                    self.log.info("*** Exiting on exit interrupt ***")
                    break

            except:
                self.log.critical("*** Unexpected exception! ***")
                self.log.critical(traceback.format_exc())
                # Reconnect database, in case there was a DB interruption
                try:
                    self.procmanager.reconnectDB()
                except:
                    self.log.critical(traceback.format_exc())
                aCTUtils.sleep(10)

    def finish(self):
        """
        clean finish handled by signals
        """
        self.log.info("Cleanup")

def main():
    am = aCTMain(sys.argv)
    am.run()
    am.finish()

if __name__ == '__main__':
    main()

