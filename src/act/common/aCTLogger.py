import os
import sys
import logging.handlers
from .aCTConfig import aCTConfigARC

import arc

LEVELS = {'debug': logging.DEBUG,
          'info': logging.INFO,
          'warning': logging.WARNING,
          'error': logging.ERROR,
          'critical': logging.CRITICAL}


class aCTLogger:

    def __init__(self,name,cluster='',arclog=True,stderr=False):
        self.conf = aCTConfigARC()
        self.logger=logging.LoggerAdapter(logging.getLogger(name), {'cluster': cluster})
        self.logger.logger.setLevel(logging.DEBUG)
        level = LEVELS.get(self.conf.logger.level, logging.NOTSET)
        self.logger.logger.setLevel(level)
        logfile = os.path.join(self.conf.logger.logdir, name + '.log')
        if stderr:
            self.handler = logging.StreamHandler()
        else:
            # aCTMain calls logrotate to rotate logs
            self.handler = logging.handlers.WatchedFileHandler(logfile)

        if cluster:
            self.formatter = logging.Formatter("[%(asctime)s] [%(filename)s:%(lineno)d] [%(levelname)s] [%(cluster)s] - %(message)s")
        else:
            self.formatter = logging.Formatter("[%(asctime)s] [%(filename)s:%(lineno)d] [%(levelname)s] - %(message)s")

        self.handler.setFormatter(self.formatter)
        self.logger.logger.addHandler(self.handler)

        if arclog:
            if stderr:
                self.arclog = arc.LogStream(sys.stderr)
            else:
                self.arclog = arc.LogFile(str(logfile))
            self.arclog.setFormat(arc.LongFormat)
            arc.Logger_getRootLogger().addDestination(self.arclog)
            if self.conf.logger.arclevel:
                arc.Logger_getRootLogger().setThreshold(arc.string_to_level(self.conf.logger.arclevel.upper()))
            else:
                arc.Logger_getRootLogger().setThreshold(arc.ERROR)

    def log(self,level,message,*args, **kwargs):
        lvl = LEVELS.get(level, logging.NOTSET)
        self.logger.log(lvl,message,*args, **kwargs)

    def write(self,s):
        if s == "\n":
            return
        self.logger.info(s)

    def __call__(self):
        return self.logger

if __name__ == '__main__':
    l=aCTLogger("downloader")
    #l.log("info","test")
    #for i in range(0,100000):
    #    l.log("error","error")
    #l.logger.info("test")
    l().info("test")
    import sys
    sys.stdout=l
    print("test print")
    print("test print1")
