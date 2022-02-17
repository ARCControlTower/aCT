"""
Process that transfers jobs from clientjobs to arcjobs table.

This program creates an object that acts as a long running process.
It is managed by another process, defined in
:mod:`act.common.aCTProcessManager`.
"""

import os
import sys
import traceback
import time

from act.arc.aCTDBArc import aCTDBArc
from act.common.aCTConfig import aCTConfigARC
from act.common.aCTLogger import aCTLogger
from act.common.aCTSignal import ExceptInterrupt
from act.client.clientdb import ClientDB


class Client2Arc(object):
    """
    Object that runs until interrupted and periodically submits new jobs.

    This object is very similar to other process objects, namely
    :class:~`act.common.aCTProcess.aCTProcess` and children, as well
    as :class:~`act.common.aCTATLASProcess.aCTATLASProcess` and children.

    Attributes:
        name: Name of a process, extracted from source code file.
        arcconf: An object that reads configuration of ARC engine.
        logger: An object that provides logging facility.
        log: An object used for emiting log messages.
        clidb: An object that provides interface to client engine's table.
        arcdb: An object that provides interface to ARC engine's table.
    """

    def __init__(self):
        """Initialize all attributes."""
        # get name, remove .py from the end
        self.name = os.path.basename(sys.argv[0])[:-3]

        self.arcconf = aCTConfigARC()

        self.logger = aCTLogger(self.name)
        self.log = self.logger()
        self.clidb = ClientDB(self.log)
        self.arcdb = aCTDBArc(self.log)

        self.log.info('Started {}'.format(self.name))

    def run(self):
        """
        Run until interrupted by signal.

        The actual work of object is done in :meth:`process` which is
        called every iteration. Interrupt signal comes from
        :class:~`act.common.aCTProcessManager.aCTProcessManager`.
        """
        try:
            while True:
                # TODO: this parsing does not make any difference
                self.arcconf.parse()
                self.process()
                time.sleep(10)  # TODO: HARDCODED
        except ExceptInterrupt as x:
            self.log.info('Received interrupt {}, exiting'.format(str(x)))
        except:
            self.log.critical('*** Unexpected exception! ***')
            self.log.critical(traceback.format_exc())
            self.log.critical('*** Process exiting ***')

    def process(self):
        """
        Check if new jobs should be submitted.

        New jobs should be submitted if there are not enough submitted or
        running jobs. Currently, proxyid is used for fairshare mechanism.
        Hardcoded constants are used for simplicity for now when determining
        whether and how many new jobs should be submitted.
        """
        proxies = self.clidb.getProxies()
        for proxyid in proxies:
            # get number of all states of jobs with fairshare proxyid
            states = self.getJobStateCount('fairshare = {}'.format(proxyid))

            # get number of running and submitted jobs
            running = 0
            submitted = 0
            for state in states:
                if state['arcstate'] == 'running':
                    running = state['COUNT(arcstate)']
                elif state['arcstate'] in ('submitted', 'submitting'):
                    submitted += state['COUNT(arcstate)']
            #self.log.debug('{} jobs running for proxyid {}'.format(running, proxyid))
            #self.log.debug('{} jobs submitted for proxyid {}'.format(submitted, proxyid))

            if submitted < max(0.2 * running, 100): # TODO: HARDCODED
                self.insertNewJobs(proxyid, 20)

    def getJobStateCount(self, select):
        """
        Return states and numbers of jobs in those states.

        Args:
            select: A string with custom WHERE clause for MySQL query.

        Returns:
            A list of dictionaries where every dictionary has a state name
            and number of jobs in that state. State name is mapped by
            'arcstate', number of jobs by 'COUNT(arcstate). For example:

            [{'arcstate': 'toclean', 'COUNT(arcstate)': 10},
             {'arcstate': 'running', 'COUNT(arcstate)': 58}]
        """
        c = self.arcdb.db.getCursor()
        try:
            c.execute(
                'SELECT arcstate,COUNT(arcstate) \
                FROM arcjobs \
                WHERE {} \
                GROUP BY arcstate'.format(select)
            )
        except:
            self.log.exception('Error getting job info from arc table')
        else:
            return c.fetchall()

    def insertNewJobs(self, proxyid, num):
        """
        Insert new jobs to ARC table.

        For now, the jobs with no arcjobid and lowest id get inserted.
        """
        # Get jobs that haven't been inserted to ARC table yet
        # (they don't have reference to ARC table, arcjobid is null).
        jobs = self.clidb.getJobsInfo(
            ['id', 'jobdesc', 'clusterlist'],
            where='proxyid = %s AND arcjobid IS NULL AND jobdesc IS NOT NULL',
            where_params=[proxyid],
            order_by='%s',
            order_by_params=['id'],
            limit=num
        )
        for job in jobs:
            clusterlist = job['clusterlist']

            # get job description, needed for setting priority
            jobdesc = self.arcdb.getArcJobDescription(job['jobdesc'])

            # insert job to ARC table
            try:
                row = self.clidb.insertArcJob(
                    jobdesc,
                    job['jobdesc'],
                    proxyid,
                    0,
                    clusterlist,
                    job['id'],
                    '',
                    proxyid
                )
            except:
                self.log.exception('Error inserting job {} to arc table'.format(job['id']))
            else: # create reference to job in client table
                self.clidb.updateJob(job['id'], {'arcjobid': row['LAST_INSERT_ID()']})


    def finish(self):
        """Log stop message."""
        self.log.info('Stopped {}'.format(self.name))


if __name__ == '__main__':
    proc = Client2Arc()
    proc.run()
    proc.finish()


