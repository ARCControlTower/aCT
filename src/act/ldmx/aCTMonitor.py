from prometheus_client.core import GaugeMetricFamily
from act.ldmx.aCTDBLDMX import aCTDBLDMX

def collect(log):

    waiting = GaugeMetricFamily('ldmx_waiting_jobs',
                                'Waiting jobs in LDMX',
                                labels=['batchid', 'sitename'])

    queued = GaugeMetricFamily('ldmx_queued_jobs',
                               'Queued jobs in LDMX',
                               labels=['batchid', 'sitename'])

    running = GaugeMetricFamily('ldmx_running_jobs',
                                'Running jobs in LDMX',
                                labels=['batchid', 'sitename'])

    finishing = GaugeMetricFamily('ldmx_finishing_jobs',
                                  'Finishing jobs in LDMX',
                                  labels=['batchid', 'sitename'])

    registering = GaugeMetricFamily('ldmx_registering_jobs',
                                    'Registering jobs in LDMX',
                                    labels=['batchid', 'sitename'])

    finished = GaugeMetricFamily('ldmx_finished_jobs',
                                 'Finished jobs in LDMX',
                                 labels=['batchid', 'sitename'])

    failed = GaugeMetricFamily('ldmx_failed_jobs',
                               'Failed jobs in LDMX',
                               labels=['batchid', 'sitename'])
    # Get current jobs
    db = aCTDBLDMX(log)
    rows = db.getGroupedJobs('ldmxstatus, batchname, sitename')

    for row in rows:
        if row['ldmxstatus'] == 'waiting':
            log.debug(f"{row['count(*)']} waiting jobs for batch {row['batchname']} on site {row['sitename']}")
            waiting.add_metric([row['batchname'], row['sitename'] or 'None'], row['count(*)'])

        if row['ldmxstatus'] == 'queueing':
            log.debug(f"{row['count(*)']} queued jobs for batch {row['batchname']} on site {row['sitename']}")
            queued.add_metric([row['batchname'], row['sitename'] or 'None'], row['count(*)'])

        if row['ldmxstatus'] == 'running':
            log.debug(f"{row['count(*)']} running jobs for batch {row['batchname']} on site {row['sitename']}")
            running.add_metric([row['batchname'], row['sitename'] or 'None'], row['count(*)'])

        if row['ldmxstatus'] == 'finishing':
            log.debug(f"{row['count(*)']} finishing jobs for batch {row['batchname']} on site {row['sitename']}")
            finishing.add_metric([row['batchname'], row['sitename'] or 'None'], row['count(*)'])

        if row['ldmxstatus'] == 'registering':
            log.debug(f"{row['count(*)']} registering jobs for batch {row['batchname']} on site {row['sitename']}")
            registering.add_metric([row['batchname'], row['sitename'] or 'None'], row['count(*)'])

        if row['ldmxstatus'] == 'finished':
            log.debug(f"{row['count(*)']} finished jobs for batch {row['batchname']} on site {row['sitename']}")
            finished.add_metric([row['batchname'], row['sitename'] or 'None'], row['count(*)'])

        if row['ldmxstatus'] == 'failed':
            log.debug(f"{row['count(*)']} failed jobs for batch {row['batchname']} on site {row['sitename']}")
            failed.add_metric([row['batchname'], row['sitename'] or 'None'], row['count(*)'])

    yield waiting
    yield queued
    yield running
    yield finishing
    yield finished
    yield failed
