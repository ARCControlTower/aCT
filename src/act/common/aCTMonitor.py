import importlib
import random

from act.arc.aCTARCProcess import aCTProcess
from act.arc.aCTDBArcNEW import aCTDBArc
from act.common.aCTConfig import aCTConfigAPP, aCTConfigARC
from prometheus_client import start_http_server
from prometheus_client.core import REGISTRY, GaugeMetricFamily
from sqlalchemy import select, func
from act.arc.dbModels import ArcJob


class aCTPrometheusCollector:

    def __init__(self, log, db):
        self.log = log
        # DB object created once by the process and reused for every scrape
        self.db = db

    def app_collect(self):

        appconf = aCTConfigAPP()
        modules = appconf.modules
        for module in modules:
            try:
                yield from importlib.import_module(f'{module}.aCTMonitor').collect(self.log)
                self.log.info(f'Added metrics from {module}.aCTMonitor')
            except ModuleNotFoundError:
                self.log.info(f'No collect in module {module}')
            except AttributeError:
                self.log.info(f'aCTMonitor.collect() not found in {module}')
            except Exception as e:
                self.log.error(f'Exception running {module}.aCTMonitor.collect: {e}')

    def collect(self):
        queued_arc_jobs = GaugeMetricFamily('arc_queued_jobs',
                                            'Queued jobs per ARC CE',
                                            labels=['ce_endpoint'])

        running_arc_jobs = GaugeMetricFamily('arc_running_jobs',
                                             'Running jobs per ARC CE',
                                             labels=['ce_endpoint'])

        finishing_arc_jobs = GaugeMetricFamily('arc_finishing_jobs',
                                               'Finishing jobs per ARC CE',
                                               labels=['ce_endpoint'])

        with self.db.Session.begin() as session:
            jobs = session.execute(select(func.count().label('counts'), ArcJob.cluster, ArcJob.arcstate).select_from(ArcJob).group_by(ArcJob.cluster, ArcJob.arcstate)).all()

        for job in jobs:
            count, cluster, state = (job.counts, job.cluster or 'None', job.arcstate)
            if state == 'submitted':
                queued_arc_jobs.add_metric([cluster], count)
            if state == 'running':
                running_arc_jobs.add_metric([cluster], count)
            if state == 'finishing':
                finishing_arc_jobs.add_metric([cluster], count)

        yield queued_arc_jobs
        yield running_arc_jobs
        yield finishing_arc_jobs
        yield from self.app_collect()


class aCTMonitor(aCTProcess):

    # override parent's init method to not take the cluster argument
    def __init__(self):
        super().__init__()

    def loadConf(self):
        self.conf = aCTConfigARC()

    def wait(self, limit=random.randint(5, 11)):
        super().wait(limit)

    def setup(self):
        super().setup()

        self.loadConf()
        self.prometheus_port = self.conf.monitor.prometheusport or 0

        if self.prometheus_port:
            # one DB object for all scrapes; pool_pre_ping handles dropped connections
            self.db = aCTDBArc(self.log)
            start_http_server(self.prometheus_port)
            REGISTRY.register(aCTPrometheusCollector(self.log, self.db))
        else:
            self.log.info('Prometheus monitoring not enabled')
