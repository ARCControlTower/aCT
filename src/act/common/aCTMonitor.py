import importlib
import random
import time

from act.arc.aCTARCProcess import aCTProcess
from act.arc.aCTDBArc import aCTDBArc
from act.common.aCTConfig import aCTConfigAPP, aCTConfigARC
from prometheus_client import start_http_server
from prometheus_client.core import REGISTRY, GaugeMetricFamily


class aCTPrometheusCollector:

    def __init__(self, log):
        self.log = log

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
        raise StopIteration

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

        db = aCTDBArc(self.log)
        jobs = db.getGroupedJobs('cluster, arcstate')

        for job in jobs:
            count, cluster, state = (job['count(*)'], job['cluster'] or 'None', job['arcstate'])
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


# TODO: The db is used in the collector class and is created on every collect
#       call. Could that be reworked so that the db would be created once in
#       the process class and then passed to the collector as an argument? Or
#       is it done this way to avoid hangs of the database and avoid
#       reconnections by just connecting every time?
class aCTMonitor(aCTProcess):

    # override parent's init method to not take the cluster argument
    def __init__(self):
        super().__init__()

    def loadConf(self):
        self.conf = aCTConfigARC()

    def wait(self):
        time.sleep(random.randint(5, 11))

    def setup(self):
        super().setup()

        self.loadConf()
        self.prometheus_port = self.conf.monitor.prometheusport or 0

        if self.prometheus_port:
            start_http_server(self.prometheus_port)
            REGISTRY.register(aCTPrometheusCollector(self.log))
        else:
            self.log.info('Prometheus monitoring not enabled')
