# Monitor process to export prometheus data
import importlib
from prometheus_client import start_http_server
from prometheus_client.core import GaugeMetricFamily, REGISTRY
from act.common.aCTProcess import aCTProcess
from act.common.aCTConfig import aCTConfigAPP
from act.arc.aCTDBArc import aCTDBArc

class aCTPrometheusCollector:

    def __init__(self, log):
        self.log = log

    def app_collect(self):

        appconf = aCTConfigAPP()
        apps = appconf.modules.app
        for app in apps:
            try:
                yield from importlib.import_module(f'{app}.aCTMonitor').collect(self.log)
                self.log.info(f'Added metrics from {app}.aCTMonitor')
            except ModuleNotFoundError as e:
                self.log.info(f'No collect in module {app}')
            except AttributeError:
                self.log.info(f'aCTMonitor.collect() not found in {app}')
            except Exception as e:
                self.log.error(f'Exception running {app}.aCTMonitor.collect: {e}')
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

class aCTMonitor(aCTProcess):

    def __init__(self):
        aCTProcess.__init__(self)
        self.prometheus_port = self.conf.monitor.prometheusport or 0

        if self.prometheus_port:
            start_http_server(self.prometheus_port)
            REGISTRY.register(aCTPrometheusCollector(self.log))
        else:
            self.log.info('Prometheus monitoring not enabled')

    def process(self):
        pass


if __name__ == '__main__':
    am = aCTMonitor()
    am.run()
    am.finish()
