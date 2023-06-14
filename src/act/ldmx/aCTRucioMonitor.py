# Prometheus exporter of Rucio RSE information
# Should eventually move to Rucio probes


from act.ldmx.aCTLDMXProcess import aCTLDMXProcess
from prometheus_client import start_http_server
from prometheus_client.core import REGISTRY, GaugeMetricFamily
from rucio.client import Client


class aCTRucioCollector:

    def __init__(self):
        self.metrics = {} # {RSE: {'used': 12324, 'total': 10000000, 'files': 34}}

    def collect(self):

        rucio_rse_used = GaugeMetricFamily('rucio_rse_used',
                                           'Used space per RSE',
                                           labels=['rse_name'])

        rucio_rse_total = GaugeMetricFamily('rucio_rse_total',
                                            'Total space per RSE',
                                            labels=['rse_name'])

        rucio_rse_files = GaugeMetricFamily('rucio_rse_files',
                                            'Number of files per RSE',
                                            labels=['rse_name'])

        for rse, metric in self.metrics.items():
            rucio_rse_used.add_metric([rse], metric['used'])
            rucio_rse_files.add_metric([rse], metric['files'])
            rucio_rse_total.add_metric([rse], metric['total'])

        yield rucio_rse_used
        yield rucio_rse_total
        yield rucio_rse_files


class aCTRucioMonitor(aCTLDMXProcess):

    def setup(self):
        super().setup()

        self.rucio = Client()
        self.rucio_prometheus_port = self.arcconf.monitor.rucioprometheusport or 0

        if self.rucio_prometheus_port:
            start_http_server(self.rucio_prometheus_port)
            self.collector = aCTRucioCollector()
            REGISTRY.register(self.collector)
        else:
            self.log.info('Prometheus monitoring not enabled')

    def wait(self, limit=120):
        super().wait(limit)

    def process(self):
        '''Actual metric gathering from Rucio is done at a low frequency here'''
        self.setSites()

        if not self.rucio_prometheus_port:
            return

        rses = self.rucio.list_rses()
        metrics = {}
        for rse in rses:
            info = self.rucio.get_rse_usage(rse['rse'], filters={'source': 'rucio'})
            metrics[rse['rse']] = next(info)
            info = self.rucio.get_rse_usage(rse['rse'], filters={'source': 'storage'})
            # Storage info is not always defined
            try:
                metrics[rse['rse']]['total'] = next(info)['total']
            except StopIteration:
                metrics[rse['rse']]['total'] = 0

        self.collector.metrics = metrics
