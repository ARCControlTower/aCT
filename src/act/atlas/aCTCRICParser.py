import logging
import time
import os
import sys
import json
from act.common.aCTConfig import aCTConfigAPP, aCTConfigARC

class aCTCRICParser:
    '''
    Load cric jsons. If file changes since last load, reload. Then load site
    info from config and overwrite cric values.
    '''

    def __init__(self, logger):
        self.log = logger
        self.conf = aCTConfigAPP()
        self.arcconf = aCTConfigARC()
        self.tparse = 0
        self.getSites()

    def _parseConfigSites(self):
        sites = {}

        for sitename, site in self.conf.panda.sites:
            siteinfo = {}
            if site.endpoints:
                siteinfo['endpoints'] = site.endpoints
            if site.flavour:
                siteinfo['flavour'] = site.flavour
            if site.schedconfig:
                siteinfo['schedconfig'] = site.schedconfig
            if site.type:
                siteinfo['type'] = site.type
            if site.corecount:
                siteinfo['corecount'] = site.corecount
            if site.maxjobs:
                siteinfo['maxjobs'] = site.maxjobs
            if site.truepilot:
                siteinfo['truepilot'] = site.truepilot
            if site.push:
                siteinfo['push'] = site.push

            siteinfo['cricjsons'] = site.cricjsons or 0
            siteinfo['status'] = site.status or 'online'
            siteinfo['enabled'] = True
            sites[sitename] = siteinfo

        self.log.info(f"Parsed sites from config: {sites.keys()}")
        return sites

    def _parseCRICJson(self, cricfilename, pilotmgr, pilotver):
        with open(cricfilename) as f:
            sites = json.load(f)
        for sitename, siteinfo in sites.items():
            siteinfo['push'] = 'push' in (siteinfo['workflow'] or 'push')
            siteinfo['schedconfig'] = sitename
            if (pilotmgr == 'all' or siteinfo['pilot_manager'] == pilotmgr) and \
               (pilotver is None or siteinfo['pilot_version'] == str(pilotver)) and \
               siteinfo['state'] == 'ACTIVE' and not siteinfo['is_virtual']:
                siteinfo['enabled'] = True
                siteinfo['maxjobs'] = self.conf.cric.maxjobs
            else:
                siteinfo['enabled'] = False
                siteinfo['maxjobs'] = 0
            siteinfo['corecount'] = siteinfo.get('corecount', 1)
            if siteinfo.get('queues'):
                siteinfo['flavour'] = siteinfo['queues'][0]['ce_flavour']
            else:
                siteinfo['flavour'] = 'UNKNOWN'
            # pull out endpoints
            endpoints = []
            for queue in siteinfo['queues']:
                if queue.get('ce_state') != 'ACTIVE':
                    if siteinfo['enabled']:
                        self.log.info(f"Skipping inactive CE {queue.get('ce endpoint')}")
                    continue
                if queue['ce_flavour'] == 'CREAM-CE':
                    endpoints.append('cream %s/ce-cream/services/CREAM2 %s %s' % (queue['ce_endpoint'], queue['ce_jobmanager'], queue['ce_queue_name']))
                elif queue['ce_flavour'] == 'HTCONDOR-CE':
                    endpoints.append('condor %s %s %s' % (queue['ce_endpoint'].split(':')[0], queue['ce_endpoint'], queue['ce_queue_name']))
                elif queue['ce_flavour'] == 'ARC-CE':
                    endpoints.append('%s/%s' % (queue['ce_endpoint'], queue['ce_queue_name']))
                else:
                    if siteinfo['enabled']:
                        self.log.warning(f"Cannot use CE flavour {queue['ce flavour']} for queue {sitename}")
            # Ignore endpoints with "default" queue unless that is the only queue
            nondefaultendpoints = [e for e in endpoints if not e.endswith(' default')]
            if not nondefaultendpoints:
                siteinfo['endpoints'] = endpoints
            else:
                siteinfo['endpoints'] = nondefaultendpoints
            if 'maxtime' not in siteinfo or siteinfo['maxtime'] == 0:
                try:
                    maxwalltime = max([int(queue['ce_queue_maxwctime']) for queue in siteinfo['queues']])
                except:
                    maxwalltime = 0
                # if maxwalltime is not set or is larger than a week, then set to 1 week
                if maxwalltime <= 0:
                    maxwalltime = 60*24*7
                else:
                    maxwalltime = min(maxwalltime, 60*24*7)
                siteinfo['maxwalltime'] = maxwalltime
            else:
                siteinfo['maxwalltime'] = min(int(siteinfo['maxtime'])/60, 60*24*7)
            if siteinfo['type'] == 'special':
                siteinfo['type'] = 'production'
            # true pilot or not, based on whether mv copytool is used
            truepilot = True
            if 'mv' in siteinfo['copytools']:
                # Check in acopytools if there is more than one copytool
                if len(siteinfo['copytools']) == 1 or 'mv' in siteinfo['acopytools'].get('pr', []):
                    truepilot = False
            siteinfo['truepilot'] = truepilot

        if len(sites) < 100:
            self.log.info(f"Parsed sites from CRIC: {sites.keys()}")
        else:
            self.log.info(f"Parsed {len(sites)} sites from CRIC")
        return sites

    def _mergeSiteDicts(self, dict1, dict2):
        for d in dict2.keys():
            if d in dict1:
                dict1[d].update(dict2[d])
            else:
                dict1[d]=dict2[d]

    def getSites(self, flavour=None):
        '''Get site info, filtered by CE flavour(s) if given'''

        self.conf = aCTConfigAPP()
        cricfile = self.conf.cric.jsonfilename
        if not cricfile:
            # No CRIC, only manually configured sites
            return self._parseConfigSites()

        # wait for CRIC json to be produced
        i = 0
        while True:
            try:
                cricmtime = os.stat(cricfile).st_mtime
                break
            except:
                i += 1
                if i > 2:
                    self.log.critical("Couldn't get CRIC json")
                    return {}
                time.sleep(10)

        # check if json file or config file changed before parsing
        if (self.tparse < cricmtime) or (self.tparse < os.stat(self.conf.path).st_mtime):
            self.log.info("CRIC file and/or config modified, reparsing site info")
            pilotmgr = self.conf.cric.pilotmanager
            pilotver = self.conf.cric.pilotversion
            start_parsing = time.time()
            self.sites = self._parseCRICJson(cricfile, pilotmgr, pilotver)
            self._mergeSiteDicts(self.sites, self._parseConfigSites())
            self.tparse = time.time()
            self.log.debug("Time to parse site info: %g s"%(self.tparse-start_parsing))

            self.log.info("Queues served:")
            for site, info in sorted(self.sites.items()):
                if not info['enabled']:
                    continue
                if not info.get('endpoints') and info.get('status') != 'offline':
                    self.log.warning(f"{site}: No CE endpoints defined, no jobs can be submitted")
                else:
                    pilotType = 'True pilot' if info['truepilot'] else 'ARC pilot'
                    self.log.info(f"{site} ({info['status']}): {pilotType} ({info['flavour']}), maxjobs {info['maxjobs']}")

        if flavour:
            return dict((k,v) for (k,v) in self.sites.items() if v.get('flavour') in flavour)
        return self.sites

if __name__ == '__main__':

    log = logging.getLogger()
    log.setLevel("DEBUG")
    out = logging.StreamHandler(sys.stdout)
    log.addHandler(out)
    cricparser = aCTCRICParser(log)
    sites = cricparser.getSites()
    sites = {s:i for s,i in sites.items() if i['enabled']}
    log.info(f'{len(sites)} sites')
