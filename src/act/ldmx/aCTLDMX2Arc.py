import os
from collections import Counter

from act.ldmx.aCTLDMXProcess import aCTLDMXProcess

class aCTLDMX2Arc(aCTLDMXProcess):
    '''
    Pick up new jobs in the db and create ARC jobs
    '''

    def __init__(self):
        aCTLDMXProcess.__init__(self)

    def processNewJobs(self):

        # Submit new jobs
        newjobs = self.dbldmx.getJobs("ldmxstatus='new' order by modified limit 100")
        for job in newjobs:

            with open(job['description']) as f:
                config = {l.split('=')[0]: l.split('=')[1].strip() for l in f}

            xrsl = self.createXRSL(job['description'], job['template'], config)
            if not xrsl:
                self.log.warning(f'Could not create xrsl for {job["id"]}')
                # Set back to new to put at the back of the queue
                self.dbldmx.updateJobLazy(job['id'], {'ldmxstatus': 'new'})
                continue

            # Send to cluster with the data if possible
            clusterlist = self.chooseEndpoints(config)
            self.log.info(f'Inserting job {job["id"]} to CEs {clusterlist}\n with xrsl {xrsl}')
            arcid = self.dbarc.insertArcJobDescription(xrsl,
                                                       proxyid=job['proxyid'],
                                                       clusterlist=clusterlist,
                                                       downloadfiles='gmlog/errors;stdout;rucio.metadata',
                                                       appjobid=str(job['id']),
                                                       fairshare=job['batchid'][:50])

            if not arcid:
                self.log.error('Failed to insert arc job')
                self.dbldmx.updateJobLazy(job['id'], {'ldmxstatus': 'failed'})
                continue

            desc = {'ldmxstatus': 'waiting', 'arcjobid': arcid['LAST_INSERT_ID()']}
            self.dbldmx.updateJobLazy(job['id'], desc)

            # Dump job description
            logdir = os.path.join(self.conf.get(["joblog", "dir"]),
                                  job['created'].strftime('%Y-%m-%d'))
            os.makedirs(logdir, 0o755, exist_ok=True)
            xrslfile = os.path.join(logdir, f'{job["id"]}.xrsl')
            with open(xrslfile, 'w') as f:
                f.write(xrsl)
                self.log.debug(f'Wrote description to {xrslfile}')

        if newjobs:
            self.dbldmx.Commit()


    def createXRSL(self, descriptionfile, templatefile, config):

        xrsl = {}

        # Parse some requirements from descriptionfile
        xrsl['memory'] = f"(memory = {float(config.get('JobMemory', 2)) * 1000})"
        xrsl['walltime'] = f"(walltime = {int(config.get('JobWallTime', 240))})"
        xrsl['cputime'] = f"(cputime = {int(config.get('JobWallTime', 240))})"
        # LDMX RTE must be before SIMPROD one
        xrsl['runtimeenvironment'] = ''
        if 'RunTimeEnvironment' in config:
            xrsl['runtimeenvironment'] = f"(runtimeenvironment = APPS/{config.get('RunTimeEnvironment')})"
        xrsl['runtimeenvironment'] += f"(runtimeenvironment = APPS/{self.conf.get(['executable', 'simprodrte'])})"
        if config.get('FinalOutputDestination'):
            xrsl['outputfiles'] = '(outputfiles = ("rucio.metadata" "")("@output.files" ""))'
        else:
            xrsl['outputfiles'] = '(outputfiles = ("rucio.metadata" ""))'

        wrapper = self.conf.get(['executable', 'wrapper'])
        xrsl['executable'] = f"(executable = ldmxsim.sh)"

        inputfiles = f'(ldmxsim.sh {wrapper})\n \
                       (ldmxproduction.config {descriptionfile})\n \
                       (ldmxjob.py {templatefile})\n \
                       (ldmx-simprod-rte-helper.py {self.conf.get(["executable", "ruciohelper"])})\n'

        if 'InputDataset' in config:
            for inputfile, inputlocal, inputremote in zip(config['InputFile'].split(','),
                                                          config['InputDataLocationLocalRSE'].split(','),
                                                          config['InputDataLocationRemote'].split(',')):
                if inputlocal == 'None':
                    # No local copy so get ARC to download it
                    inputfiles += f'({inputfile.split(":")[1]} \"{inputremote}\" "cache=no")\n'

        if 'PileupLocation' in config:
            for pup in config.get('PileupLocation').split(','):
                if pup != 'None':
                    inputfiles += f'({pup.split("/")[-1]} \"{pup}\" "cache=copy")\n'

        xrsl['inputfiles'] = f'(inputfiles = {inputfiles})'

        xrsl['stdout'] = '(stdout = stdout)'
        xrsl['gmlog'] = '(gmlog = gmlog)'
        xrsl['join'] = '(join = yes)'
        xrsl['rerun'] = '(rerun = 2)'
        xrsl['count'] = '(count = 1)'
        xrsl['jobName'] = '(jobname = "LDMX Prod Simulation")'

        return '&' + '\n'.join(xrsl.values())

    def chooseEndpoints(self, config):

        # Now all local RSEs should be the same within a job
        localrse = config.get('InputDataLocationLocalRSE', 'None').split(',')[0]
        if localrse == 'None':
            # use all endpoints
            return ','.join(self.endpoints)

        return self.rses.get(localrse)

    def process(self):

        self.processNewJobs()


if __name__ == '__main__':

    ar = aCTLDMX2Arc()
    ar.run()
    ar.finish()
