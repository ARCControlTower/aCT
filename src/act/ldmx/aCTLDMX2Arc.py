import os

from act.ldmx.aCTLDMXProcess import aCTLDMXProcess


class aCTLDMX2Arc(aCTLDMXProcess):
    '''
    Pick up new jobs in the db and create ARC jobs
    '''

    def processNewJobs(self):
        """
        Submit new jobs.

        Signal handling strategy:
        - exit is checked before every job update
        """
        # Submit new jobs
        select = "ldmxstatus='new' and ldmxjobs.batchid = ldmxbatches.id order by ldmxjobs.modified limit 100"
        columns = ['ldmxjobs.id', 'ldmxjobs.created', 'ldmxjobs.description',
                'ldmxjobs.template', 'proxyid', 'batchname']
        newjobs = self.dbldmx.getJobs(select, columns=columns, tables='ldmxjobs, ldmxbatches')
        for job in newjobs:

            if self.mustExit:
                self.log.info(f"Exiting early due to requested shutdown")
                self.stopWithException()

            with open(job['description']) as f:
                config = {l.split('=')[0]: l.split('=')[1].strip() for l in f}

            xrsl = self.createXRSL(job['description'], job['template'], config)
            if not xrsl:
                self.log.warning(f'Could not create xrsl for {job["id"]}')
                # Set back to new to put at the back of the queue
                self.dbldmx.updateJob(job['id'], {'ldmxstatus': 'new'})
                continue

            # Send to cluster with the data if possible
            clusterlist = self.chooseEndpoints(config)
            self.log.info(f'Inserting job {job["id"]} to CEs {clusterlist}\n with xrsl {xrsl}')
            arcid = self.dbarc.insertArcJobDescription(xrsl,
                                                       proxyid=job['proxyid'],
                                                       clusterlist=clusterlist,
                                                       downloadfiles='diagnose=gmlog/errors;stdout;rucio.metadata',
                                                       appjobid=str(job['id']),
                                                       fairshare=job['batchname'])

            if not arcid:
                self.log.error('Failed to insert arc job')
                self.dbldmx.updateJob(job['id'], {'ldmxstatus': 'failed'})
                continue

            desc = {'ldmxstatus': 'waiting', 'arcjobid': arcid['LAST_INSERT_ID()']}
            self.dbldmx.updateJob(job['id'], desc)

            # Dump job description
            logdir = os.path.join(self.conf.joblog.dir,
                                    job['created'].strftime('%Y-%m-%d'))
            os.makedirs(logdir, 0o755, exist_ok=True)
            xrslfile = os.path.join(logdir, f'{job["id"]}.xrsl')
            with open(xrslfile, 'w') as f:
                f.write(xrsl)
                self.log.debug(f'Wrote description to {xrslfile}')

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
        xrsl['runtimeenvironment'] += f"(runtimeenvironment = APPS/{self.conf.executable.simprodrte})"
        if config.get('FinalOutputDestination'):
            xrsl['outputfiles'] = '(outputfiles = ("rucio.metadata" "")("@output.files" ""))'
        else:
            xrsl['outputfiles'] = '(outputfiles = ("rucio.metadata" ""))'

        wrapper = self.conf.executable.wrapper
        if 'SimExecutable' in config :
            wpath, wname = os.path.split(wrapper)
            wrapper = os.path.join(wpath, config.get('SimExecutable'))

        #keep executable naming as is but point to actual wrapper script
        xrsl['executable'] = f"(executable = ldmxsim.sh)"
        inputfiles = f'(ldmxsim.sh {wrapper})\n \
                       (ldmxproduction.config {descriptionfile})\n \
                       (ldmxjob.py {templatefile})\n \
                       (ldmx-simprod-rte-helper.py {self.conf.executable.ruciohelper})\n'

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

        if 'ImageLocation' in config:
            img= config.get('ImageLocation')#.split(',')
            if img != 'None':
                inputfiles += f'({img.split("/")[-1]} \"{img}\" "cache=copy")\n'

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
        self.setSites()
        self.processNewJobs()
