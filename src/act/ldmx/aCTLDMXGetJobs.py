from collections import defaultdict
from itertools import groupby
import json
import math
import os
import random
import tempfile
import time

from rucio.common.exception import RucioException, DataIdentifierNotFound

from act.ldmx.aCTLDMXProcess import aCTLDMXProcess

class aCTLDMXGetJobs(aCTLDMXProcess):
    '''
    Pick up new jobs and register them in the LDMX db
    '''

    def __init__(self):
        aCTLDMXProcess.__init__(self)

    def generateJobs(self, config):

        if 'InputDataset' in config:
            # Jobs with input files: generate one job per file
            try:
                scope, name = config['InputDataset'].split(':')
            except ValueError:
                raise Exception(f'{config["InputDataset"]} is not correctly formatted as scope:name')

            # List dataset replicas and set filename and RSE in the config
            files = list(self.rucio.list_replicas([{'scope': scope, 'name': name}]))

            # Check if pileup is also needed
            if 'PileupDataset' in config:
                self.log.info(f'Using pileup dataset {config["PileupDataset"]}')
                pileup = []
                try:
                    pscope, pname = config['PileupDataset'].split(':')
                except ValueError:
                    raise Exception(f'{config["PileupDataset"]} is not correctly formatted as scope:name')

                pileup = list(self.rucio.list_replicas([{'scope': pscope, 'name': pname}]))
                if len(pileup) < len(files):
                    self.log.info(f'Pileup dataset {config["PileupDataset"]} is smaller than input \
                                    dataset {config["InputDataset"]}, will have to reuse some events!')
                    pileup *= len(files) // len(pileup) + 1
                random.shuffle(pileup)

            # List dataset replicas and group based on local replicas
            inputfilegrouping = self.groupInputFiles(files, config)
            inputfilesperjob = int(config.get('InputFilesPerJob', 1))
            newconfig = config.copy()
            for files in inputfilegrouping.values():
                for i, f in enumerate(files, start=1):

                    for k, v in f.items():
                        newconfig[k] = f'{newconfig[k]},{v}' if k in newconfig else v

                    if i % inputfilesperjob == 0 or i == len(files):
                        newconfig['runNumber'] = math.ceil(i / inputfilesperjob)
                        # Set metadata of just the last file
                        try:
                            meta = self.rucio.get_metadata(f["InputFile"].split(':')[0],
                                                           f["InputFile"].split(':')[1],
                                                           plugin='JSON')
                        except RucioException as e:
                            raise Exception(f'Rucio exception while looking up metadata for {f["InputFile"]}: {e}')

                        newconfig['InputMetadata'] = json.dumps({ "inputMeta" : meta})

                        # Add pileup if needed
                        if 'PileupDataset' in config:
                            pfile = pileup[i-1]
                            # Always use remote copy so that it is cached
                            for rep in pfile['rses'].values():
                                if not rep[0].startswith('file://'):
                                    newconfig['PileupLocation'] = rep[0]
                            if not newconfig.get('PileupLocation'):
                                raise Exception(f'No suitable locations found for pileup file {pfile["scope"]}:{pfile["name"]}')
                            newconfig['PileupLocationLocal'] = f'./{pfile["name"]}'
                            newconfig['PileupFile'] = f'{pfile["scope"]}:{pfile["name"]}'

                        yield newconfig
                        newconfig = config.copy()

        else:
            # Jobs with no input: generate jobs based on specified number of jobs
            randomseed1 = int(config['RandomSeed1SequenceStart'])
            randomseed2 = int(config['RandomSeed2SequenceStart'])
            njobs = int(config['NumberofJobs'])
            self.log.info(f'Creating {njobs} jobs')
            for n in range(njobs):
                newconfig = config
                newconfig['RandomSeed1'] = randomseed1+n
                newconfig['RandomSeed2'] = randomseed2+n
                newconfig['runNumber']   = randomseed1+n
                yield newconfig

    def getOutputBase(self, config):

        output_rse = config.get('FinalOutputDestination')
        if not output_rse:
            return None

        # Get RSE URL with basepath. Exceptions will be caught by the caller
        rse_info = self.rucio.get_protocols(output_rse)
        if not rse_info or len(rse_info) < 1:
            raise f"Empty info returned by Rucio for RSE {output_rse}"

        return '{scheme}://{hostname}:{port}{prefix}'.format(**rse_info[0])

    def groupInputFiles(self, files, config):
        inputfileinfo = []
        for f in files:
            if not f:
                raise Exception(f'No such dataset {config["InputDataset"]}')
            fname = f'{f["scope"]}:{f["name"]}'
            # Info for each input file - some will be filled later
            inputfileconfig = {'InputFile': fname,
                               'InputDataLocationLocal': None,
                               'InputDataLocationLocalRSE': None,
                               'InputDataLocationRemote': None,
                               'InputDataLocationRemoteRSE': None,
                               'InputMetadata': None,
                               'PileupLocation': None,
                               'PileupLocationLocal': None}

            for rse, rep in f['rses'].items():
                if rse in self.rses:
                    self.log.debug(f'Found local replica of {fname} in {rse}')
                    inputfileconfig['InputDataLocationLocal'] = rep[0].replace('file://', '')
                    inputfileconfig['InputDataLocationLocalRSE'] = rse
                if not rep[0].startswith('file://'):
                    self.log.debug(f'Found remote replica of {fname} in {rse}')
                    inputfileconfig['InputDataLocationRemote'] = rep[0]
                    inputfileconfig['InputDataLocationRemoteRSE'] = rse

            # Check at least one replica is accessible
            if not inputfileconfig['InputDataLocationLocalRSE'] and not inputfileconfig['InputDataLocationRemoteRSE']:
                raise Exception(f'No usable locations for file {fname} in dataset {config["InputDataset"]}')

            if 'InputDataLocationLocal' not in inputfileconfig:
                # File will be downloaded by ARC and placed in session dir
                inputfileconfig['InputDataLocationLocal'] = f'./{f["name"]}'

            inputfileinfo.append(inputfileconfig)

        # Group by local RSE or "None"
        filegrouping = defaultdict(list)
        keyfunc = lambda x: str(x['InputDataLocationLocalRSE'])
        for k, g in groupby(sorted(inputfileinfo, key=keyfunc), keyfunc):
            filegrouping[k] = list(g)

        return filegrouping

    def getNewJobs(self):
        '''
        Read new job files in buffer dir and create necessary job descriptions
        '''

        bufferdir = self.conf.get(['jobs', 'bufferdir'])
        configsdir = os.path.join(bufferdir, 'configs')
        os.makedirs(configsdir, 0o755, exist_ok=True)
        jobs = [os.path.join(configsdir, j) for j in os.listdir(configsdir) if os.path.isfile(os.path.join(configsdir, j))]
        now = time.time()
        try:
            # Take the first proxy available
            proxyid = self.dbarc.getProxiesInfo('TRUE', ['id'], expect_one=True)['id']
        except Exception:
            self.log.error('No proxies found in DB')
            return

        for jobfile in jobs:

            # Avoid partially written files by delaying the read
            if now - os.path.getmtime(jobfile) < 5:
                self.log.debug(f'Job {jobfile} is too new')
                continue

            self.log.info(f'Picked up job at {jobfile}')

            with open(jobfile) as f:
                try:
                    config = {l.split('=')[0]: l.split('=')[1].strip() for l in f if '=' in l}
                    batchid = config.get('BatchID', f'Batch-{time.strftime("%Y-%m-%dT%H:%M:%S")}')
                except Exception as e:
                    self.log.error(f'Failed to parse job config file {jobfile}: {e}')
                    os.remove(jobfile)
                    continue

            try:
                templatefile = os.path.join(bufferdir, 'templates', config['JobTemplate'])
                with open(templatefile) as tf:
                    template = tf.readlines()
            except Exception as e:
                self.log.error(f'Bad template file or template not defined in {jobfile}: {e}')
                os.remove(jobfile)
                continue

            try:
                # Get base path for output storage if necessary
                output_base = self.getOutputBase(config)

                # Generate copies of config and template
                jobfiles = []
                for jobconfig in self.generateJobs(config):
                    newjobfile = os.path.join(self.tmpdir, os.path.basename(jobfile))
                    with tempfile.NamedTemporaryFile(mode='w', prefix=f'{newjobfile}.', delete=False, encoding='utf-8') as njf:
                        newjobfile = njf.name
                        njf.write('\n'.join(f'{k}={v}' for k,v in jobconfig.items()) + '\n')
                        if output_base:
                            njf.write(f'FinalOutputBasePath={output_base}\n')
 
                        nouploadsites = self.arcconf.getListCond(["sites", "site"], "noupload=1", ["endpoint"])
                        if nouploadsites:
                            self.log.debug(nouploadsites)
                            njf.write(f'NoUploadSites={",".join(nouploadsites)}\n')

                    newtemplatefile = os.path.join(self.tmpdir, os.path.basename(templatefile))
                    with tempfile.NamedTemporaryFile(mode='w', prefix=f'{newtemplatefile}.', delete=False, encoding='utf-8') as ntf:
                        newtemplatefile = ntf.name
                        for l in template:
                            if l.startswith('sim.runNumber'):
                                ntf.write(f'sim.runNumber = {jobconfig["runNumber"]}\n')
                            elif l.startswith('p.run = RUNNUMBER'):
                                ntf.write(f'p.run = {jobconfig["runNumber"]}\n')
                            elif l.startswith('p.inputFiles'):
                                ntf.write(f'p.inputFiles = [ "{jobconfig["InputFile"].split(":")[1]}" ]\n')
                            elif l.startswith('p.maxEvents') and 'NumberOfEvents' in jobconfig :
                                ntf.write(f'p.maxEvents = {jobconfig["NumberOfEvents"]}\n')                           
                            elif l.startswith('lheLib=INPUTFILE'):
                                ntf.write(f'lheLib="{jobconfig["InputFile"].split(":")[1]}"\n')
                            elif l.startswith('pileupFileName'):
                                ntf.write(f'pileupFileName = "{jobconfig["PileupFile"].split(":")[1]}" \n')
                            elif l.startswith('sim.randomSeeds'):
                                ntf.write(f'sim.randomSeeds = [ {jobconfig.get("RandomSeed1", 0)}, {jobconfig.get("RandomSeed2", 0)} ]\n')
                            else:
                                ntf.write(l)
                    jobfiles.append((newjobfile, newtemplatefile))

                for (newjobfile, newtemplatefile) in jobfiles:
                    self.dbldmx.insertJob(newjobfile, newtemplatefile, proxyid, batchid=batchid)
                    self.log.info(f'Inserted job from {newjobfile} into DB')

            except Exception as e:
                self.log.error(f'Failed to create jobs from {jobfile}: {e}')
            os.remove(jobfile)


    def archiveBatches(self):
        '''Move completed batches to the archive table'''

        # Find out batch statuses
        batches = self.dbldmx.getGroupedJobs('batchid, ldmxstatus')
        batchdict = defaultdict(lambda: defaultdict(str))
        for batch in batches:
            batchdict[batch['batchid']][batch['ldmxstatus']] = batch['count(*)']

        for batchid, statuses in batchdict.items():
            if [s for s in statuses if s not in ['finished', 'failed', 'cancelled']]:
                continue

            # All jobs are finished, so archive
            select = f"batchid='{batchid}'" if batchid else 'batchid is NULL'
            columns = ['id', 'sitename', 'ldmxstatus', 'starttime', 'endtime', 'batchid']
            jobs = self.dbldmx.getJobs(select, columns)
            if not jobs:
                continue

            self.log.info(f'Archiving {len(jobs)} jobs for batch {batchid}')
            for job in jobs:
                self.log.debug(f'Archiving LDMX job {job["id"]}')
                self.dbldmx.insertJobArchiveLazy(job)
                self.dbldmx.deleteJob(job['id']) # commit is called here


    def process(self):

        self.getNewJobs()

        # Move old jobs to archive - every hour
        if time.time() - self.starttime > 3600:
            self.log.info("Checking for jobs to archive")
            self.archiveBatches()
            self.starttime = time.time()


if __name__ == '__main__':

    ar = aCTLDMXGetJobs()
    ar.run()
    ar.finish()
