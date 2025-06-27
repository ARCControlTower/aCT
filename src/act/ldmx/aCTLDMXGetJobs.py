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

        #pull the user id to use for all subsequent user based scopes (images, analysis job output)
        config['UserID'] = "user."+os.environ['USER'] if os.environ['USER']!='almalinux' else "prod"
        self.log.info(f'Setting UserID {config["UserID"]}')

        #first, set up to use a specific image. modify config --> copied to all later newconfig
        if 'LdmxImage' in config:
            # Jobs will use specified image rather than one based on RTE
            if ";" not in config['LdmxImage'] : #no scope defined. assume user's scope
                name=config['LdmxImage']
                if ".sif" not in name :
                    name=name+".sif"
                scope=config['UserID']+".image"
            else :
                try:
                    scope, name = config['LdmxImage'].split(':')
                except ValueError: #normally we should never end up here 
                    raise Exception(f'{config["LdmxImage"]} is not correctly formatted as scope:name')
            #now enforce this naming scheme for metadata preservation, and error message
            config["LdmxImage"]=scope+':'+name 
            self.log.info(f'Setting LdmxImage={config["LdmxImage"]}')
            # List dataset replica (should be just one) and set filename in the config
            try:
                imageList = list(self.rucio.list_replicas([{'scope': scope, 'name': name}]))
            except RucioException as e:
                raise Exception(f'Rucio exception while looking up image {config["LdmxImage"]}: {e}')
            image=imageList[0]
            # Always use remote copy so that it is cached                                                                           
            for rep in image['rses'].values():
                if not rep[0].startswith('file://'):
                    self.log.debug(f'Found remote replica of {image} in {rep}')
                    config['ImageLocation'] = rep[0]
            if not config.get('ImageLocation'):
                raise Exception(f'No suitable locations found for image file {image["scope"]}:{image["name"]}')
            config['ImageLocationLocal'] = f'{image["name"]}'  #try removing ./
            config['ImageFile'] = f'{image["scope"]}:{image["name"]}'


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
            runnumber = 1
            for files in inputfilegrouping.values():
                for i, f in enumerate(files, start=1):

                    for k, v in f.items():
                        newconfig[k] = f'{newconfig[k]},{v}' if k in newconfig else v

                    if i % inputfilesperjob == 0 or i == len(files):
                        newconfig['runNumber'] = runnumber
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
                        runnumber += 1

        elif 'IsImage' in config and config['IsImage']=="Yes":
            self.log.info(f'Preparing image generating job with FileName={config["FileName"]}')
            #first: check if an image with this name already exists
            if ";" not in config['FileName'] : #no scope defined (normal). assume user's scope
                name=config['FileName']
                if ".sif" not in name :
                    name=name+".sif"
                scope=config['UserID']+".image"
            else :
                try:
                    scope, name = config['FileName'].split(':')
                except ValueError: #normally we should never end up here
                    raise Exception(f'{config["FileName"]} is not correctly formatted as scope:name; unable to verify that it does not already exist in rucio')

            try :
                image = list(self.rucio.list_replicas([{'scope': scope, 'name': name}]))
            except RucioException as e:
                raise Exception(f'Rucio exception while looking up image {f["FileName"]}: {e}')
            if len(image) > 0 : 
                raise Exception(f'A file named  {f["FileName"]} has already been defined in rucio. Choose a different name.')
            #else. all is good!
            newconfig = config.copy()
            yield newconfig


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
        Check new batches and create necessary job descriptions
        '''

        now = time.time()
        try:
            # Take the first proxy available
            proxyid = self.dbarc.getProxiesInfo('TRUE', ['id'], expect_one=True)['id']
        except Exception:
            self.log.error('No proxies found in DB')
            return

        batches = self.dbldmx.getBatches("status='new'")

        for batch in batches:

            self.log.info(f"New batch {batch['batchname']}")
            configfile = batch['description']
            templatefile = batch['template']
            user = self.dbldmx.getUser(batch['userid'])
            if not user:
                self.log.error(f"No such user with userid {batch['userid']}")
                self.dbldmx.updateBatch(batch['id'], {'status': 'failed'})
                continue

            try:
                with open(configfile) as f:
                    config = {l.split('=')[0]: l.split('=')[1].strip() for l in f if '=' in l}
            except Exception as e:
                self.log.error(f'Failed to parse job config file {configfile}: {e}')
                self.dbldmx.updateBatch(batch['id'], {'status': 'failed'})
                continue

            try:
                with open(templatefile) as tf:
                    template = tf.readlines()
            except Exception as e:
                self.log.error(f'Bad template file or template not defined in {configfile}: {e}')
                self.dbldmx.updateBatch(batch['id'], {'status': 'failed'})
                continue

            try:
                # Get base path for output storage if necessary
                output_base = self.getOutputBase(config)

                # Generate copies of config and template
                jobfiles = []
                for jobconfig in self.generateJobs(config):
                    newjobfile = os.path.join(self.tmpdir, os.path.basename(configfile))
                    with tempfile.NamedTemporaryFile(mode='w', prefix=f'{newjobfile}.', delete=False, encoding='utf-8') as njf:
                        newjobfile = njf.name
                        njf.write('\n'.join(f'{k}={v}' for k,v in jobconfig.items()) + '\n')
                        if output_base:
                            njf.write(f'FinalOutputBasePath={output_base}\n')

                        nouploadsites = [site.endpoint for _, site in self.arcconf.sites if site.noupload == 1]
                        if nouploadsites:
                            self.log.debug(nouploadsites)
                            njf.write(f'NoUploadSites={",".join(nouploadsites)}\n')

                        if 'Scope' not in jobconfig:
                            njf.write(f'Scope=user.{user["ruciouser"]}\n')

                        if 'BatchID' not in jobconfig:
                            njf.write(f'BatchID={batch["batchname"]}\n')

                    newtemplatefile = os.path.join(self.tmpdir, os.path.basename(templatefile))
                    with tempfile.NamedTemporaryFile(mode='w', prefix=f'{newtemplatefile}.', delete=False, encoding='utf-8') as ntf:
                        newtemplatefile = ntf.name
                        # might need a long list of input files, but without the scope. 
                        if "InputFile" in jobconfig :
                            inFileScope = jobconfig["InputFile"].split(":")[0] # use first input file to look up the scope
                            inFileList = (jobconfig["InputFile"].replace(inFileScope+":", "")).split(",")
                        for l in template:
                            if l.startswith('sim.runNumber'):
                                ntf.write(f'sim.runNumber = {jobconfig["runNumber"]}\n')
                            elif l.startswith('p.run = RUNNUMBER'):
                                ntf.write(f'p.run = {jobconfig["runNumber"]}\n')
                            elif l.startswith('p.inputFiles'):
                                ntf.write(f'p.inputFiles = {inFileList} \n') #jobconfig["InputFile"].split(":")[1]}" ]\n')
                            elif l.startswith('p.maxEvents') and 'NumberOfEvents' in jobconfig :
                                ntf.write(f'p.maxEvents = {jobconfig["NumberOfEvents"]}\n')                           
                            elif l.startswith('lheLib=INPUTFILE'):
                                ntf.write(f'lheLib="{jobconfig["InputFile"].split(":")[1]}"\n')
                                #ntf.write(f'lheLib={inFileList}\n') # {jobconfig["InputFile"].split(":")[1]}"\n')
                            elif l.startswith('pileupFileName'):
                                ntf.write(f'pileupFileName = "{jobconfig["PileupFile"].split(":")[1]}" \n')
                            elif l.startswith('sim.randomSeeds'):
                                ntf.write(f'sim.randomSeeds = [ {jobconfig.get("RandomSeed1", 0)}, {jobconfig.get("RandomSeed2", 0)} ]\n')
                            #ldmx-sw v1-style mac template stuff
                            elif l.startswith('/random/setSeeds'):
                                ntf.write(f'/random/setSeeds {jobconfig.get("RandomSeed1", 0)} {jobconfig.get("RandomSeed2", 0)}\n')
                            elif l.startswith('/ldmx/persistency/root/runNumber'):
                                ntf.write(f'/ldmx/persistency/root/runNumber {jobconfig["runNumber"]}\n')
                            elif  l.startswith('detector =') or l.startswith('detector='):
                                ntf.write(f'detector = "ldmx-det-v{jobconfig["DetectorVersion"]}"\n')
                            elif  l.startswith('sim.setDetector(') :
                                ntf.write(f'sim.setDetector("ldmx-det-v{jobconfig["DetectorVersion"]}", False )\n')
                            elif  l.startswith('sim.scoringPlanes') :  #overwrite use SP false-->true by reconfiguring
                                ntf.write(f'sim.scoringPlanes = makeScoringPlanesPath("ldmx-det-v{jobconfig["DetectorVersion"]}")\n')
                                ntf.write(f'sim.setDetector("ldmx-det-v{jobconfig["DetectorVersion"]}", True )\n')
                            elif  l.startswith('nElectrons =') or l.startswith('nElectrons='):
                                ntf.write(f'nElectrons = {jobconfig["ElectronNumber"]}\n')              
                            elif  l.startswith('beamEnergy =') or l.startswith('beamEnergy='):
                                ntf.write(f'beamEnergy = {jobconfig["BeamEnergy"]}\n')              
                            else:
                                ntf.write(l)
                    jobfiles.append((newjobfile, newtemplatefile))

                for (newjobfile, newtemplatefile) in jobfiles:
                    self.dbldmx.insertJob(newjobfile, newtemplatefile, proxyid, batch['userid'], batchid=batch['id'])
                    self.log.info(f'Inserted job from {newjobfile} into DB')

                self.dbldmx.updateBatch(batch['id'], {'status': 'inprogress'})
            except Exception as e:
                self.log.error(f'Failed to create jobs from {configfile}: {e}')
                self.dbldmx.updateBatch(batch['id'], {'status': 'failed'})


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
            select += ' AND ldmxjobs.batchid = ldmxbatches.id'
            columns = ['ldmxjobs.id', 'sitename', 'ldmxstatus', 'starttime', 'endtime', 'batchname']
            jobs = self.dbldmx.getJobs(select, columns, tables='ldmxjobs, ldmxbatches')
            if not jobs:
                continue

            self.log.info(f'Archiving {len(jobs)} jobs for batch {batchid}')
            for job in jobs:
                self.log.debug(f'Archiving LDMX job {job["id"]}')
                # For backwards compatbility with archive table
                job['batchid'] = job['batchname']
                del job['batchname']
                self.dbldmx.insertJobArchiveLazy(job)
                self.dbldmx.deleteJob(job['id']) # commit is called here

            # Set final batch state
            finalbatchstate = 'finished'
            if 'failed' in statuses:
                finalbatchstate = 'failed'
            elif 'cancelled' in statuses:
                finalbatchstate = 'cancelled'
            self.dbldmx.updateBatch(batchid, {'status': finalbatchstate})


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
