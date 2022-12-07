import json
import os
import shutil
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

from act.ldmx.aCTLDMXProcess import aCTLDMXProcess
from rucio.common.exception import (DataIdentifierAlreadyExists,
                                    DataIdentifierNotFound, FileAlreadyExists,
                                    FileReplicaAlreadyExists, RucioException)

try:
    import selinux
except:
    selinux = None


class aCTLDMXRegister(aCTLDMXProcess):
    '''
    Post-processing for LDMX jobs. Reads metadata json file and registers output
    files in Rucio.
    '''

    def setup(self):
        super().setup()
        self.scopes = self.rucio.list_scopes()

    def processDoneJobs(self):
        '''
        Look for done jobs, set to registering, and output metadata in Rucio.
        Check registering jobs that previously failed.

        Signal handling strategy:
        - This operation involves several filesystem operations so the signals
          are deferred for the entire operation.
        '''
        # exit handling context manager
        with self.sigdefer:

            select = "ldmxstatus='finishing' and arcstate='done' and arcjobs.id=ldmxjobs.arcjobid limit 100"
            columns = ['arcjobs.id', 'JobID', 'appjobid', 'cluster', 'UsedTotalWallTime', 'fairshare',
                       'arcjobs.EndTime', 'stdout', 'ldmxjobs.created', 'description', 'template']
            arcjobs = self.dbarc.getArcJobsInfo(select, columns=columns, tables='arcjobs,ldmxjobs')
            for aj in arcjobs:
                self.log.info(f'Found finished job {aj["id"]}')
                select = f"id={int(aj['appjobid'])}"
                desc = {'ldmxstatus': 'registering',
                        'computingelement': aj['cluster'],
                        'sitename': self.endpoints[aj['cluster']],
                        'starttime': (aj['EndTime'] or datetime.now(timezone.utc)) - timedelta(0, aj['UsedTotalWallTime']),
                        'endtime': aj['EndTime'] or datetime.now(timezone.utc)}
                self.dbldmx.updateJobs(select, desc)

            self.register(arcjobs)

            select = f"ldmxstatus='registering' and " \
                     f"{self.dbldmx.timeStampLessThan('ldmxjobs.modified', 300, utc=False)} and " \
                     "arcjobs.id=ldmxjobs.arcjobid limit 100"
            arcjobs = self.dbarc.getArcJobsInfo(select, columns=columns, tables='arcjobs,ldmxjobs')
            self.register(arcjobs)


    def register(self, arcjobs):
        '''
        Do the registration and output file handling
        '''
        for aj in arcjobs:
            select = f"id={int(aj['appjobid'])}"
            jobid = aj.get('JobID')
            if not jobid:
                self.log.error(f'No JobID in arcjob {aj["id"]}')
                self.dbldmx.updateJobs(select, {'ldmxstatus': 'failed'})
                self.cleanInputFiles(aj)
                # Nothing to fetch or clean so continue
                continue

            # Read the metadata and insert into rucio
            res = self.insertMetadata(aj)
            if res is None: # retry
                self.log.info(f'Will try {aj["id"]} later')
                # Update to set modification time
                self.dbldmx.updateJobs(select, {'modified': datetime.now().isoformat()})
                continue
            elif not res: # fail
                self.dbldmx.updateJobs(select, {'ldmxstatus': 'failed'})
            else: # success
                self.dbldmx.updateJobs(select, {'ldmxstatus': 'finished'})

            # copy to joblog dir files downloaded for the job if configured
            self.copyOutputFiles(aj)

            # Clean tmp dir
            self.cleanDownloadedJob(jobid)

            # Set arc job to clean
            select = f"id={aj['id']}"
            desc = {"arcstate": "toclean", "tarcstate": self.dbarc.getTimeStamp()}
            self.dbarc.updateArcJobs(desc, select)

            # Clean input files
            self.cleanInputFiles(aj)


    def copyOutputFiles(self, arcjob):
        '''
        Copy job stdout and errors log to final location if requested
        '''
        sessionid = arcjob['JobID'][arcjob['JobID'].rfind('/')+1:]
        date = arcjob['created'].strftime('%Y-%m-%d')
        outd = os.path.join(self.conf.joblog.dir, date)
        os.makedirs(outd, 0o755, exist_ok=True)

        if self.conf.joblog.keepsuccessful and self.conf.joblog.dir:
            localdir = os.path.join(self.tmpdir, sessionid)
            gmlogerrors = os.path.join(localdir, "gmlog", "errors")
            arcjoblog = os.path.join(outd, "%s.log" % arcjob['appjobid'])
            try:
                shutil.move(gmlogerrors, arcjoblog)
                os.chmod(arcjoblog, 0o644)
                if selinux:
                    selinux.restorecon(arcjoblog) #pylint: disable=E1101
            except Exception as e:
                self.log.error(f'Failed to copy {gmlogerrors}: {e}')

            jobstdout = arcjob['stdout']
            if jobstdout:
                arcjobout = os.path.join(outd, '%s.out' % arcjob['appjobid'])
                try:
                    shutil.move(os.path.join(localdir, jobstdout), arcjobout)
                    os.chmod(arcjobout, 0o644)
                    if selinux:
                        selinux.restorecon(arcjobout) #pylint: disable=E1101
                except Exception as e:
                    self.log.error(f'Failed to copy file {os.path.join(localdir, jobstdout)}, {str(e)}')
        else:
            # delete xrsl file
            try:
                os.remove(os.path.join(outd, f"{arcjob['appjobid']}.xrsl"))
            except IOError as e:
                self.log.debug(f"Failed to remove xrsl file for {arcjob['id']}: {e}")


    def cleanInputFiles(self, job):
        '''
        Clean job input files in tmp dir
        '''
        try:
            os.remove(job['description'])
            os.remove(job['template'])
            self.log.debug(f'Removed {job["description"]} and {job["template"]}')
        except:
            pass

    def rucioAddReplica(self, rse, pfn, metadata, meta):
        '''
        Add a replica to Rucio and handle error conditions
        '''
        try:
            self.rucio.add_replica(rse, metadata['scope'], metadata['name'], metadata['bytes'],
                                   metadata['adler32'], pfn=pfn, md5=metadata['md5'], meta=meta)
        except RucioException as e:
            # Check if the scope doesn't yet exist
            # Should raise ScopeNotFound but see https://github.com/rucio/rucio/issues/3980
            if metadata['scope'] not in self.scopes:
                scope = metadata['scope']
                self.log.warning(f'Scope {scope} does not exist, adding it')
                self.rucio.add_scope('ldmx-admin', scope)
                self.scopes.append(scope)
                self.rucio.add_replica(rse, metadata['scope'], metadata['name'], metadata['bytes'],
                                       metadata['adler32'], pfn=pfn, md5=metadata['md5'], meta=meta)
            else:
                raise


    def insertMetadata(self, arcjob):
        '''
        Read metadata file and insert into Rucio
        '''
        sessionid = arcjob['JobID'][arcjob['JobID'].rfind('/')+1:]
        metadatafile = os.path.join(self.tmpdir, sessionid, 'rucio.metadata')
        try:
            with open(metadatafile) as f:
                metadata = json.load(f)
        except Exception as e:
            self.log.error(f'Failed to read metadata.json file at {metadatafile}: {e}')
            return False

        if 'local_replica' not in metadata and 'remote_output' not in metadata:
            self.log.error('No replicas to register')
            return False

        # Set some aCT metadata
        metadata['ComputingElement'] = urlparse(arcjob['cluster']).hostname or 'unknown'
        metadata['JobSubmissionTime'] = arcjob['created']
        metadata['BatchID'] = arcjob['fairshare']
        try:
            scope = metadata['scope']
            name = metadata['name']
            dscope = metadata['datasetscope']
            dname = metadata['datasetname']
            cscope = metadata['containerscope']
            cname = metadata['containername']
            nevents = int(metadata.get('NumberOfEvents', 0))
            meta = {'events': nevents}
            self.log.info(f'Inserting metadata info for {scope}:{name}: {metadata}')

            # Add replicas
            if 'local_replica' in metadata:
                pfn = f'file://{metadata["local_replica"]}'
                rse = self.sites[self.endpoints[arcjob['cluster']]]['rse']
                self.rucioAddReplica(rse, pfn, metadata, meta)
 
            if 'remote_output' in metadata:
                pfn = metadata['remote_output']['pfn']
                rse = metadata['remote_output']['rse']
                self.rucioAddReplica(rse, pfn, metadata, meta)

            try:
                # Attach to dataset
                self.rucio.attach_dids(dscope, dname, [{'scope': scope, 'name': name}])
            except DataIdentifierNotFound:
                try:
                    self.rucio.add_dataset(dscope, dname)
                except RucioException as e:
                    self.log.error(f'Dataset {dscope}:{dname} does not exist and failed to create it: {e}')
                else:
                    self.rucio.attach_dids(dscope, dname, [{'scope': scope, 'name': name}])
                    # Add new dataset to container
                    try:
                        self.rucio.attach_dids(cscope, cname, [{'scope': dscope, 'name': dname}])
                    except DataIdentifierNotFound:
                        try:
                            self.rucio.add_container(cscope, cname)
                        except RucioException as e:
                            self.log.error(f'Container {cscope}:{cname} does not exist and failed to create it: {e}')
                        else:
                            self.rucio.attach_dids(cscope, cname, [{'scope': dscope, 'name': dname}])

            # Add metadata, removing all rucio "native" metadata
            native_metadata = ['scope', 'name', 'bytes', 'md5', 'adler32',
                               'datasetscope', 'datasetname',
                               'containerscope', 'containername', 'remote_output',
                               'local_replica']
            # Metadata values must be strings to be searchable
            self.rucio.set_metadata_bulk(scope, name,
                                         {x: str(y) for x, y in metadata.items() if x not in native_metadata})
        except KeyError as e:
            self.log.info(f'key missing in metadata json: {e}')
            return False
        except (FileAlreadyExists, FileReplicaAlreadyExists, DataIdentifierAlreadyExists) as e:
            self.log.error(f'Rucio exception: {e}')
            return False
        except RucioException as e:
            # Any other Rucio exception should be retried
            self.log.warning(f'Rucio exception: {e}')
            return None

        return True


    def cleanDownloadedJob(self, arcjobid):
        '''
        Remove directory to which job was downloaded.
        '''

        sessionid = arcjobid[arcjobid.rfind('/')+1:]
        localdir = os.path.join(self.tmpdir, sessionid)
        self.log.debug(f'Removing directory {localdir}')
        shutil.rmtree(localdir, ignore_errors=True)


    def process(self):
        self.setSites()
        # Look for done jobs and process the metadata
        self.processDoneJobs()
