import datetime
from json import JSONDecodeError
from collections import defaultdict

from act.arc.aCTARCProcess import aCTARCProcess
from act.arc.dbModels import ArcJob, JobDescription
from pyarcrest.errors import ARCHTTPError
from sqlalchemy import select, delete


class aCTCleaner(aCTARCProcess):

    # TODO: refactor to some library aCT job operation
    def processToClean(self):
        """
        Clean designated jobs from ARC cluster and DB.

        Signal handling strategy:
        - method checks termination before job batch for every proxyid
        """
        # delete jobs that are taking too long
        with self.db.Session.begin() as session:
            tstamp = self.db.getTimeStamp()
            limit = tstamp - datetime.timedelta(hours=1)
            jobstoclean = session.execute(select(ArcJob.id, ArcJob.appjobid, ArcJob.jobdesc) \
                                             .where(ArcJob.arcstate=='toclean', ArcJob.cluster==self.cluster, ArcJob.tarcstate<limit)).all()
            if jobstoclean:
                session.execute(delete(ArcJob).where(ArcJob.id.in_([job.id for job in jobstoclean])))
                session.execute(delete(JobDescription).where(JobDescription.id.in_([job.jobdesc for job in jobstoclean])))
                for job in jobstoclean:
                    self.log.warning(f"Could not clean appjob({job.appjobid}) in time, removing from DB")

        # clean remaining jobs
        with self.db.Session() as session:
            toclean = session.execute(select(ArcJob.id, ArcJob.appjobid, ArcJob.proxyid, ArcJob.IDFromEndpoint, ArcJob.jobdesc) \
                                      .where(ArcJob.arcstate=='toclean', ArcJob.cluster==self.cluster).limit(100)).all()

        if not toclean:
            self.log.info(f"Nothing to clean")
            return
        self.log.info(f"Cleaning {len(toclean)} jobs")
        
        # aggregate jobs by proxyid
        jobsdict = defaultdict(list)
        for job in toclean:
            jobsdict[job.proxyid].append(job)

        for proxyid, dbjobs in jobsdict.items():
            self.stopOnFlag()

            # get parameters for ARC
            arcjobs = []
            arcids = []
            for job in dbjobs:
                if job.IDFromEndpoint:
                    arcjobs.append(job)
                    arcids.append(job.IDFromEndpoint)

            # get REST client
            arcrest = self.getARCClient(proxyid)
            if not arcrest:
                continue

            # clean jobs in ARC
            try:
                results = arcrest.cleanJobs(arcids)
            except JSONDecodeError as exc:
                self.log.error(f"Invalid JSON response from ARC: {exc}")
                continue
            except Exception as exc:
                self.log.error(f"Error cleaning jobs in ARC: {exc}")
                continue
            finally:
                arcrest.close()

            # log results
            for job, result in zip(arcjobs, results):
                if result.error:
                    error = result.value
                    if isinstance(error, ARCHTTPError):
                        self.log.error(f"Error cleaning appjob({job.appjobid}) from ARC: {error.status} {error.text}")
                else:
                    self.log.info(f"Successfully cleaned appjob({job.appjobid}) from ARC")

            # update DB
            with self.db.Session.begin() as session:
                session.execute(delete(ArcJob).where(ArcJob.id.in_([job.id for job in dbjobs])))
                session.execute(delete(JobDescription).where(JobDescription.id.in_([job.jobdesc for job in dbjobs])))
                for job in dbjobs:
                    self.log.info(f"Successfully cleaned appjob({job.appjobid}) in arc DB")

        self.log.info("Done")

    def process(self):
        self.processToClean()
