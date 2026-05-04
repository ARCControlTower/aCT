from act.arc.aCTDBArcNEW import aCTDBArc
from act.atlas.dbModels import PandaJob, PandaArchive

class aCTDBPanda(aCTDBArc):
        '''
         pandajobs:
           - id: Auto-incremented counter
           - modified: Auto-updated modification time
           - created: Creation time of panda job
           - pandaid: Panda job ID
           - pandajob: String of panda job description
           - siteName: Panda Resource
           - prodSourceLabel: Type of job (managed, test, etc)
           - arcjobid: Row ID of job in arcjobs table
           - condorjobid: Row ID of job in condorjobs table
           - pandastatus: Panda job status corresponding to state on the panda server
                 sent: job is retrieved from panda
                 starting: job is in aCT but not yet running
                 running: job is running on worker node
                 transferring: job is finished but uploading output files or in aCT post-processing
                 finished: job finished successfully
                 failed: job failed (code or grid failure)
           - actpandastatus: aCT internal state of panda jobs
                 In addition to above states:
                 tovalidate: job has finished or failed and output files should
                   be validated or cleaned
                 toresubmit: job will be resubmitted but first output files
                   should be cleaned
                 done: aCT is finished with this job, nothing more needs to be done
                 donefailed: aCT is finished, job failed
                 tobekilled: panda requests that the job is cancelled
                 cancelled: job was cancelled in ARC, still need to send final heartbeat
                 donecancelled: job was cancelled, nothing more needs to be done
           - theartbeat: Timestamp of last heartbeat (pstatus set)
           - priority: Job priority
           - node: Worker node on which the job is running
           - startTime: Job start time
           - endTime: Job end time
           - computingElement: CE where the job is running
           - proxyid: ID of proxy in proxies table to use for this job
           - sendhb: Flag to say whether or not to send heartbeat
           - corecount: Number of cores used by job
           - metadata: Generic json metadata sent by the client
           - error: Error string from a failed job

        pandaarchive:
          - Selected fields from above list:
            - pandaid, siteName, actpandastatus, startTime, endTime
        '''

if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.DEBUG)
    adb = aCTDBPanda(logging.getLogger())
    adb.createTables()
