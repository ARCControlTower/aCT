# aCT processes

aCT is made up of several modules, each implementing a particular functionality:
- *arc*: job orchestration on ARC clusters
- *condor*: job orchestration on Condor clusters
- *client*: user interfaces (REST) for job management in aCT
- *atlas*: orchestrates execution of jobs from Panda on clusters by
  cooperating with *arc* and *condor* modules
- *ldmx*: orchestrates the execution of jobs for LDMX on clusters by
  cooperating with *arc* module

aCT operates in the form of multiple processes that cooperate to provide the
required functionality. Each module can define several processes that implement
its functionality. aCT starts as one main process that creates and supervises
the processes defined by the modules.

The termination mechanism used for the service as a whole as well as individual
processes are OS signals. That requires proper signal handling and exit
strategies. It has to be assumed that a signal can come at any time, completely
asynchronously, when devising the signal handling and exit strategies.

Two mechanisms are implemented for use to implement proper handling and exit.
First is *aCTTransaction* class. Instances of this class provide basic database
transaction wrapper which is required to maintain consistency between the
database objects and prevents signals from interrupting the transaction commit
and causing a broken state. The database operations performed in the context of
the transaction should be lazy as the commit is handled by the transaction
context (there are some exceptions, mostly due to the database objects lacking
lazy operations; preferably, the lazy operations could be added through optional
flags; even better, the model of database interaction could be improved, e. g.
handle multiple tables in the same session instead of partitioning them
independently by table).

Unfortunately, not all required operations are transactional. The examples of
those are file system operations, interaction with ARC, Condor, Panda, etc.
Those require certain sections of code that perform state updates in multiple
non-transactional systems to handle the signal interruptions properly to
maintain the correct state across the systems. This is the role of the second
implemented mechanism in the *aCTSignalDeferrer* class. The idea is to structure
the operations in a way where "consistency-critical" updates are grouped
together and executed atomically.  Note that not all parts are currently
optimized or structured for the most efficient atomic updates.

One possibility for improving transactionality would be to extend the deferring
mechanism used currently for *aCTSignalDeferrer* to file system and other
non-transactional operations e. g. instead of calling file system operation
for deleting the file, the transaction could provide a method that would
schedule the operation for later commit.

More detail about the implementation of aCT processes is provided in the
`src/act/common/aCTProcess.py` and the inheriting classes. The implementation of
*aCTTransaction* is also there. The implementation of the process management is
described in `src/act/common/aCTProcessManager.py`. The implementation of
signal handling is in `src/act/common/aCTSignal.py`.
