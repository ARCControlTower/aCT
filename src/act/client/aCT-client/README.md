# NOTICE:
Code suitable for testing currently resides on branch `test`. All links provided
here use that branch.

# Install

## System dependencies
Nordugrid ARC Client has to be installed to use `arcproxy` for proxy
certificates.
See [instructions](http://www.nordugrid.org/arc/arc6/users/client_install.html).

## Notes on install locations
aCT client is currently meant to be installed using Python's `pip` package manager.
By default, `pip` tries to install packages to system directory which is not advised
to do. Users should either install it using `pip install --user` or in a virtual
environment.

## Installation to virtual environment
Virtual environmet is a directory structure that includes all programs and libraries
needed for a standalone Python interpreter that is completely separate from the one
of the system or other virtual environments.

To create a virtual environment, the user needs to choose the location of the
environment and then run:  
`$ python3 -m venv /path/to/act-venv`

Once virtual environment is created, it has to be activated to be used.  
`$ source /path/to/act-venv/bin/activate`  
All commands require shell with an active virtual environment.

To install aCT client, run this command:  
`(act-venv) $ pip install git+https://github.com/ARCControlTower/aCT.git@test#subdirectory=src/act/client/aCT-client`  
The command installs aCT client from git repository as it is not distributed in
a package repository like PyPI. To run aCT client commands the virtual envionmnet
needs to be activated. Your shell might indicate that the environment is active
by prepending prefix to its prompt:  
`(act-venv) $ `  

## Upgrading to newest version
`(act-venv) $ pip uninstall aCT-client`  
`(act-venv) $ pip install git+https://github.com/ARCControlTower/aCT.git@test#subdirectory=src/act/client/aCT-client`  

# Configuration
Default location for aCT configuration file is `$HOME/.config/act-client/config.yaml`.
Configuration file can also be passed to commands, e. g.:  
`(act-venv) $ act --conf /path/to/your/conf <subcommand> <args>`  

Configuration is in YAML format. Parameters with default values and optional parameters
can be omitted. Possible parameters are:
- `server`: URL of aCT server (should include port if non standard)
- `clusters`: a YAML *mapping* of names to lists of clusters. A list of enabled
  clusters for a server can be obtained with `act info`. If no `--clusterlist`
  flag is given, the list called `default` will be used.
- `token`: location where client should store auth token
  (optional, default: `$HOME/.local/share/act-client/token`)
- `proxy`: location of proxy that client uses for authentication
  (optional, default: `/tmp/x509up_u$UID` - default location used by ARC client)
- `webdav`: path to WebDAV folder accessible with your proxy certificate credentials
  (optional, but required for use with empty `--webdav` flag)

Example configuration:
``` yaml
server: https://act.vega.izum.si
webdav: https://dcache.sling.si:2880/gen.vo.sling.si/john/jobdata
clusters:
  default:
    - https://arc01.vega.izum.si/cpu
    - https://arc02.vega.izum.si/cpu
    - https://hpc.arnes.si/all
    - https://nsc.ijs.si/gridlong
  longcpu:
    - https://arc01.vega.izum.si/longcpu
    - https://hpc.arnes.si/long
    - https://nsc.ijs.si/gridlong
  gpu:
    - https://arc01.vega.izum.si/gpu
    - https://hpc.arnes.si/gpu
  largememory:
    - https://arc01.vega.izum.si/largemem
    - https://hpc.arnes.si/all
    - https://nsc.ijs.si/gridlong
```

# Example usage

First, obtain proxy certificate with VOMS credentials needed to access the
clusters:
``` sh
$ arcproxy -S gen.vo.sling.si
Enter pass phrase for private key:
Your identity: /C=SI/O=SiGNET/O=IJS/OU=F9/CN=John Doe
Contacting VOMS server (named gen.vo.sling.si): voms.sling.si on port: 15001
Proxy generation succeeded
Your proxy is valid until: 2022-03-08 23:44:07
```

Authenticate on the aCT server with the generated proxy:
``` sh
(act-client) $ act proxy
Successfully inserted proxy. Access token stored in /home/john/.local/share/act-client/token
```

Submit jobs:
``` sh
(act-client) $ act sub arctest1.xrsl arctest2.xrsl arctest3.xrsl arctest4.xrsl --webdav
Inserted job arctest1 with ID 28517
Inserted job arctest2 with ID 28518
Inserted job arctest3 with ID 28519
Inserted job arctest4 with ID 28520
```

Check status of jobs:
``` sh
id    jobname  JobID                                                                                  State     arcstate
-------------------------------------------------------------------------------------------------------------------------
28517 arctest1 https://pikolit.ijs.si:443/arex/lqYKDmcZml0n4J8tmqCBXHLnABFKDmABFKDmFoFKDmmk7KDmuhWSun Undefined submitted
28518 arctest2 https://pikolit.ijs.si:443/arex/geMODmcZml0n4J8tmqCBXHLnABFKDmABFKDmFoFKDmnk7KDmD1SOsn Undefined submitted
28519 arctest3 https://pikolit.ijs.si:443/arex/wHqLDmdZml0n4J8tmqCBXHLnABFKDmABFKDmFoFKDmok7KDml5d6kn Undefined submitted
28520 arctest4 https://pikolit.ijs.si:443/arex/Mm0KDmeZml0n4J8tmqCBXHLnABFKDmABFKDmFoFKDmpk7KDmCmAxyn Undefined submitted
```

Wait for jobs to finish:
``` sh
id    jobname  JobID                                                                                  State    arcstate
-----------------------------------------------------------------------------------------------------------------------
28517 arctest1 https://pikolit.ijs.si:443/arex/lqYKDmcZml0n4J8tmqCBXHLnABFKDmABFKDmFoFKDmmk7KDmuhWSun Finished done
28518 arctest2 https://pikolit.ijs.si:443/arex/geMODmcZml0n4J8tmqCBXHLnABFKDmABFKDmFoFKDmnk7KDmD1SOsn Finished done
28519 arctest3 https://pikolit.ijs.si:443/arex/wHqLDmdZml0n4J8tmqCBXHLnABFKDmABFKDmFoFKDmok7KDml5d6kn Finished done
28520 arctest4 https://pikolit.ijs.si:443/arex/Mm0KDmeZml0n4J8tmqCBXHLnABFKDmABFKDmFoFKDmpk7KDmCmAxyn Finished done
```

Download job results:
``` sh
Results for job arctest1 stored in lqYKDmcZml0n4J8tmqCBXHLnABFKDmABFKDmFoFKDmmk7KDmuhWSun
Results for job arctest2 stored in geMODmcZml0n4J8tmqCBXHLnABFKDmABFKDmFoFKDmnk7KDmD1SOsn
Results for job arctest3 stored in wHqLDmdZml0n4J8tmqCBXHLnABFKDmABFKDmFoFKDmok7KDml5d6kn
Results for job arctest4 stored in Mm0KDmeZml0n4J8tmqCBXHLnABFKDmABFKDmFoFKDmpk7KDmCmAxyn
Cleaning WebDAV directories ...
```

For more details about job management with aCT client read on.

# Overview of aCT client
aCT client is a program that allows submission and management of jobs similar to
ARC Client tools with added benefits of aCT job management like job brokering to
multiple clusters.

## Job states
Every job once submitted to aCT is in a particular aCT state. Users should have a
rough understanding of job states to know which operations can or cannot be
performed on jobs. Job states should be rather intuitive to users that are
familiar with job workflows. When aCT submits jobs to ARC middleware, it also
tracks its state in the middleware. aCT client by default provides information
on both states which should give the user a good indication about what is going
on with their jobs.

**WARNING**: the way jobs transition between different states is not always
intuitive. This is especially the case when killing jobs. The intuitive state
flow would for instance be: `running/submitted -> cancelling -> cancelled`.
But it does not always go this way. Sometimes the jobs will go back to
`submitted` state after being in `cancelling` for a while and appear as if
they were resubmitted. In such cases it is best to wait for jobs to reach one
of the "terminal" states like `cancelled` or `failed` or even `done` if the kill
operation did not propagate fast enough. Once the jobs are in such state they
can be managed accordingly.

Most common states:
- `running` - the job is running in ARC middleware
- `tosubmit`, `submitting`, `submitted` - jobs are in the process of submission
  or have just been submitted. It might take a while after the job is successfully
  submitted to get into the `running` state. Here, information about the state
  of the job in ARC middleware can provide additional clues about what is happening
  to the job.
- `cancelled` - jobs that are killed after they were submitted to ARC middleware
  eventually end up in this state. Such jobs have to be cleaned.
- `failed` - jobs that failed because of some error end up in this state. Sometimes,
  jobs end up in this state after being killed. Some failed jobs can be downloaded
  to inspect logs or partial outputs, depending on how the failure happened.
  The only way for user to find out if there is anything to be downloaded is to
  instruct aCT to fetch (`act fetch`) any available results (read on for info on
  that). Failed jobs can also be resubmitted with `act resub` or cleaned from
  the system with `act clean`.
- `donefailed` - `failed` jobs end up in this state if aCT is instructed to fetch
  them. User can try to download such jobs (`act get`) but there might be nothing
  to download.
- `finished` - the job finished successfully in ARC middleware but its results
  haven't been fetched by aCT yet. The fetch is automatic, the user just has to
  wait for aCT to do that. Therefore, `finished` is not a "terminal" state and
  such jobs should be waited to transition to `done`.
- `done` - this is a state of successfully finished jobs that have also been
  fetched by aCT and can now be downloaded with `act get`. Such jobs can
  also be cleaned if the results are not important.

There are other possible states. You can always try to kill jobs that end up
in weird states and then clean them if necessary. Otherwise, you should
consult admin and developers.

## Authentication
To perform any operation on aCT, the user needs to be authenticated. This is done
with a valid proxy certificate with proper VOMS extensions (by using `arcproxy`
for instance). Then, the proxy needs to be submitted to aCT:  
`(act-venv) $ act proxy`  

This creates a delegated proxy on server required by aCT for further job and data
operations and stores a local access token. The lifetime of the token is the same
as for the proxy. If the token and proxy expire while jobs are still managed by
aCT, the user should create a new proxy and submit it with `act proxy`. This will
create a new delegated proxy and access token and allow further operations on
existing jobs.

**WARNING**: If jobs appear to be stuck in some intermediate state for unusual
amount of time it might be because proxy or VOMS attributes are expired. In such
case, create a new proxy and submit it to aCT.

## Submission
Jobs can be submitted using the following command:  
`(act-venv) $ act sub job1.xrsl job2.xrsl ...`  
The command instructs aCT to submit jobs to a given list of clusters. Those can
be provided in several ways. First option is to provide a list of cluster URLs:
`--clusterlist=https://arc01.vega.izum.si/cpu,https://arc02.vega.izum.si/cpu`
Other two options require configuration to be set up properly. If no `--clusterlist`
flag is given, the program will take a list of clusters from `default` group from
configuration. Instead of a list of URLs, the user can also give a name of a cluster
group from configuration that the program will then look up, e. g.
`--clusterlist=vega`. Refer to *Configuration* section example for more info.
The list of enabled clusters on aCT server can be obtained by running `act info`.

Another flag that can be given for submission command is `--webdav`. If this flag
is not given, the local job input files will be uploaded to internal data management
system of aCT. If this flag is provided without a value, the value will be taken
from configuration file. The client will treat the value as a URL to WebDAV
directory and upload input files there. If this flag is given a value, it will
be used as a URL.

**WARNING**: Currently, if you use explicit URL for a particular group of jobs
that is different from the one in configuration or if configuration does not
have WebDAV URL specified, you have to use the `--webdav` flag with the same URL
for any subsequent `act clean`, `act get` or `act kill` command on those jobs for
the client to properly clean up all data files from given location. Otherwise,
the files would start to pile up and have to be deleted manually.

**WARNING**: If you check status of submitted jobs immediately after submission,
the submitted jobs will have empty state. aCT periodically checks for freshly
submitted jobs and assigns a proper state to them (period is determined by
server configuration). However, if jobs remain in an empty state for a longer
period of time, there is probably something wrong and the user has to kill such
jobs to remove them from the system.

**WARNING**: Currently, there is no indicator on how job submission is
progressing. If you submit many jobs with big files it can take a while for
submission of all to be finished and results to be printed.

**WARNING**: There is a brief period of time in the beginning of job submission
process where signals are ignored. It may thus appear as if submission cannot be
cancelled. In such case, you can continue trying to cancel it using ctrl+c.

## Job status
To check the status of all jobs, run the following command:  
`(act-venv) $ act stat -a`  
It prints a table of values that provide information on jobs managed by aCT.
The default values provide for instance the ID of a job, its name, ID in ARC
middleware and states in ARC middleware as well as in aCT.
The `stat` command has several parameters for filtering jobs based in ID, name
and state. It also takes two flags, `--arc` and `--client` that allow the user
to specify exactly which job attributes they want to be printed.
`act stat --get-cols` should be consulted for info on which attributes can be queried.

## Fetching and resubmitting failed jobs
Jobs in `failed` state can be fetched using `act fetch`. aCT will download any
outputs and mark jobs `donefailed`. Jobs that are `failed` can also be
resubmitted using `act resub`.

## Downloading job results
The results of jobs in `done` or `donefailed` state can be downloaded using the
command `act get`. `donefailed` jobs sometimes don't have results available.
The results will be downloaded to the current directory. For every job a directory
with a hash name of its ARC ID will be created and output files will be stored in
that directory (as is with `arcget` from ARC Client tools). Successfully downloaded
jobs are automatically cleaned from the system.

## Killing and cleaning jobs
Command `act kill` is used to kill jobs that are in one of the submission or running
states or if their state is empty (freshly submitted jobs that are waiting to be
passed on to ARC middleware). Jobs with empty state are also cleaned from the
system automatically. Any jobs that aCT started to process eventually have to be
cleaned from the system. This happens automatically with successful `act get`
operation or by excplicitly using `act clean` on certain terminal states like
`failed`, `done`, `donefailed`, `cancelled`.

## Ctrl+C
aCT client programs have to perform proper cleanup of jobs and data files in case
of certain errors or specific conditions, like ctrl+c. That means that program
execution cannot always be stopped immediately. Programs that have to perform
proper cleanup and can take a while before they stop if they are cancelled with
ctrl+c are `act get` and `act sub`. Programs `act kill` and `act clean` can also
perform longer cleanup but they cannot be cancelled as cleanup is their only
operation.
