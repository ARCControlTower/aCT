# LDMX Module

This module contains agents for handling jobs for the LDMX experiment

# Configuration

Example app configuration (aCTConfigAPP.yaml):

```
modules:
    app:
        act.ldmx

jobs:
    bufferdir: /path/to/bufferdir

executable:
    wrapper: /path/to/LDCS-repo/LDCS/helperScripts/ldmxsim.sh
    ruciohelper: /path/to/LDCS-repo/LDCS/helperScripts/ldmx-simprod-rte-helper.py
    simprodrte: LDMX-SIMPROD-3.0

joblog:
    urlprefix: http://hostname/jobs
    dir: /var/www/html/jobs
    keepsuccessful: 0
```

- `bufferdir` is a directory for storing temporary configuration files generated per job by aCT
- `wrapper` is the wrapper script which runs the job on grid worker nodes
- `ruciohelper` is another script which runs at the end of the job to extract rucio metadata of output files
- `simprodrte` is the RTE required to run the simulation. The LDMX software RTE is specified in the job configuration.
- `joblog` defines the URL and local directory for job logs. If `keepsuccessful` is set to zero then only failed job logs are kept.

The wrapper and rucio helper scripts are maintained in the LDCS repo at https://github.com/LDMX-Software/LDCS/tree/master/helperScripts

For job submission and management see the "ACT operations" doc in the LDCS google drive folder.

# Job definition

Jobs are defined in configuration files which contain lines of the form `key=value`. Some of these are related to the physics processes of the job and others are used by aCT to manage job configuration and submission.

## aCT-specific fields

| Parameter | Example | Explanation |
| --------- | ------- | ----------- |
| RandomSeed1SequenceStart | 1 | Start value of random seed 1 |
| RandomSeed2SequenceStart | 1001 | Start value of random seed 2 |
| NumberofJobs | 100 | Number of jobs to create from this configuration |
| **Rucio-related parameters** | | |
| Scope | mc20 | Scope of output file/dataset |
| SampleId | v12-4GeV-1e-ecal_photonuclear | Container for output dataset |
| BatchID | v12-4GeV-1e-batch1 | Output dataset name |
| FinalOutputDestination | SLAC_GRIDFTP | RSE to upload output file to |
| **ARC-related parameters** | | |
| RunTimeEnvironment | LDMX-2.1.1 | LDMX software RTE to use |
| JobWallTime | 600 | Walltime estimate of jobs (minutes) |
| JobMemory | 2 | Memory estimate of jobs (GB) |
| **Job input parameters** | | |
| InputDataset | validation:v2.3.0-testInput | Input dataset |
| InputFilesPerJob | 10 | Number of input files in input dataset per job |
| PileupDataset | mc20:v12-4GeV-1e-inclusive_pileup | Pileup dataset |
