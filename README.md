# ARC Control Tower

ARC Control Tower (aCT) is a system for submitting and managing payloads on ARC (and other) Computing Elements. It is used as an interface between Panda and Grid sites for the ATLAS experiment at CERN.

[![Build Status](https://app.travis-ci.com/ARCControlTower/aCT.svg?branch=master)](https://app.travis-ci.com/ARCControlTower/aCT)

# Overview

aCT consists of two related state-machines - one which controls job submission and management on Computing Elements (CEs) and another which manages an app-specific workflow. Normally the app-specific part creates jobs or pulls work from an external service, converts to ARC XRSL job descriptions, and passes them into the ARC-handling part. Then the ARC part submits jobs, queries their status and downloads the output when they finish. The app part can then do any post-processing of the result.

# Installing

aCT requires python >= 3.6 and is designed to run in a python virtual environment.

## Mandatory dependencies

For CentOS 7:

`# yum install epel-release`

`# yum install python-pip nordugrid-arc6-client python36-nordugrid-arc6 git`

ARC python bindings are not available in pip so must be installed as a system package.

## Optional dependencies

`# yum install nordugrid-arc6-plugins-gridftpjob` - for submission to ARC's GridFTP interface

`# yum install nordugrid-arc6-plugins-xrootd` - for aCT to validate any output files written using the xrootd protocol

`# yum install condor` - if aCT will submit to HTCondor-CE or CREAM CE

`# yum install python3-libselinux` - if SELinux is enabled

## Setting up the virtualenv

```
$ python3 -m venv aCT
$ source aCT/bin/activate
$ pip install git+https://github.com/ARCControlTower/aCT
```

Then one of two workarounds must be done to use ARC modules in the virtualenv, either create symlinks inside the virtualenv, eg
```
$ ln -s /usr/lib64/python3.6/site-packages/_arc.cpython-36m-x86_64-linux-gnu.so aCT/lib64/python3.6/site-packages/
$ ln -s /usr/lib64/python3.6/site-packages/arc aCT/lib64/python3.6/site-packages/arc
```
or add the system packages to your python path
```
export PYTHONPATH=/usr/lib64/python3.6/site-packages/arc
```
The actual paths may depend on your system and python version.

aCT requires a database. MySQL/MariaDB is the only officially supported database but work is ongoing to use sqlite. __Note__ that MySQL/MariaDB >= 5.6 is required whereas the default on CentOS 7 is 5.5.

# Configuring

aCT is configured with 2 configuration files, `aCTConfigARC.yaml` and optional `aCTConfigAPP.yaml`. The former configures the ARC side of aCT and the latter configures the app-specific side. These files are searched for in the following places in order until found:
```
$ACTCONFIGARC
$VIRTUAL_ENV/etc/act/aCTConfigARC.yaml
/etc/act/aCTConfigARC.yaml
./aCTConfigARC.yaml
```
and the same for aCTConfigAPP.yaml. Configuration templates can be found in etc/act in the virtualenv.

The app config must contain at least

```yaml
modules:
  app:
    - act.app
```

where `app` is the app-specific python module. The app config can also contain any app-specific configuration.

Once configuration is set up, the `actbootstrap` tool should be used to create the necessary database tables.

# Running

The `actmain` tool starts and stops aCT
```
$ actmain start
Starting aCT... 
$ actmain stop
Stopping aCT...  stopped
```

# Administration

Several tools exist to help administer aCT

- `actreport`: shows a summary of job states and sites for all the jobs in the database
- `actbootstrap`: create database tables
- `actheartbeatwatchdog`: checks the database for jobs that have not sent heartbeats for a given time and manually send the heartbeat
- `actcriticalmonitor`: checks logs for critical error messages in the last hour - can be run in a cron to send emails

# Monitoring

[Prometheus](https://prometheus.io/) monitoring can be enabled by setting the port number on which to expose metrics in `aCTConfigARC.yaml`, eg

```yaml
monitor:
  prometheusport: 8001
```

By default only ARC metrics are reported, but app-specific metrics can also be added - see the developers section below.

# Client tools

__Experimental__ client tools exist which allow job management through simple command line tools (`act sub`, `act stat`, etc). These tools allow aCT to be used as a generic job submission engine, independent from the ATLAS part.

# For developers

Developing a new app for aCT is as easy as defining a new sub-module of `act`. Certain elements of the app files must follow a template:

- `__init__.py` may define a list of agent processes that will be started by aCT with `app_processes = [...]`
- `aCTBootstrap.py` may define a `bootstrap()` method which will be called by `actbootstrap` to peform any initialisation
- `aCTReport.py` may define a `report()` method which will be called by `actreport` and can output any app-specific information
- `aCTMonitor.py` may define a `collect()` method which yields app-specific Prometheus metrics.

Apart from these there is no limit to the naming or number of app agents.
