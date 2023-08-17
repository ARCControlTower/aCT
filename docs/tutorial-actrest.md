# aCT REST on Almalinux 9

In this tutorial, you will set up an aCT server with the REST interface that can
be used with aCT client on Almalinux 9 server *hostname*. Replace *hostname* in
relevant commands with your server. aCT will run as *act* user from a custom
Python virtual environment. *root* is used for admin tasks.

Until further notice, the commands are executed as the root user on the server machine.


## "fix" selinux

SELinux prevents systemd to run virtual environment programs as services.  You
can fix this by either disabling SELinux or setting things up properly to work
with SELinux. Here, SELinux is disabled. `SELINUX=disabled` has to be set
in `/etc/selinux/config` and the system has to be rebooted.


## Create *act* user
```
useradd act
```


## Set password for *act* user
```
passwd act
```


## Set up SSH access

Copy ssh key for *root* access (run this command on your machine(s)):
```
ssh-copy-id root@hostname
```

Copy ssh key for *act* access (run this command on your machine(s)):
```
ssh-copy-id act@hostname
```

Create or edit ssh config file to disable ssh password login, enable
root login and key authentication:
``` txt title="/etc/ssh/sshd_config.d/70-actssh.conf"
PermitRootLogin yes
PubkeyAuthentication yes
PasswordAuthentication no
```

Restart SSH service:
```
systemctl restart sshd
```

## Upgrade the system
```
dnf upgrade -y
```

## Install config-manager command for YUM
```
dnf install -y yum-utils
```

## Enable CRB repository
```
dnf config-manager --set-enabled crb
```

## Install Epel repository
```
dnf install -y epel-release
```

## Create database for aCT

### Install database
```
dnf install -y mariadb mariadb-server
```

### Enable and start database service
```
systemctl enable --now mariadb
```

### Run the database setup script
Follow the defaults and set the root password:
```
mysql_secure_installation
```

### Connect to DB
```
mariadb
```

### Create database *act* (run this command in database prompt)
``` mysql
create database act;
```

### Create user *act* with access to the database *act* (run this command in database prompt)
Come up with your own *PASSWORD*:
``` mysql
grant all privileges on act.* TO 'act'@'%' identified by 'PASSWORD';
```

## Install NorduGrid ARC Client Tools
[Installation instructions](http://www.nordugrid.org/arc/arc6/users/client_install.html) for reference.

### Install NorduGrid repository for Rocky 9 (compatible with Almalinux 9)
```
dnf install -y https://download.nordugrid.org/packages/nordugrid-release/releases/6.1/rocky/9/x86_64/nordugrid-release-6.1-1.el9.noarch.rpm
```

!!! warning

    There is a bug in the *nordugrid-release* package repo files where *baseurl* entries have wrong URL path.
    The `centos/el9` URL portions has to be replaced with `rocky/9` in the following files:

    * `/etc/yum.repos.d/nordugrid.repo`
    * `/etc/yum.repos.d/nordugrid-updates.repo`
    * `/etc/yum.repos.d/nordugrid-testing.repo`

### Install ARC packages required for aCT
```
dnf install -y nordugrid-arc-client nordugrid-arc-arcctl nordugrid-arc-plugins-arcrest python3-nordugrid-arc
```

### Install security credentials bundle
```
arcctl deploy igtf-ca classic
```


## Install system dependencies
```
dnf install -y git logrotate
```


## Set up aCT

aCT requires several directories to be set up for logs, tmp files, virtual environment and
other runtime data. Here, all directories will be in a single work directory, `$HOME/workdir`.

The configs will be stored in /etc/act that has to be created with proper permissions.

### Set up config in `/etc/act`

Create `/etc/act/aCTConfigARC.yaml`. Use the *PASSWORD* created for the
*act* database user.
``` yaml title="/etc/act/aCTConfigARC.yaml"
db:
    host: localhost
    port: 3306
    name: act
    type: mysql
    user: act
    password: PASSWORD

tmp:
    dir: /home/act/workdir/tmp

logger:
    level: debug
    arclevel: verbose
    logdir: /home/act/workdir/log

jobs:
    checkinterval: 30
    checkmintime: 20
    maxtimerunning: 604800
    maxtimeundefined: 86400
    maxtimeidle: 86400
    maxtimeheld: 3600
    maxtimehold: 172800
    maxtimepreparing: 86400
    maxtimefinishing: 86400
    maxtimefinished: 86400

voms:
    proxystoredir: /home/act/workdir/proxies
    cacertdir: /etc/grid-security/certificates

actlocation:
    datman: /home/act/workdir/datman
```

Generate a JWT secret for the REST tokens:
```
openssl rand -base64 64
```

Create `/etc/act/aCTConfigAPP.yaml`. Use the JWT secret from previous step.
For the list of clusters use URLs to queues that you want the aCT to allow sending jobs to.
``` yaml title="/etc/act/aCTConfigAPP.yaml"
modules:
    - act.arc
    - act.client

disabledProcs:
    act.arc:
        single:
            - aCTProxyHandler
            - aCTMonitor

user:
    jwt_secret: "JWTSECRET"
    clusters:
        - https://pikolit.ijs.si/batch
        - https://rebula.ijs.si/batch
        - https://arc01.vega.izum.si/cpu
        - https://arc02.vega.izum.si/cpu
        - https://arc01.vega.izum.si/largemem
        - https://arc02.vega.izum.si/largemem
        - https://hpc.arnes.si/all
        - https://nsc.ijs.si/gridlong
        - https://maister1.hpc-rivr.um.si/long
        - https://situla.fis.unm.si/compute
```

Set the ownership of the config files to *act* user:
```
chown act:act /etc/act/aCTConfigARC.yaml /etc/act/aCTConfigAPP.yaml
```

Set file permissions:
```
chmod 660 /etc/act/aCTConfigARC.yaml /etc/act/aCTConfigAPP.yaml
```

### Create work directory for aCT
This is where logs, runtime data, virtual environment and deployment files
will all be in one place.
Until further notice, run the commands as *act* user.
```
mkdir -p $HOME/workdir
```

### Create Python virtual environment
```
python3 --system-site-packages -m venv $HOME/workdir/venv
```

### Update several packages that manage packages to avoid potential warnings
```
$HOME/workdir/venv/bin/pip install -U pip setuptools wheel
```

### Create requirements.in file to install and upgrade packages

``` txt title="$HOME/workdir/requirements.in"
aCT @ git+https://github.com/ARCControlTower/aCT.git@dev

pyarcrest @ git+https://github.com/jakobmerljak/pyarcrest.git@altapi

gunicorn
```

### Install packages from requirements.in file
```
$HOME/workdir/venv/bin/pip install -r $HOME/workdir/requirements.in
```

### Run actbootstrap
```
$HOME/workdir/venv/bin/actboostrap
```

### Configure systemd services for aCT

Create the *act* service file:
``` ini title="/etc/systemd/system/act.service"
[Unit]
Description = aCT job management framework
Requires = mariadb.service
After = mariadb.service

[Service]
ExecStart = /home/act/workdir/venv/bin/actmain
Restart = on-failure
User = act

# prefer mixed kill mode to prevent double signal to child processes
KillMode = mixed

[Install]
WantedBy = multi-user.target
```

Create the *actrest* service file:
``` ini title="/etc/systemd/system/actrest.service"
[Unit]
Description = Python APP server for aCT REST
Requires = mariadb.service
After = mariadb.service

[Service]
ExecStart = /home/act/workdir/venv/bin/gunicorn act.client.app:app --bind 0.0.0.0:8080 --timeout 900
Restart = on-failure
User = act

[Install]
WantedBy = multi-user.target
```

Reload unit files:
```
systemctl daemon-reload
```

Enable and start systemd services:
```
systemctl enable --now act actrest
```

## Firewall
Open port 8080 for TCP:
```
firewall-cmd --permanent --zone=public --add-port=8080/tcp
```

Reload firewall:
```
firewall-cmd --reload
```

## HAProxy config (optional)

This is a config portion of HAProxy config that can be used to add SSL encryption
and proxying to gunicorn:

``` txt title="/etc/haproxy/haproxy.cfg"
frontend http_web
    bind :80
    bind :::80
    mode http
    default_backend rgw

frontend rgw-https
    mode http
    bind :443 ssl crt /etc/ssl/private/rgw.pem
    bind :::443 ssl crt /etc/ssl/private/rgw.pem
    option forwardfor
    default_backend rgw

backend rgw
    mode http
    server rgw1 127.0.0.1:8080 check
```
