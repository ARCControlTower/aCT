from setuptools import setup, find_packages

setup(
    name='aCT',
    version='0.1',
    description='ARC Control Tower',
    url='http://github.com/ARCControlTower/aCT',
    python_requires='>=3.6',
    author='aCT team',
    author_email='act-dev@cern.ch',
    license='Apache 2.0',
    package_dir={'': 'src'},
    packages=find_packages('src'),
    install_requires=[
        # utf8 errors can be received for higher versions, should
        # switch to mariadb connector
        'mysql-connector-python==8.0.29',

        'htcondor',                 # bindings to use HTCondor to submit jobs
        'requests',                 # for APF mon calls
        'prometheus_client',        # Prometheus monitoring
        'selinux',                  # SELinux context handling
        'psutil',                   # Reports of process kills
        'tabulate',                 # Pretty tables

        'flask',
        'gunicorn',
        'sqlalchemy',
        'pyjwt',

        # 38.* do not work with python 3.6, 37.* emit deprecation warnings
        # which upset the alarm and monitoring tools
        'cryptography==36.0.2',

        'pyyaml',
        'setproctitle',
    ],
    entry_points={
        'console_scripts': [
            'actbootstrap = act.common.aCTBootstrap:main',
            'actmain = act.common.aCTMain:main',
            'actreport = act.common.aCTReport:main',
            'actcriticalmonitor = act.common.aCTCriticalMonitor:main',
            'actheartbeatwatchdog = act.atlas.aCTHeartbeatWatchdog:main',
            'actldmxadmin = act.ldmx.aCTLDMXAdmin:main',
            'actldmx = act.ldmx.aCTLDMXUser:main',

            'actbulksub = act.client.actbulksub:main',
            'actcat     = act.client.actcat:main',
            'actclean   = act.client.actclean:main',
            'actfetch   = act.client.actfetch:main',
            'actget     = act.client.actget:main',
            'actkill    = act.client.actkill:main',
            'actproxy   = act.client.actproxy:main',
            'actresub   = act.client.actresub:main',
            'actstat    = act.client.actstat:main',
            'actsub     = act.client.actsub:main'
        ]
    },
)
