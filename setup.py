from setuptools import setup, find_packages

setup(name='aCT',
      version='0.1',
      description='ATLAS Control Tower',
      url='http://github.com/ATLASControlTower/aCT',
      python_requires='>=2.7',
      author='aCT team',
      author_email='act-dev@cern.ch',
      license='Apache 2.0',
      package_dir = {'': 'src'},
      packages=find_packages('src'),
      install_requires=[
        'mysql-connector-python',   # connection to MySQL database
        'htcondor',                 # bindings to use HTCondor to submit jobs
        'pylint',                   # for travis automatic tests
        'requests',                 # for APF mon calls

        'pyopenssl',
        'flask',
        'gunicorn',           # Python 2 is not supported in >= 20.*
        'sqlalchemy'
      ],
      entry_points={
        'console_scripts': [
            'actbootstrap = act.common.aCTBootstrap:main',
            'actmain = act.common.aCTMain:main',
            'actreport = act.common.aCTReport:main',
            'actcriticalmonitor = act.common.aCTCriticalMonitor:main',
            'actheartbeatwatchdog = act.atlas.aCTHeartbeatWatchdog:main',

            'actbulksub = act.client.actbulksub:main',
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
      data_files=[
          ('etc/act', ['doc/aCTConfigARC.xml.template',
                       'doc/aCTConfigATLAS.xml.template'])
      ]
)
