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
        'mysql-connector==2.1.*',   # connection to MySQL database
        'htcondor',                 # bindings to use HTCondor to submit jobs
        'pylint',                   # for travis automatic tests
        'requests'                  # for APF mon calls
      ],
      entry_points={
        'console_scripts': [
            'actmain = act.common.aCTMain:main',
            'actreport = act.common.aCTReport:main'
        ]
      },
      data_files=[
          ('etc/act', ['doc/aCTConfigARC.xml.template',
                       'doc/aCTConfigATLAS.xml.template'])
      ]
)