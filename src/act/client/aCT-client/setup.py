from setuptools import setup

setup(
    name = 'aCT-client',
    version = '0.1',
    url = 'http://github.com/ATLASControlTower/aCT',
    author = 'aCT team',
    author_email = 'act-dev@cern.ch',
    package_dir = {'': 'src'},
    py_modules = ['config', 'common'],
    scripts = [
        'src/actproxy.py',
        'src/actlistproxies.py',
        'src/actdeleteproxy.py',
        'src/actstat.py',
        'src/actclean.py',
        'src/actfetch.py',
        'src/actkill.py',
        'src/actresub.py',
        'src/actsub.py',
        'src/actget.py',
    ],
    install_requires = [
        'requests',
    ]
)
