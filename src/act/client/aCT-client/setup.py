from setuptools import setup

setup(
    name = 'aCT-client',
    version = '0.1',
    url = 'http://github.com/ATLASControlTower/aCT',
    author = 'aCT team',
    author_email = 'act-dev@cern.ch',
    package_dir = {'': 'src'},
    py_modules = [
        'actproxy',
        'actstat',
        'actclean',
        'actfetch',
        'actkill',
        'actresub',
        'actsub',
        'actget',
        'config',
        'common',
        'x509proxy',
        'delegate_proxy',
    ],
    entry_points={
        'console_scripts': [
            'actproxy       = actproxy:main',
            'actstat        = actstat:main',
            'actclean       = actclean:main',
            'actfetch       = actfetch:main',
            'actkill        = actkill:main',
            'actresub       = actresub:main',
            'actsub         = actsub:main',
            'actget         = actget:main',
        ]
    },
    install_requires = [
        'cryptography',
        'pyyaml',
        'trio',
        'httpx',
    ]
)
