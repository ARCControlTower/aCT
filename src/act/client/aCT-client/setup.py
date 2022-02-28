from setuptools import setup

setup(
    name = 'aCT-client',
    version = '0.1',
    url = 'http://github.com/ATLASControlTower/aCT',
    author = 'aCT team',
    author_email = 'act-dev@cern.ch',
    package_dir = {'': 'src'},
    entry_points={
        'console_scripts': [
            'act = act_client.cli:main',
        ]
    },
    install_requires=[
        'cryptography',
        'pyyaml',
        'trio',
        'httpx',
    ]
)
