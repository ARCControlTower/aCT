# Reference for implementation:
# - https://stackoverflow.com/questions/2352181/how-to-use-a-dot-to-access-members-of-dictionary
# - https://www.pythonlikeyoumeanit.com/Module4_OOP/Special_Methods.html
# - https://starriet.medium.com/getattr-vs-getattribute-in-python-840f936836b2

import os

import yaml


class DictObj:
    """Implements more ergonomic interface for configuration structure."""

    def __init__(self, dictionary):
        """Recursively construct DictObj from given config dictionary."""
        for name, value in dictionary.items():
            setattr(self, name, DictObj._wrap(value))

    def __iter__(self):
        """Implement dict like iteration over attributes and values."""
        return iter(vars(self).items())

    def __getitem__(self, key):
        """
        Return attribute or empty DictObj if accessed with index.

        This method makes DictObj subscriptable to avoid exception in cases
        when nonexistent list attributes are accessed as part of a chain.
        """
        return getattr(self, key) if isinstance(key, str) else DictObj({})

    def __getattr__(self, attr):
        """Return empty object on missing attribute."""
        return DictObj({})

    def __bool__(self):
        """Return false for empty objects."""
        return bool(vars(self))

    def __contains__(self, attr):
        """Implement existence check with "in" keyword."""
        return attr in vars(self)

    def get(self, attr, default=None):
        """Implement dict like attr access with attr name and default value."""
        return getattr(self, attr) or default

    def __str__(self):
        return str(vars(self))

    @staticmethod
    def _wrap(value):
        if isinstance(value, list):
            return [DictObj._wrap(el) for el in value]
        elif isinstance(value, dict):
            return DictObj(value)
        else:
            return value


class aCTConfig:

    def __init__(self, path):
        self.path = path
        self.tparse = 0
        self.parse()

    def parse(self):
        mtime = os.stat(self.path)[8]
        if mtime <= self.tparse:
            return
        with open(self.path) as f:
            self.conf = DictObj(yaml.safe_load(f.read()))
        self.tparse = mtime

    def __iter__(self):
        """Proxy conf object iteration."""
        return iter(self.conf)

    def __getitem__(self, key):
        """Proxy conf object list access."""
        return self.conf[key]

    def __getattr__(self, attr):
        """Proxy conf object attributes."""
        return getattr(self.conf, attr)

    def __bool__(self):
        """Proxy boolean value for conf object."""
        return bool(self.conf)

    def __contains__(self, attr):
        """Proxy existence check with "in" keyword for conf object."""
        return attr in self.conf

    def get(self, attr, default=None):
        """Proxy dict like attr access with attr name and default value."""
        return self.conf.get(attr, default)

    def __str__(self):
        return str(self.conf)


class aCTConfigARC(aCTConfig):

    def __init__(self, path=""):
        if path and os.path.exists(path):
            confpath = path
        elif 'ACTCONFIGARC' in os.environ and os.path.exists(os.environ['ACTCONFIGARC']):
            confpath = os.environ['ACTCONFIGARC']
        elif 'VIRTUAL_ENV' in os.environ and os.path.exists(os.path.join(os.environ['VIRTUAL_ENV'], 'etc', 'act', 'aCTConfigARC.yaml')):
            confpath = os.path.join(os.environ['VIRTUAL_ENV'], 'etc', 'act', 'aCTConfigARC.yaml')
        elif os.path.exists('/etc/act/aCTConfigARC.yaml'):
            confpath = '/etc/act/aCTConfigARC.yaml'
        elif os.path.exists('aCTConfigARC.yaml'):
            confpath = "aCTConfigARC.yaml"
        else:
            raise Exception('aCTConfigARC.yaml not found')
        super().__init__(confpath)


class aCTConfigAPP(aCTConfig):

    def __init__(self, path=""):
        if path and os.path.exists(path):
            confpath = path
        elif 'ACTCONFIGAPP' in os.environ and os.path.exists(os.environ['ACTCONFIGAPP']):
            confpath = os.environ['ACTCONFIGAPP']
        elif 'VIRTUAL_ENV' in os.environ and os.path.exists(os.path.join(os.environ['VIRTUAL_ENV'], 'etc', 'act', 'aCTConfigAPP.yaml')):
            confpath = os.path.join(os.environ['VIRTUAL_ENV'], 'etc', 'act', 'aCTConfigAPP.yaml')
        elif os.path.exists('/etc/act/aCTConfigAPP.yaml'):
            confpath = '/etc/act/aCTConfigAPP.yaml'
        elif os.path.exists('aCTConfigAPP.yaml'):
            confpath = "aCTConfigAPP.yaml"
        else:
            raise Exception('aCTConfigAPP.yaml not found')
        super().__init__(confpath)


if __name__ == "__main__":
    conf = aCTConfigARC()
    for key, value in conf:
        print(f"{key}: {value}")
    print(f'"voms" in conf: {"voms" in conf}')
    print(f'"blablabla" in conf: {"blablabla" in conf}')
    print(f'not conf.voms: {not conf.voms}')
    print(f'not conf.blablabla: {not conf.blablabla}')
    print('conf.get("voms", DictObj()): {}'.format(conf.get("voms", DictObj({}))))
    print('conf.get("blablabla", DictObj()): {}'.format(conf.get("blablabla", DictObj({}))))

    for key, value in conf.voms:
        print(f"{key}: {value}")
    print(f'"roles" in conf.voms: {"roles" in conf.voms}')
    print(f'"blablabla" in conf.voms: {"blablabla" in conf.voms}')
    print(f'not conf.voms.roles: {not conf.voms.roles}')
    print(f'not conf.voms.blablabla: {not conf.voms.blablabla}')
    print('conf.voms.get("roles", []): {}'.format(conf.voms.get("roles", [])))
    print('conf.voms.get("blablabla", []): {}'.format(conf.voms.get("blablabla", [])))

    for role in conf.voms.roles:
        print(f"VOMS role: {role}")

    conf = aCTConfigAPP()
    for key, value in conf:
        print(f"{key}: {value}")
    print(f'"panda" in conf: {"panda" in conf}')
    print(f'"blablabla" in conf: {"blablabla" in conf}')
    print(f'not conf.panda: {not conf.panda}')
    print(f'not conf.blablabla: {not conf.blablabla}')
    print('conf.get("panda", DictObj()): {}'.format(conf.get("panda", DictObj({}))))
    print('conf.get("blablabla", DictObj()): {}'.format(conf.get("blablabla", DictObj({}))))

    for key, value in conf.panda:
        print(f"{key}: {value}")
    print(f'"roles" in conf.panda: {"roles" in conf.panda}')
    print(f'"blablabla" in conf.panda: {"blablabla" in conf.panda}')
    print(f'not conf.panda.sites: {not conf.panda.sites}')
    print(f'not conf.panda.blablabla: {not conf.panda.blablabla}')
    print('conf.panda.get("sites", []): {}'.format(conf.panda.get("sites", [])))
    print('conf.panda.get("blablabla", []): {}'.format(conf.panda.get("blablabla", [])))

    for site in conf.panda.sites:
        print(f"site: {site}")

    print(f'conf.blablabla.blablablabla.voms.roles[10].whatever.somemoreblablabla.get("maxtimerunning", 1234): {conf.blablabla.blablablabla.voms.roles[10].whatever.somemoreblablabla.get("maxtimerunning", 1234)}')
