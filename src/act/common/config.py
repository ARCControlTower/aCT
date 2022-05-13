import yaml


class Config:

    defaultConfig = {
        "jwt_secret": "aCT JWT secret",
        "clusters": [],
        "timeouts": {
            "ARC_state": {
                "ACCEPTING": 60,
                "ACCEPTED": 60,
                "PREPARING": 60,
                "PREPARED": 60,
                "SUBMITTING": 60,
                "QUEUING": 60,
                "RUNNING": 10080,
                "HELD": 60,
                "EXITINGLRMS": 60,
                "OTHER": 10080,
                "EXECUTED": 60,
                "FINISHING": 60,
                "FINISHED": 2880,
                "FAILED": 2880,
                "KILLING": 60,
                "KILLED": 2880,
                "WIPED": 2880,
            },
            "aCT_state": {
                "cancelling": 60,
            },
        }
    }

    def __init__(self, confpath="/etc/act/config.yaml"):
        self.config = deepCopy(Config.defaultConfig)

        if confpath:
            self.loadYAML(confpath)

    def loadYAML(self, confpath):
        with open(confpath) as f:
            yml = yaml.safe_load(f.read())
        loadYAML(self.config, yml)

    def __getitem__(self, arg):
        return self.config[arg]


def deepCopy(config):
    if isinstance(config, dict):
        newDict = {}
        for key, value in config.items():
            newDict[key] = deepCopy(value)
        return newDict
    elif isinstance(config, list):
        newList = []
        for value in config:
            newList.append(deepCopy(value))
        return newList
    else:
        return config


def loadYAML(config, yml):
    if not isinstance(config, type(yml)):
        return

    if isinstance(yml, dict):
        for key, value in yml.items():
            if key not in config:
                config[key] = yml[key]
            elif not isinstance(config[key], type(yml[key])):
                continue
            elif isinstance(yml[key], dict):
                loadYAML(config[key], yml[key])
            else:
                config[key] = yml[key]
