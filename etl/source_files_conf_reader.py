from functools import lru_cache

import yaml


@lru_cache(maxsize=None)
def read_groups():
    with open("etl/sources.yaml", "r") as ymlFile:
        conf = yaml.safe_load(ymlFile)
    return conf["groups"]


def read_module(module_name):
    groups = read_groups()
    for group in groups:
        for module in group["modules"]:
            if module_name == module["name"]:
                return module
    return None

