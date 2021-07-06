from functools import lru_cache

import yaml


@lru_cache(maxsize=None)
def read_groups():
    with open("etl/sources.yaml", "r") as ymlFile:
        conf = yaml.safe_load(ymlFile)
    return conf["groups"]


