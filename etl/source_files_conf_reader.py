from functools import lru_cache
from pathlib import Path

import yaml


@lru_cache(maxsize=None)
def read_groups():
    sources_file_path = "etl/sources.yaml" if Path("etl/sources.yaml").is_file() else "sources.yaml"
    with open(sources_file_path, "r") as ymlFile:
        conf = yaml.safe_load(ymlFile)
    return conf["groups"]


def read_module(module_name):
    groups = read_groups()
    for group in groups:
        for module in group["modules"]:
            if module_name == module["name"]:
                return module
    return None

