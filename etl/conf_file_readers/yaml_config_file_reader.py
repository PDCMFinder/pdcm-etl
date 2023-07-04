from functools import lru_cache
from pathlib import Path

import yaml


@lru_cache(maxsize=None)
def read_yaml_config_file(file_name):
    # Directory containing configuration files
    path = Path(__file__).parent / "../" / file_name
    with open(path, "r") as ymlFile:
        conf = yaml.safe_load(ymlFile)
    return conf
