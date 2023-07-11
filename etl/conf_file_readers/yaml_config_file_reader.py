from functools import lru_cache
from pathlib import Path

import yaml


@lru_cache(maxsize=None)
def read_yaml_config_file(file_name):
    file_path = file_name
    file_path_with_root_folder = "etl/" + file_path
    if Path(file_path_with_root_folder).is_file():
        file_path = file_path_with_root_folder
    # Directory containing configuration files
    with open(file_path, "r") as ymlFile:
        conf = yaml.safe_load(ymlFile)
    return conf
