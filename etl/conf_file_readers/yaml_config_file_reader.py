import os
from functools import lru_cache
from pathlib import Path

import yaml


@lru_cache(maxsize=None)
def read_yaml_config_file(file_name):
    print("Debug info for read_yaml_config_file [", file_name, "]")
    cwd = os.getcwd()
    print("cwd", cwd)
    op1 = "etl/" + file_name
    op2 = "../etl/" + file_name
    print("original:", file_name, ".Path?", Path(file_name).is_file())
    print("op1:", op1, ".Path?", Path(op1).is_file())
    print("op2:", op2, ".Path?", Path(op2).is_file())
    file_path = file_name
    file_path_with_root_folder = "etl/" + file_path
    if Path(file_path_with_root_folder).is_file():
        file_path = file_path_with_root_folder
    # Directory containing configuration files
    with open(file_path, "r") as ymlFile:
        conf = yaml.safe_load(ymlFile)
    return conf
