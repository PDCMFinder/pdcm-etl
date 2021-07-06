from functools import lru_cache

import yaml


@lru_cache(maxsize=None)
def read_groups():
    with open("etl/sources.yaml", "r") as ymlFile:
        conf = yaml.safe_load(ymlFile)
    return conf["groups"]


def build_schema_by_group_file(file_id: str):
    conf_groups = read_groups()
    for group in conf_groups:
        for file in group["files"]:
            if file["id"] == file_id:
                columns = file["columns"]

