from functools import lru_cache
from pathlib import Path

import yaml


@lru_cache(maxsize=None)
def read_entities():
    dic = {}
    entities_file_path = "etl/entities.yaml" if Path("etl/entities.yaml").is_file() else "entities.yaml"
    with open(entities_file_path, "r") as ymlFile:
        conf = yaml.safe_load(ymlFile)
    entities = conf["entities"]
    for entity in entities:
        entityName = entity['entity']
        dic[entityName] = entity['columns']
    return dic


def get_columns_by_entity(entity):
    entities = read_entities()
    return entities[entity]