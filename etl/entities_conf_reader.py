from functools import lru_cache

import yaml


@lru_cache(maxsize=None)
def read_entities():
    dic = {}
    with open("etl/entities.yaml", "r") as ymlFile:
        conf = yaml.safe_load(ymlFile)
    entities = conf["entities"]
    for entity in entities:
        entityName = entity['entity']
        dic[entityName] = entity['columns']
    return dic


def get_columns_by_entity(entity):
    entities = read_entities()
    return entities[entity]