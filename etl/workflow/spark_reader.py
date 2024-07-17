import glob
import json
import time
import yaml

import luigi
from luigi.contrib.spark import PySparkTask
from py4j.protocol import Py4JJavaError
from pyspark import SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.utils import IllegalArgumentException
from pyspark.sql.functions import lit, input_file_name, regexp_extract
from pyspark.sql.types import StructType, StructField, StringType

from etl import logger
from etl.constants import Constants
from etl.jobs.util.cleaner import trim_all_str
from etl.source_files_conf_reader import read_module
from etl.workflow.config import PdcmConfig

ROOT_FOLDER = "data/UPDOG"


def build_schema_from_cols(columns):
    schema = []
    for column in columns:
        schema.append(StructField(column, StringType(), True))
    return StructType(schema)


def select_rows_with_data(df: DataFrame, columns) -> DataFrame:
    print(f"cols from select_rows_with_data: {columns}")
    if "Field" in df.columns:
        df = df.select(columns).where("nvl(field, '') not like '#%'")
    else:
        df = df.select(columns)
    return df


def clean_column_names(df: DataFrame):
    columns = df.columns
    for column in columns:
        df = df.withColumnRenamed(column, trim_all_str(column))
    return df


def read_files(session, path_patterns, schema):
    start = time.time()

    df = session.read.option('sep', '\t').option('header', True).option('schema', schema).csv(
        [p.replace("'", "") for p in path_patterns])
    df = clean_column_names(df)
    df = select_rows_with_data(df, schema.fieldNames())
    datasource_pattern = "{0}\\/([a-zA-Z-]+)(\\/)".format(ROOT_FOLDER.replace("/", "\\/"))
    df = df.withColumn("_data_source", lit(input_file_name()))
    df = df.withColumn(Constants.DATA_SOURCE_COLUMN, regexp_extract("_data_source", datasource_pattern, 1))
    df = df.drop("_data_source")

    end = time.time()
    logger.info(
        "Read from path {0} count: {1} in {2} seconds".format(path_patterns, df.count(), round(end - start, 4)))
    return df


def read_json(session, json_content):
    df = session.read.option("multiline", True).json(session.sparkContext.parallelize([json_content]))
    return df


class ReadByModuleAndPathPatterns(PySparkTask):
    raw_folder_name = luigi.Parameter()
    path_patterns = luigi.ListParameter()
    columns_to_read = luigi.ListParameter()
    data_dir_out = luigi.Parameter()

    def output(self):
        return PdcmConfig().get_target(
            "{0}/{1}/{2}".format(self.data_dir_out, Constants.RAW_DIRECTORY, self.raw_folder_name))

    def app_options(self):
        return [
            f"'{','.join([p for p in self.path_patterns])}'",
            ','.join(self.columns_to_read),
            self.output().path]

    def main(self, sc: SparkContext, *args):
        spark = SparkSession(sc)
        print(f"input: {args}")
        path_patterns = args[0].split(',')
        columns_to_read = args[1].split(',')
        output_path = args[2]

        schema = build_schema_from_cols(columns_to_read)

        try:
            if path_patterns == ["''"]:
                raise IOError("Empty path")
            df = read_files(spark, path_patterns, schema)
        except (Py4JJavaError, IllegalArgumentException, FileNotFoundError, IOError) as error:
            no_empty_patterns = list(filter(lambda x: x != '', path_patterns))
            if "java.io.FileNotFoundException" in str(error) or len(no_empty_patterns) == 0 or error.__class__ in [
                FileNotFoundError, IOError]:
                empty_df = spark.createDataFrame(sc.emptyRDD(), schema)
                df = empty_df
                df = df.withColumn(Constants.DATA_SOURCE_COLUMN, lit(""))
            else:
                raise error
        df.write.mode("overwrite").parquet(output_path)


def build_path_patterns(data_dir, providers, file_patterns):
    data_dir_root = "{0}/{1}".format(data_dir, ROOT_FOLDER)
    paths_patterns = []

    for file_pattern in file_patterns:
        matching_providers = []
        for provider in providers:
            current_file_pattern = str(file_pattern).replace("$provider", provider)
            if glob.glob("{0}/{1}/{2}".format(data_dir_root, provider, current_file_pattern)):
                paths_patterns.append("{0}/{1}/{2}".format(data_dir_root, provider, current_file_pattern))
    return paths_patterns


def build_path_pattern_by_provider(data_dir, provider, file_pattern):
    data_dir_root = "{0}/{1}".format(data_dir, ROOT_FOLDER)
    path_pattern = "{0}/{1}/{2}".format(data_dir_root, provider, file_pattern.replace("$provider", provider))
    return path_pattern


def get_tsv_extraction_task_by_module(data_dir, providers, data_dir_out, module_name):
    module = read_module(module_name)
    file_patterns = module["name_patterns"]
    columns = module["columns"]
    path_patterns = build_path_patterns(data_dir, list(providers), file_patterns)
    print(f"from get_tsv_extraction_task_by_module: {path_patterns}")
    return ReadByModuleAndPathPatterns(module_name, path_patterns, columns, data_dir_out)


def extract_provider_name(path: str):
    init_index = path.index(ROOT_FOLDER) + len(ROOT_FOLDER) + 1
    next_slash = path.index("/", init_index)
    return path[init_index:next_slash]


def get_json_by_yaml(yaml_content):
    yaml_as_json = yaml.safe_load(yaml_content)
    yaml_as_json = json.dumps(yaml_as_json, ensure_ascii=False)
    return yaml_as_json


class ReadYamlsByModule(PySparkTask):
    raw_folder_name = luigi.Parameter()
    yaml_paths = luigi.ListParameter()
    columns_to_read = luigi.ListParameter()
    data_dir_out = luigi.Parameter()

    def output(self):
        return PdcmConfig().get_target(
            "{0}/{1}/{2}".format(self.data_dir_out, Constants.RAW_DIRECTORY, self.raw_folder_name))

    def app_options(self):
        return [
            ','.join(self.yaml_paths),
            ','.join(self.columns_to_read),
            self.output().path]

    def main(self, sc, *args):
        spark = SparkSession(sc)

        yaml_file_paths = args[0].split(',')
        columns_to_read = args[1].split(',')
        output_path = args[2]

        all_json_and_providers = []

        for yaml_file_path in yaml_file_paths:
            with open(yaml_file_path, 'r', encoding='utf-8') as stream:
                yaml_as_json = get_json_by_yaml(stream)
                json_content_and_provider = (yaml_as_json, extract_provider_name(yaml_file_path))
                all_json_and_providers.append(json_content_and_provider)

        source_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), build_schema_from_cols(columns_to_read))
        source_df = source_df.withColumn(Constants.DATA_SOURCE_COLUMN, lit(None).astype(StringType()))
        for json_and_provider in all_json_and_providers:
            json_content = json_and_provider[0]
            provider = json_and_provider[1]
            df = read_json(spark, json_content)
            df = df.select(columns_to_read)
            df = df.withColumn(Constants.DATA_SOURCE_COLUMN, lit(provider))
            source_df = source_df.union(df)

        source_df.write.mode("overwrite").parquet(output_path)


def get_yaml_extraction_task_by_module(data_dir, providers, data_dir_out, module_name):
    module = read_module(module_name)
    file_patterns = module["name_patterns"]
    columns = module["columns"]
    # There should be only one yaml file by module
    file_path = str(file_patterns[0])

    yaml_paths = []
    for provider in providers:
        yaml_file_path = build_path_pattern_by_provider(data_dir, provider, file_path)
        yaml_paths.append(yaml_file_path)
    return ReadYamlsByModule(module_name, yaml_paths, columns, data_dir_out)


if __name__ == "__main__":
    luigi.run()
