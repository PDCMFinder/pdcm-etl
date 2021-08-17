import glob
import time

import luigi
from luigi.contrib.spark import PySparkTask
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit, input_file_name, regexp_extract
from pyspark.sql.types import StructType, StructField, StringType

from etl import logger
from etl.constants import Constants
from etl.jobs.util.cleaner import trim_all_str
from etl.source_files_conf_reader import read_groups, read_module

ROOT_FOLDER = "data/UPDOG"


def build_schema_from_cols(columns):
    schema = []
    for column in columns:
        schema.append(StructField(column, StringType(), True))
    return StructType(schema)


def select_rows_with_data(df: DataFrame, columns) -> DataFrame:
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

    df = session.read.option('sep', '\t').option('header', True).option('schema', schema).csv(path_patterns)
    df = clean_column_names(df)
    df = select_rows_with_data(df, schema.fieldNames())

    df = df.withColumn("_data_source", lit(input_file_name()))
    datasource_pattern = "{0}\\/([a-zA-Z-]+)(\\/)".format(ROOT_FOLDER.replace("/", "\\/"))
    df = df.withColumn(Constants.DATA_SOURCE_COLUMN, regexp_extract("_data_source", datasource_pattern, 1))
    df = df.drop("_data_source")
    df.show()
    end = time.time()
    logger.info(
        "Read from path {0} count: {1} in {2} seconds".format(path_patterns, df.count(), round(end - start, 4)))
    return df


class ReadByModuleAndPathPatterns(PySparkTask):
    raw_folder_name = luigi.Parameter()
    path_patterns = luigi.ListParameter()
    columns_to_read = luigi.ListParameter()
    data_dir_out = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget("{0}/{1}/{2}".format(self.data_dir_out, Constants.RAW_DIRECTORY, self.raw_folder_name))

    def app_options(self):
        return [
            '|'.join([p for p in self.path_patterns]),
            ','.join(self.columns_to_read),
            self.output().path]

    def main(self, sc, *args):
        spark = SparkSession(sc)

        path_patterns = args[0].split('|')
        columns_to_read = args[1].split(',')
        output_path = args[2]

        schema = build_schema_from_cols(columns_to_read)

        if len(path_patterns) > 0 and path_patterns != ['']:
            print("@This should be a non empty pattern list:", path_patterns)
            df = read_files(spark, path_patterns, schema)
        else:
            empty_df = spark.createDataFrame(sc.emptyRDD(), schema)
            df = empty_df
            df = df.withColumn(Constants.DATA_SOURCE_COLUMN, lit(""))
        df.write.mode("overwrite").parquet(output_path)


def build_path_patterns(data_dir, providers, file_patterns):
    data_dir_root = "{0}/{1}".format(data_dir, ROOT_FOLDER)
    paths_patterns = []

    for file_pattern in file_patterns:
        print("checking pattern", file_pattern)
        matching_providers = []
        for provider in providers:
            print("...check ", file_pattern, "--", provider)
            print("does this exist??", "{0}/{1}/{2}".format(data_dir_root, provider, file_pattern))
            if glob.glob("{0}/{1}/{2}".format(data_dir_root, provider, file_pattern)):
                matching_providers.append(provider)
        print("matching_providers",matching_providers)
        if matching_providers:
            joined_providers_list = ','.join([p for p in matching_providers])
            providers_pattern = "{" + joined_providers_list + "}"
            path_pattern = "{0}/{1}/{2}".format(data_dir_root, providers_pattern, file_pattern)
            print("this should exist", path_pattern)
            paths_patterns.append(path_pattern)

    print("nre paths_patterns", paths_patterns)
    return paths_patterns


    providers_where_matches = []
    for provider in providers:
        for pattern in file_patterns:
            if glob.glob("{0}/{1}/{2}".format(data_dir_root, provider, pattern)):
                providers_where_matches.append(provider)

    print("Build pattern for {0} {1}. Pattern where matches {2}".format(providers, file_patterns, providers_where_matches))
    if providers_where_matches:
        joined_providers_list = ','.join([p for p in providers_where_matches])
        providers_pattern = "{" + joined_providers_list + "}"

        for file_pattern in file_patterns:
            path_pattern = "{0}/{1}/{2}".format(data_dir_root, providers_pattern, file_pattern)
            paths_patterns.append(path_pattern)

    return paths_patterns


def get_tsv_extraction_task_by_module(data_dir, providers, data_dir_out, module_name):
    module = read_module(module_name)
    file_patterns = module["name_patterns"]
    columns = module["columns"]
    path_patterns = build_path_patterns(data_dir, list(providers), file_patterns)
    return ReadByModuleAndPathPatterns(module_name, path_patterns, columns, data_dir_out)

def get_tasks_to_run(data_dir, providers, data_dir_out):
    tasks = []
    groups = read_groups()
    for group in groups:
        skip = group.get("skip")
        if skip is None or not skip:
            for file in group["modules"]:
                file_id = file["name"]
                file_patterns = file["name_patterns"]
                columns = file["columns"]
                path_patterns = build_path_patterns(data_dir, list(providers), file_patterns)
                tasks.append(ReadByModuleAndPathPatterns(file_id, path_patterns, columns, data_dir_out))
    return tasks


if __name__ == "__main__":
    luigi.run()
