import glob
import json
import time

import yaml

import luigi
from luigi.contrib.spark import PySparkTask
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit, input_file_name, regexp_extract
from pyspark.sql.types import StructType, StructField, StringType

from etl import logger
from etl.constants import Constants
from etl.jobs.util.cleaner import trim_all_str
from etl.source_files_conf_reader import read_module
from etl.workflow.config import PdcmConfig

import csv

ROOT_FOLDER = "data/UPDOG"


def build_schema_from_cols(columns):
    schema = []
    for column in columns:
        schema.append(StructField(column, StringType(), True))
    return StructType(schema)


def select_rows_with_data(df: DataFrame, columns) -> DataFrame:
    print("columns-->", columns)
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

    datasource_pattern = "{0}\\/([a-zA-Z-]+)(\\/)".format(ROOT_FOLDER.replace("/", "\\/"))
    if regexp_extract("_data_source", datasource_pattern, 1) != "":
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
        matching_providers = []
        for provider in providers:
            current_file_pattern = str(file_pattern).replace("$provider", provider)
            if glob.glob("{0}/{1}/{2}".format(data_dir_root, provider, current_file_pattern)):
                matching_providers.append(provider)

        if matching_providers:
            joined_providers_list = ','.join([p for p in matching_providers])
            providers_pattern = "{" + joined_providers_list + "}"
            path_pattern = "{0}/{1}/{2}".format(
                data_dir_root, providers_pattern, file_pattern.replace("$provider", providers_pattern))
            paths_patterns.append(path_pattern)

    return paths_patterns


def build_path_pattern_by_provider(data_dir, provider, file_pattern):
    data_dir_root = "{0}/{1}".format(data_dir, ROOT_FOLDER)
    print("provider", provider, type(provider))
    pattern = file_pattern.replace("$provider", provider)
    print("pattern", pattern)
    path_pattern = "{0}/{1}/{2}".format(data_dir_root, provider, file_pattern.replace("$provider", provider))
    return path_pattern


def get_tsv_extraction_task_by_module(data_dir, providers, data_dir_out, module_name):
    module = read_module(module_name)
    file_patterns = module["name_patterns"]
    columns = module["columns"]
    path_patterns = build_path_patterns(data_dir, list(providers), file_patterns)
    return ReadByModuleAndPathPatterns(module_name, path_patterns, columns, data_dir_out)


class ReadYamlByModule(PySparkTask):
    raw_folder_name = luigi.Parameter()
    path_pattern = luigi.Parameter()
    columns_to_read = luigi.ListParameter()
    provider = luigi.Parameter()
    data_dir_out = luigi.Parameter()

    def output(self):
        return PdcmConfig().get_target(
            "{0}/{1}/{2}".format(self.data_dir_out, Constants.RAW_DIRECTORY, self.raw_folder_name))

    def app_options(self):
        return [
            self.path_pattern,
            ','.join(self.columns_to_read),
            self.provider,
            self.output().path]

    def main(self, sc, *args):
        spark = SparkSession(sc)

        yaml_file_path = args[0]
        columns_to_read = args[1].split(',')
        provider = args[2]
        output_path = args[3]

        with open(yaml_file_path, 'r') as stream:
            yaml_as_json = yaml.safe_load(stream)
            yaml_as_json = json.dumps(yaml_as_json)

        df = read_json(spark, yaml_as_json)
        df = df.select(columns_to_read)
        df = df.withColumn(Constants.DATA_SOURCE_COLUMN, lit(provider))
        df.write.mode("overwrite").parquet(output_path)


def get_yaml_extraction_task_by_module(data_dir, providers, data_dir_out, module_name):
    module = read_module(module_name)
    file_patterns = module["name_patterns"]
    columns = module["columns"]
    # There should be only one yaml file by module
    file_path = str(file_patterns[0])

    for provider in providers:
        yaml_file_path = build_path_pattern_by_provider(data_dir, provider, file_path)
        return ReadYamlByModule(module_name, yaml_file_path, columns, provider, data_dir_out)


def extract_markers(input_path):
    markers = []
    with open(input_path + "/markers.tsv") as fp:
        tsv_file = csv.reader(fp, delimiter="\t")
        first_row = True
        for line in tsv_file:
            if first_row:
                first_row = False
            else:
                markers.append(line)
    return markers


def create_marker_dataframe(markers) -> DataFrame:
    spark = SparkSession.builder.getOrCreate()
    columns = ["hgnc_id", "approved_symbol", "approved_name", "status", "previous_symbols", "alias_symbols", "accession_numbers", "refseq_ids", "alias_names", "ensembl_gene_id", "ncbi_gene_id"]
    df = spark.createDataFrame(data=markers, schema=columns)
    return df


class ReadMarkerFromTsv(PySparkTask):

    data_dir = luigi.Parameter()
    data_dir_out = luigi.Parameter()

    def main(self, sc, *args):
        spark = SparkSession(sc)

        input_path = args[0]
        output_path = args[1]

        columns = ["hgnc_id", "approved_symbol", "approved_name", "status", "previous_symbols", "alias_symbols",
                   "accession_numbers", "refseq_ids", "alias_names", "ensembl_gene_id", "ncbi_gene_id"]
        df = read_files(spark, input_path+"/markers/markers.tsv", build_schema_from_cols(columns))
        df.write.mode("overwrite").parquet(output_path)

    def output(self):
        return PdcmConfig().get_target(
            "{0}/{1}/{2}".format(self.data_dir_out, Constants.RAW_DIRECTORY, Constants.GENE_MARKER_MODULE))

    def app_options(self):
        return [
            self.data_dir,
            self.output().path]


if __name__ == "__main__":
    luigi.run()
