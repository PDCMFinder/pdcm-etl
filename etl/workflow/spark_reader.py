import glob
import json
import time
import yaml

import luigi
from luigi.contrib.spark import PySparkTask
from py4j.protocol import Py4JJavaError
from pyspark import SparkContext, SparkConf
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit, input_file_name, regexp_extract
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.utils import IllegalArgumentException

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


def read_obo_file(session, file_path, columns):
    start = time.time()

    term_id = ""
    term_name = ""
    term_url = ""
    term_is_a = []

    term_list = []
    # graph = nx.DiGraph()

    if session.sparkContext.master != "yarn":
        with open(file_path) as fp:
            lines = fp.readlines()
    else:
        rdd = session.sparkContext.textFile(file_path)
        lines = rdd.collect()
    for line in lines:
        if line.strip() == "[Term]":
            # check if the term is initialised and if so, add it to ontology_terms
            if term_id != "":
                # graph.add_node(term_id, name=term_name, term_id=term_id)
                term_list.append((term_id, term_name, term_url, ','.join(term_is_a)))
                # reset term attributes
                term_id = ""
                term_name = ""
                term_url = ""
                term_is_a = []

        elif line.startswith("id:"):
            term_id = line[4:].strip()
            term_url = "http://purl.obolibrary.org/obo/" + term_id.replace(":", "_")

        elif line.startswith("name:"):
            term_name = line[5:].strip()

        elif line.startswith("is_a:"):
            start = "is_a:"
            end = "!"
            is_a_id = line[line.find(start) + len(start):line.rfind(end)].strip()
            # graph.add_edge(is_a_id, term_id)
            term_is_a.append(is_a_id)

    # return graph

    print("Num of terms:" + str(len(term_list)))
    df = session.createDataFrame(data=term_list, schema=columns)
    end = time.time()

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

    def main(self, sc: SparkContext, *args):
        spark = SparkSession(sc)

        path_patterns = args[0].split('|')
        columns_to_read = args[1].split(',')
        output_path = args[2]

        schema = build_schema_from_cols(columns_to_read)

        if sc.master == "yarn":
            hadoop = sc._jvm.org.apache.hadoop
            fs = hadoop.fs.FileSystem
            current_fs = fs.get(sc._jsc.hadoopConfiguration())
            path_patterns = [path for path in path_patterns if
                             path != "" and current_fs.globStatus(hadoop.fs.Path(path))]
        try:
            df = read_files(spark, path_patterns, schema)
        except (Py4JJavaError, IllegalArgumentException) as error:
            no_empty_patterns = list(filter(lambda x: x != '', path_patterns))
            if "java.io.FileNotFoundException" in str(error) or len(no_empty_patterns) == 0:
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
            if glob.glob("{0}/{1}/{2}".format(data_dir_root, provider,
                                              current_file_pattern)) or PdcmConfig().deploy_mode == "cluster":
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
    path_pattern = "{0}/{1}/{2}".format(data_dir_root, provider, file_pattern.replace("$provider", provider))
    return path_pattern


def get_tsv_extraction_task_by_module(data_dir, providers, data_dir_out, module_name):
    module = read_module(module_name)
    file_patterns = module["name_patterns"]
    columns = module["columns"]
    path_patterns = build_path_patterns(data_dir, list(providers), file_patterns)
    return ReadByModuleAndPathPatterns(module_name, path_patterns, columns, data_dir_out)


def extract_provider_name(path: str):
    init_index = path.index(ROOT_FOLDER) + len(ROOT_FOLDER) + 1
    next_slash = path.index("/", init_index)
    return path[init_index:next_slash]


def get_json_by_yaml(yaml_content):
    yaml_as_json = yaml.safe_load(yaml_content)
    yaml_as_json = json.dumps(yaml_as_json)
    yaml_as_json = yaml_as_json.encode("unicode_escape").decode("utf-8")
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
            PdcmConfig().deploy_mode,
            self.output().path]

    def main(self, sc, *args):
        spark = SparkSession(sc)

        yaml_file_paths = args[0].split(',')
        columns_to_read = args[1].split(',')
        deploy_mode = args[2]
        output_path = args[3]

        all_json_and_providers = []

        if deploy_mode == "cluster":
            for yaml_file_path in yaml_file_paths:
                yaml_as_json = sc.wholeTextFiles(yaml_file_path).collect()[0][1]
                yaml_as_json = get_json_by_yaml(yaml_as_json)
                json_content_and_provider = (yaml_as_json, extract_provider_name(yaml_file_path))
                all_json_and_providers.append(json_content_and_provider)
        else:
            for yaml_file_path in yaml_file_paths:
                with open(yaml_file_path, 'r') as stream:
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
    columns = ["hgnc_id", "approved_symbol", "approved_name", "status", "previous_symbols", "alias_symbols",
               "accession_numbers", "refseq_ids", "alias_names", "ensembl_gene_id", "ncbi_gene_id"]
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
        df = read_files(spark, input_path + "/markers/markers.tsv", build_schema_from_cols(columns))
        df.write.mode("overwrite").parquet(output_path)

    def output(self):
        return PdcmConfig().get_target(
            "{0}/{1}/{2}".format(self.data_dir_out, Constants.RAW_DIRECTORY, Constants.GENE_MARKER_MODULE))

    def app_options(self):
        return [
            self.data_dir,
            self.output().path]


class ReadOntologyFromObo(PySparkTask):
    data_dir = luigi.Parameter()
    data_dir_out = luigi.Parameter()

    def setup(self, conf: SparkConf):
        conf.set("spark.kryoserializer.buffer.max", "128m")

    def main(self, sc, *args):
        spark = SparkSession(sc)

        input_path = args[0]
        output_path = args[1]

        columns = ["term_id", "term_name", "term_url", "is_a"]
        df = read_obo_file(spark, input_path + "/ontology/ncit.obo", columns)
        df.show()
        df.write.mode("overwrite").parquet(output_path)

    def output(self):
        return PdcmConfig().get_target(
            "{0}/{1}/{2}".format(self.data_dir_out, Constants.RAW_DIRECTORY, Constants.ONTOLOGY_MODULE))

    def app_options(self):
        return [
            self.data_dir,
            self.output().path]


class ReadDiagnosisMappingsFromJson(PySparkTask):
    data_dir = luigi.Parameter()
    data_dir_out = luigi.Parameter()

    def main(self, sc, *args):
        spark = SparkSession(sc)

        input_path = args[0]
        output_path = args[1]

        columns = ["datasource", "diagnosis", "primary_tissue", "tumor_type", "mapped_term_url", "justification",
                   "map_type"]
        df = read_diagnosis_mapping_file(spark, input_path, columns)
        df.show()
        df.write.mode("overwrite").parquet(output_path)

    def output(self):
        return PdcmConfig().get_target(
            "{0}/{1}/{2}".format(self.data_dir_out, Constants.RAW_DIRECTORY, Constants.MAPPING_DIAGNOSIS_MODULE))

    def app_options(self):
        return [
            self.data_dir,
            self.output().path]


def read_diagnosis_mapping_file(session, input_path, columns):
    if session.sparkContext.master != "yarn":
        with open(input_path + "/mapping/diagnosis_mappings.json", 'r') as jsonfile:
            data = jsonfile.read()
    else:
        rdd = session.sparkContext.textFile(input_path + "/mapping/diagnosis_mappings.json")
        data = rdd.collect()[0]
    obj = json.loads(data)
    data_rows = []
    for entity in obj['mappings']:
        datasource = entity['mappingValues']['DataSource']
        diagnosis = entity['mappingValues']['SampleDiagnosis']
        primary_tissue = entity['mappingValues']['OriginTissue']
        tumor_type = entity['mappingValues']['TumorType']
        mapped_term_url = entity['mappedTermUrl']
        justification = entity['justification']
        map_type = entity['mapType']
        data_rows.append((datasource, diagnosis, primary_tissue, tumor_type, mapped_term_url, justification, map_type))

    df = session.createDataFrame(data=data_rows, schema=columns)
    return df


if __name__ == "__main__":
    luigi.run()