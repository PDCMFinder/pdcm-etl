import glob

import luigi
from luigi.contrib.spark import PySparkTask
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType, StructField, StringType

from etl.constants import Constants
from etl.jobs.util.file_manager import get_not_empty_files
from etl.source_files_conf_reader import read_groups

ROOT_FOLDER = "data/UPDOG"


def get_data_dir_path(data_dir: str, provider: str, ):
    return "{0}/{1}/{2}".format(data_dir, ROOT_FOLDER, provider)


def get_paths(data_dir, providers, file_pattern):
    data_dir_root = "{0}/{1}".format(data_dir, ROOT_FOLDER)
    filesList = []
    for provider in providers:
        pattern = "{0}/{1}/{2}".format(data_dir_root, provider, file_pattern)
        filesList += (glob.glob(pattern))
    return filesList


def build_schema_from_cols(columns):
    schema = []
    for column in columns:
        schema.append(StructField(column, StringType(), True))
    return StructType(schema)


def get_datasource_from_path(path: str):
    start_idx = path.index(ROOT_FOLDER) + len(ROOT_FOLDER) + 1
    end_idx = path.index("/", start_idx)
    return path[start_idx:end_idx]


def select_rows_with_data(df: DataFrame, columns) -> DataFrame:
    if "Field" in df.columns:
        df = df.select(columns).where("nvl(field, '') not like '#%'")
    else:
        df = df.select(columns)
    return df


def read_with_columns(session, path, schema):
    data_source = get_datasource_from_path(path)
    df = session.read.option('sep', '\t').option('header', True).option('schema', schema).csv(path)
    df = select_rows_with_data(df, schema.fieldNames())
    # Add a data_source column that makes it easy to identify the provider in the modules
    df = df.withColumn(Constants.DATA_SOURCE_COLUMN, lit(data_source))
    return df


class ReadWithSpark(PySparkTask):
    file_id = luigi.Parameter()
    files_paths = luigi.ListParameter()
    columns_to_read = luigi.ListParameter()
    data_dir_out = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget("{0}/{1}/{2}".format(self.data_dir_out, Constants.RAW_DIRECTORY, self.file_id))

    def app_options(self):
        return [
            ','.join([p for p in self.files_paths]),
            ','.join(self.columns_to_read),
            self.output().path]

    def main(self, sc, *args):
        spark = SparkSession(sc)

        input_paths = args[0].split(',')
        columns_to_read = args[1].split(',')
        output_path = args[2]

        # Only work with paths that have data. Some files are empty because they are optional so we create them emtpy
        # so the task that checks their existence does not fail
        non_empty_paths = []
        if input_paths != ['']:
            non_empty_paths = get_not_empty_files(input_paths)

        schema = build_schema_from_cols(columns_to_read)

        if len(non_empty_paths) > 0:
            streams = read_with_columns(spark, non_empty_paths[0], schema)
            for stream_path in non_empty_paths[1:]:
                streams = streams.union(read_with_columns(spark, stream_path, schema))
        else:
            empty_df = spark.createDataFrame(sc.emptyRDD(), schema)
            streams = empty_df
            streams = streams.withColumn(Constants.DATA_SOURCE_COLUMN, lit(""))
        streams.write.mode("overwrite").parquet(output_path)


def get_tasks_to_run(data_dir, providers, data_dir_out):
    tasks = []
    groups = read_groups()
    for group in groups:
        skip = group.get("skip")
        if skip is None or not skip:
            for file in group["files"]:
                file_id = file["id"]
                filePattern = file["name_pattern"]
                columns = file["columns"]

                paths = get_paths(data_dir, list(providers), filePattern)
                tasks.append(ReadWithSpark(file_id, paths, columns, data_dir_out))
    return tasks


class Extract(luigi.WrapperTask):
    data_dir = luigi.Parameter()
    providers = luigi.ListParameter()
    data_dir_out = luigi.Parameter()

    def requires(self):
        tasks = get_tasks_to_run(self.data_dir, self.providers, self.data_dir_out)
        yield tasks


class ExtractFile(luigi.Task):
    data_dir = luigi.Parameter()
    providers = luigi.ListParameter()
    data_dir_out = luigi.Parameter()
    file_id = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget("{0}/{1}/{2}".format(self.data_dir_out, Constants.RAW_DIRECTORY, self.file_id))

    def requires(self):
        return Extract(self.data_dir, self.providers, self.data_dir_out)


class ExtractPatient(ExtractFile):
    file_id = Constants.PATIENT_MODULE


class ExtractSample(ExtractFile):
    file_id = Constants.SAMPLE_MODULE


class ExtractSharing(ExtractFile):
    file_id = Constants.SHARING_MODULE


class ExtractLoader(ExtractFile):
    file_id = Constants.LOADER_MODULE


class ExtractModel(ExtractFile):
    file_id = Constants.MODEL_MODULE


class ExtractModelValidation(ExtractFile):
    file_id = Constants.MODEL_VALIDATION_MODULE


class ExtractSamplePlatform(ExtractFile):
    file_id = Constants.SAMPLE_PLATFORM_MODULE


class ExtractDrugDosing(ExtractFile):
    file_id = Constants.DRUG_DOSING_MODULE


class ExtractPatientTreatment(ExtractFile):
    file_id = Constants.PATIENT_TREATMENT_MODULE


class ExtractCna(ExtractFile):
    file_id = Constants.CNA_MODULE


if __name__ == "__main__":
    luigi.run()
