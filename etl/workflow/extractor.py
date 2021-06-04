import luigi
from luigi.contrib.spark import PySparkTask
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType

from etl.constants import Constants
from etl.jobs.util.file_manager import get_not_empty_files
from etl.modules_schema import ModulesSchema


class CheckMetaDataExistForModuleAndProvider(luigi.Task):
    """ Task to require the existence of a metadata file for a module and provider. """
    data_dir = luigi.Parameter()
    module_name = luigi.Parameter()
    provider = luigi.Parameter()

    def output(self):
        file_name = "{0}_metadata-{1}.tsv".format(self.provider, self.module_name)
        file_path = "{0}/data/UPDOG/{1}/{2}".format(self.data_dir, self.provider, file_name)
        return luigi.LocalTarget(file_path)


class CheckOptionalModuleFileExists(luigi.Task):
    """ Task to require the existence of an optional file for a module and provider.
     Creates the file if it doesn't exist
     """
    data_dir = luigi.Parameter()
    module_name = luigi.Parameter()
    provider = luigi.Parameter()

    def output(self):
        file_name = f"{self.provider}_{self.module_name}.tsv"
        file_path = f"{self.data_dir}/data/UPDOG/{self.provider}/{file_name}"
        return luigi.LocalTarget(file_path)

    def run(self):
        if not self.output().exists():
            print(f"File {self.output().path} does not exist")
            f = self.output().open('w')
            f.close()


class ExtractModuleMetaDataSpark(PySparkTask):
    """ Spark task that reads the data. """
    providers = luigi.ListParameter()
    data_dir = luigi.Parameter()
    data_dir_out = luigi.Parameter()
    module_name = luigi.Parameter()

    is_file_optional = False

    name = "PDX_Extract_Module_MetaData_Spark"

    def output(self):
        return luigi.LocalTarget("{0}/{1}/{2}".format(self.data_dir_out, Constants.RAW_DIRECTORY, self.module_name))

    def requires(self):
        if self.is_file_optional:
            return [
                CheckOptionalModuleFileExists(
                    self.data_dir,
                    self.module_name,
                    provider) for provider in self.providers]
        else:
            return [
                CheckMetaDataExistForModuleAndProvider(
                    self.data_dir,
                    self.module_name,
                    provider) for provider in self.providers]

    def app_options(self):
        return [
                self.module_name,
                ','.join([p.path for p in self.input()]),
                self.output().path]

    def main(self, sc, *args):
        spark = SparkSession(sc)

        module_name = args[0]
        input_paths = args[1].split(',')
        output_path = args[2]

        # Only work with paths that have data. Some files are empty because they are optional so we create them emtpy
        # so the task that checks their existence does not fail
        input_paths = get_not_empty_files(input_paths)
        schema_by_module = ModulesSchema.MODULES_SCHEMAS[module_name]
        schema = StructType(schema_by_module)

        if len(input_paths) > 0:
            streams = read_with_columns(spark, input_paths[0], schema)
            for stream_path in input_paths[1:]:
                streams = streams.union(read_with_columns(spark, stream_path, schema))
        else:
            print(f"Create empty df for {module_name}")
            empty_df = spark.createDataFrame(sc.emptyRDD(), schema)
            streams = empty_df
        streams.write.mode("overwrite").parquet(output_path)


def read_with_columns(session, path, schema):
    split = path.split('/')
    data_source = split[-2]
    df = session.read.option('sep', '\t').option('header', True).option('schema', schema).csv(path)
    df = df.select(schema.fieldNames()).where("Field is null")
    # Add a data_source column that makes it easy to identify the provider in the modules
    df = df.withColumn(Constants.DATA_SOURCE_COLUMN, lit(data_source))
    return df


class ExtractPatientModuleSpark(ExtractModuleMetaDataSpark):
    module_name = Constants.PATIENT_MODULE


class ExtractSampleModuleSpark(ExtractModuleMetaDataSpark):
    module_name = Constants.SAMPLE_MODULE


class ExtractSharingModuleSpark(ExtractModuleMetaDataSpark):
    module_name = Constants.SHARING_MODULE


class ExtractLoaderModuleSpark(ExtractModuleMetaDataSpark):
    module_name = Constants.LOADER_MODULE


class ExtractModelModuleSpark(ExtractModuleMetaDataSpark):
    module_name = Constants.MODEL_MODULE


class ExtractModelModuleValidationSpark(ExtractModuleMetaDataSpark):
    module_name = Constants.MODEL_VALIDATION_MODULE


class ExtractSamplePlatformModuleSpark(ExtractModuleMetaDataSpark):
    module_name = Constants.PLATFORM_SAMPLE_MODULE
    is_file_optional = True


if __name__ == "__main__":
    luigi.run()
