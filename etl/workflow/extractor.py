import luigi
from luigi.contrib.spark import PySparkTask
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

from etl.constants import Constants


class CheckMetaDataExistForModuleAndProvider(luigi.Task):
    """ Task to require the existence of a metadata file for a module and provider. """
    data_dir = luigi.Parameter()
    module_name = luigi.Parameter()
    provider = luigi.Parameter()

    def output(self):
        file_name = "{0}_metadata-{1}.tsv".format(self.provider, self.module_name)
        file_path = "{0}/data/UPDOG/{1}/{2}".format(self.data_dir, self.provider, file_name)
        return luigi.LocalTarget(file_path)


class ExtractModuleMetaDataSpark(PySparkTask):
    """ Spark task that reads the data. """
    providers = luigi.ListParameter()
    data_dir = luigi.Parameter()
    data_dir_out = luigi.Parameter()
    module_name = luigi.Parameter()

    name = "PDX_Extract_Module_MetaData_Spark"

    def output(self):
        return luigi.LocalTarget("{0}/{1}/{2}".format(self.data_dir_out, Constants.RAW_DIRECTORY, self.module_name))

    def requires(self):
        return [
            CheckMetaDataExistForModuleAndProvider(
                self.data_dir,
                self.module_name,
                provider) for provider in self.providers]

    def app_options(self):
        return [','.join([p.path for p in self.input()]),
                self.output().path,
                ','.join(self.column_list)]

    def main(self, sc, *args):
        spark = SparkSession(sc)

        input_paths = args[0].split(',')
        output_path = args[1]
        column_list = args[2].split(',')
        streams = read_with_columns(spark, input_paths[0], column_list)
        for stream_path in input_paths[1:]:
            streams = streams.union(read_with_columns(spark, stream_path, column_list))
        streams.write.mode("overwrite").parquet(output_path)


def read_with_columns(session, path, columns):
    split = path.split('/')
    data_source = split[-2]
    df = session.read.option('sep', '\t').option('header', True).csv(path)
    df = df.select(columns).where("Field is null")
    # Add a data_source column that makes it easy to identify the provider in the modules
    df = df.withColumn(Constants.DATA_SOURCE_COLUMN, lit(data_source))
    return df


class ExtractPatientModuleSpark(ExtractModuleMetaDataSpark):
    module_name = "patient"
    column_list = [
        "patient_id",
        "sex",
        "history",
        "ethnicity",
        "ethnicity_assessment_method",
        "initial_diagnosis",
        "age_at_initial_diagnosis"]


class ExtractSampleModuleSpark(ExtractModuleMetaDataSpark):
    module_name = "sample"
    column_list = [
        "patient_id",
        "sample_id",
        "collection_date",
        "collection_event",
        "months_since_collection_1",
        "age_in_years_at_collection",
        "diagnosis",
        "tumour_type",
        "primary_site",
        "collection_site",
        "stage",
        "staging_system",
        "grade",
        "grading_system",
        "virology_status",
        "sharable",
        "treatment_naive_at_collection",
        "treated",
        "prior_treatment",
        "model_id"
    ]


class ExtractSharingModuleSpark(ExtractModuleMetaDataSpark):
    module_name = "sharing"
    column_list = [
        "model_id",
        "provider_type",
        "accessibility",
        "europdx_access_modality",
        "email",
        "name",
        "form_url",
        "database_url",
        "provider_name",
        "provider_abbreviation",
        "project"
        ]


class ExtractLoaderModuleSpark(ExtractModuleMetaDataSpark):
    module_name = "loader"
    column_list = [
        "name",
        "abbreviation",
        "internal_url",
        "internal_dosing_url"
        ]


class ExtractModelModuleSpark(ExtractModuleMetaDataSpark):
    module_name = "model"
    column_list = [
        "model_id",
        "host_strain",
        "host_strain_full",
        "engraftment_site",
        "engraftment_type",
        "sample_type",
        "sample_state",
        "passage_number",
        "publications"
        ]


if __name__ == "__main__":
    luigi.run()
