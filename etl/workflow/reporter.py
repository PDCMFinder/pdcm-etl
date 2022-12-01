from datetime import datetime

import luigi
from luigi.contrib.spark import PySparkTask
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType, ArrayType

from etl.constants import Constants
from etl.jobs.load.database_manager import execute_report_procedure, copy_entity_to_database
from etl.jobs.util.dataframe_functions import flatten_array_columns
from etl.workflow.config import PdcmConfig
from etl.workflow.loader import CreateFksAndIndexes


class WriteReleaseInfoCsv(PySparkTask):
    """
        Generate data for release_info.
    """
    providers = luigi.ListParameter()
    data_dir_out = luigi.Parameter()
    env = luigi.Parameter()

    def requires(self):
        return CreateFksAndIndexes()

    def output(self):
        return PdcmConfig().get_target(
            "{0}/{1}/{2}".format(self.data_dir_out, "database_formatted", "release_info"))

    def main(self, sc, *args):
        spark = SparkSession(sc)

        schema = StructType([
            StructField('name', StringType(), True),
            StructField('date', DateType(), True),
            StructField('providers', ArrayType(StringType()), True)
        ])

        # Calculating the name of the release
        if self.env == "local":
            name = "local release"
        else:
            path = str(self.data_dir_out)
            name = path.rsplit('/', 1)[1]

        date = datetime.now()

        data = [(name, date, self.providers)]

        df = spark.createDataFrame(data=data, schema=schema)
        df = flatten_array_columns(df)

        df.coalesce(1).write.option("sep", "\t").option("quote", "\u0000").option(
            "header", "true"
        ).mode("overwrite").csv(self.output().path)


class LoadReleaseInfo(luigi.Task):
    db_host = luigi.Parameter()
    db_port = luigi.Parameter()
    db_name = luigi.Parameter()
    db_user = luigi.Parameter()
    db_password = luigi.Parameter()
    data_dir_out = luigi.Parameter()
    env = luigi.Parameter()
    """
        Write data in release_info.
    """

    def requires(self):
        return WriteReleaseInfoCsv()

    def output(self):
        return PdcmConfig().get_target(
            "{0}/{1}_{2}/{3}".format(self.data_dir_out, "database", self.env, Constants.RELEASE_INFO_ENTITY))

    def run(self):

        copy_entity_to_database(
            Constants.RELEASE_INFO_ENTITY, self.input().path, self.db_host, self.db_port, self.db_name,
            self.db_user, self.db_password)

        with self.output().open('w') as outfile:
            outfile.write("Entity {0} copied".format(Constants.RELEASE_INFO_ENTITY))


class ExecuteAnalysis(luigi.Task):
    data_dir = luigi.Parameter()
    providers = luigi.ListParameter()
    data_dir_out = luigi.Parameter()
    db_host = luigi.Parameter()
    db_port = luigi.Parameter()
    db_name = luigi.Parameter()
    db_user = luigi.Parameter()
    db_password = luigi.Parameter()

    def output(self):
        return PdcmConfig().get_target(
            "{0}/{1}/{2}".format(self.data_dir_out, Constants.REPORTS_DIRECTORY, "pdcm_tables_report.txt"))

    def requires(self):
        return CreateFksAndIndexes(self.data_dir_out)

    def run(self):
        result = execute_report_procedure(self.db_host, self.db_port, self.db_name, self.db_user, self.db_password)
        with self.output().open('w') as outfile:
            outfile.write("Providers:{0}\n".format(list(self.providers)))
            outfile.write(result)


if __name__ == "__main__":
    luigi.run()
