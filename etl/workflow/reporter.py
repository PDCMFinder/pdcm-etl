import luigi
from luigi.contrib.spark import PySparkTask
from pyspark import SparkContext, SparkConf
from pyspark.sql import DataFrame, SparkSession

from etl.constants import Constants
from etl.jobs.load.database_manager import execute_report_procedure
from etl.workflow.config import PdcmConfig
from etl.workflow.loader import CreateFksAndIndexes


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
        print("out", "{0}/{1}/{2}".format(self.data_dir_out, Constants.REPORTS_DIRECTORY, "analysis_executed"))
        return PdcmConfig().get_target(
            "{0}/{1}/{2}".format(self.data_dir_out, Constants.REPORTS_DIRECTORY, "analysis_executed"))

    def requires(self):
        return CreateFksAndIndexes(self.data_dir_out)

    def run(self):
        list_providers = list(self.providers)
        print("Providers:", self.providers)
        result = execute_report_procedure(self.db_host, self.db_port, self.db_name, self.db_user, self.db_password)
        with self.output().open('w') as outfile:
            print("type", type(outfile))
            outfile.write("Providers:{0}\n".format(list_providers))
            outfile.write(result)


class GenerateReport(PySparkTask):
    data_dir = luigi.Parameter()
    providers = luigi.ListParameter()
    data_dir_out = luigi.Parameter()
    db_host = luigi.Parameter()
    db_port = luigi.Parameter()
    db_name = luigi.Parameter()
    db_user = luigi.Parameter()
    db_password = luigi.Parameter()

    def output(self):
        return PdcmConfig().get_target("{0}/{1}/{2}".format(self.data_dir_out, Constants.REPORTS_DIRECTORY, "report"))

    def requires(self):
        return ExecuteAnalysis()

    def app_options(self):
        return [
            self.db_host,
            self.db_port,
            self.db_name,
            self.db_user,
            self.db_password,
            self.output().path]

    def main(self, sc: SparkContext, *args):
        spark = SparkSession(sc)

        db_host = args[0]
        db_port = args[1]
        db_name = args[2]
        db_user = args[3]
        db_password = args[4]
        output_path = args[5]

        properties = {
            "user": db_user,
            "password": db_password,
            "stringtype": "unspecified",
            "driver": "org.postgresql.Driver",
        }
        jdbc_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"

        df = spark.read.jdbc(jdbc_url,"report", properties=properties)

        records_by_table_df = df.where("report_type = 'records_by_table'").orderBy(["report_value"])
        records_by_table_df.show()
        records_by_table_df.write.json(output_path + 'data.json')


if __name__ == "__main__":
    luigi.run()
