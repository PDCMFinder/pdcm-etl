import luigi

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
