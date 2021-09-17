import luigi

from etl.workflow.loader import CreateFksAndIndexes
from etl.jobs.load.database_loader import PostgreSQLLoader


class PdcmEtl(luigi.Task):
    """ Executes the ETL process for the specified providers. """
    data_dir = luigi.Parameter()
    data_dir_out = luigi.Parameter()
    providers = luigi.ListParameter()

    def requires(self):
        return CreateFksAndIndexes(self.data_dir_out)


if __name__ == "__main__":
    luigi.run()
