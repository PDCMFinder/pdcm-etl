import luigi

from etl.workflow.loader import Load


class PdcmEtl(luigi.Task):
    """ Executes the ETL process for the specified providers. """
    data_dir = luigi.Parameter()
    data_dir_out = luigi.Parameter()
    providers = luigi.ListParameter()

    def requires(self):
        return Load(self.data_dir, self.providers, self.data_dir_out)


if __name__ == "__main__":
    luigi.run()
