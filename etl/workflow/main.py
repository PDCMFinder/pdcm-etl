import luigi

from etl.workflow.loader import LoadPublicDBObjects, Cache


class PdcmEtl(luigi.Task):
    """ Executes the ETL process for the specified providers. """
    data_dir = luigi.Parameter()
    data_dir_out = luigi.Parameter()
    providers = luigi.ListParameter()

    def requires(self):
        return [LoadPublicDBObjects(), Cache()]


if __name__ == "__main__":
    luigi.run()
