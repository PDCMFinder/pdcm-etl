import luigi
from luigi.contrib.webhdfs import WebHdfsTarget


class PdcmConfig(luigi.Config):
    deploy_mode = luigi.Parameter(default="client")

    def get_target(self, path):
        return (
            luigi.LocalTarget(path)
            if self.deploy_mode in ["local", "client"]
            else WebHdfsTarget(path)
        )