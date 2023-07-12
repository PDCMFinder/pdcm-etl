import luigi
from luigi.contrib.spark import PySparkTask
from pyspark.sql import SparkSession
from functools import lru_cache
from pathlib import Path
import yaml

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from etl.constants import Constants
from etl.workflow.config import PdcmConfig


@lru_cache(maxsize=None)
def read_model_characterizations_conf_file():
    model_characterizations_conf_path = \
        "etl/model_characterizations.yaml" if Path("etl/model_characterizations.yaml").is_file() \
        else "model_characterizations.yaml"
    with open(model_characterizations_conf_path, "r") as ymlFile:
        conf = yaml.safe_load(ymlFile)
    return conf


class ReadModelCharacterizationsConf(PySparkTask):
    data_dir = luigi.Parameter()
    data_dir_out = luigi.Parameter()
    module_name = luigi.Parameter()

    def main(self, sc, *args):
        spark = SparkSession(sc)

        output_path = args[0]

        schema = StructType([
            StructField('id', IntegerType(), False),
            StructField('name', StringType(), False),
            StructField('description', StringType(), False),
            StructField('applies_on', StringType(), False),
            StructField('score_name', StringType(), False),
            StructField('calculation_method', StringType(), False)
        ])
        resources = read_model_characterizations_conf_file()["model_characterizations"]
        df = spark.createDataFrame(data=resources, schema=schema)
        df.write.mode("overwrite").parquet(output_path)

    def output(self):
        return PdcmConfig().get_target(
            "{0}/{1}/{2}".format(self.data_dir_out, Constants.RAW_DIRECTORY, self.module_name))

    def app_options(self):
        return [self.output().path]


if __name__ == "__main__":
    luigi.run()
