import luigi
from luigi.contrib.spark import PySparkTask
from pyspark.sql import SparkSession
from functools import lru_cache
from pathlib import Path
import yaml

from pyspark.sql.functions import lit, concat, col
from pyspark.sql.types import StructType, StructField, StringType

from etl.constants import Constants
from etl.workflow.config import PdcmConfig


@lru_cache(maxsize=None)
def read_external_resources_conf():
    external_resources_path = \
        "etl/external_resources.yaml" if Path("etl/external_resources.yaml").is_file() else "external_resources.yaml"
    with open(external_resources_path, "r") as ymlFile:
        conf = yaml.safe_load(ymlFile)
    return conf["external_resources"]


class ReadExternalResourcesFromCsv(PySparkTask):
    data_dir = luigi.Parameter()
    data_dir_out = luigi.Parameter()

    def main(self, sc, *args):
        spark = SparkSession(sc)

        input_path = args[0]
        output_path = args[1]

        schema = StructType([
            StructField('entry', StringType(), False),
            StructField('type', StringType(), False),
            StructField('resource', StringType(), False),
            StructField('link', StringType(), False)
        ])

        df = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)
        resources_conf = read_external_resources_conf()

        for resource in resources_conf:
            complete_location = input_path + "/" + resource["file_location"]
            resource_df = spark.read.option('sep', ',').option('header', True).csv(complete_location)
            resource_df = resource_df.withColumn("resource", lit(resource["name"]))
            resource_df = resource_df.withColumn("type", lit(resource["type"]))
            resource_df = resource_df.withColumn("link", concat(lit(resource["url_template"]), col("entry_id")))
            resource_df = resource_df.select(["entry", "type", "resource", "link"])
            df = df.union(resource_df)
        df.write.mode("overwrite").parquet(output_path)

    def output(self):
        return PdcmConfig().get_target(
            "{0}/{1}/{2}".format(self.data_dir_out, Constants.RAW_DIRECTORY, Constants.EXTERNAL_RESOURCES_MODULE))

    def app_options(self):
        return [
            self.data_dir,
            self.output().path]


if __name__ == "__main__":
    luigi.run()
