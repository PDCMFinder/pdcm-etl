import luigi
from luigi.contrib.spark import PySparkTask
from pyspark.sql import SparkSession
from functools import lru_cache
from pathlib import Path
import yaml

from pyspark.sql.functions import lit, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from etl.constants import Constants
from etl.workflow.config import PdcmConfig


@lru_cache(maxsize=None)
def read_resources_conf_file():
    external_resources_path = \
        "etl/external_resources.yaml" if Path("etl/external_resources.yaml").is_file() else "external_resources.yaml"
    with open(external_resources_path, "r") as ymlFile:
        conf = yaml.safe_load(ymlFile)
    return conf


class ReadResources(PySparkTask):
    data_dir = luigi.Parameter()
    data_dir_out = luigi.Parameter()
    module_name = luigi.Parameter()

    def main(self, sc, *args):
        spark = SparkSession(sc)

        output_path = args[0]

        schema = StructType([
            StructField('id', IntegerType(), False),
            StructField('name', StringType(), False),
            StructField('label', StringType(), False),
            StructField('type', StringType(), False),
            StructField('link_building_method', StringType(), False),
            StructField('link_template', StringType(), False)
        ])
        resources = read_resources_conf_file()["resources"]
        df = spark.createDataFrame(data=resources, schema=schema)
        df.write.mode("overwrite").parquet(output_path)

    def output(self):
        return PdcmConfig().get_target(
            "{0}/{1}/{2}".format(self.data_dir_out, Constants.RAW_DIRECTORY, self.module_name))

    def app_options(self):
        return [self.output().path]


class ReadDownloadedExternalResourcesFromCsv(PySparkTask):
    data_dir = luigi.Parameter()
    data_dir_out = luigi.Parameter()
    module_name = luigi.Parameter()

    def main(self, sc, *args):
        spark = SparkSession(sc)

        input_path = args[0]
        external_resources_parquet_file_path = args[1]
        output_path = args[2]

        downloaded_resources_information = read_resources_conf_file()["resources_download_conf"]
        downloaded_resources_information_df = spark.createDataFrame(data=downloaded_resources_information)

        schema = StructType([
            StructField('entry', StringType(), False),
            StructField('type', StringType(), False),
            StructField('resource', StringType(), False),
            StructField('link', StringType(), False)
        ])

        raw_external_resources_df = spark.read.parquet(external_resources_parquet_file_path)

        downloaded_resources_information_df = downloaded_resources_information_df.withColumnRenamed("resource_id", "id")

        resources_with_csv_paths = downloaded_resources_information_df.join(
            raw_external_resources_df, on=["id"], how='left')

        df = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)

        rows = [row.asDict() for row in resources_with_csv_paths.collect()]
        for row in rows:
            complete_location = input_path + "/externalDBs/" + row["processed_file"]
            resource_df = spark.read.option('sep', ',').option('header', True).csv(complete_location)
            resource_df = resource_df.withColumn("resource", lit(row["label"]))
            resource_df = resource_df.withColumn("type", lit(row["type"]))
            resource_df = resource_df.withColumn("link", lit(row["link_template"]))
            resource_df = resource_df.withColumn("link", expr("regexp_replace(link, 'ENTRY_ID', entry_id)"))
            resource_df = resource_df.select(["entry", "type", "resource", "link"])
            df = df.union(resource_df)

        df.write.mode("overwrite").parquet(output_path)
        df.show(truncate=False)

    def output(self):
        return PdcmConfig().get_target(
            "{0}/{1}/{2}".format(self.data_dir_out, Constants.RAW_DIRECTORY, self.module_name))

    def app_options(self):
        return [
            self.data_dir,
            self.input().path,
            self.output().path]


if __name__ == "__main__":
    luigi.run()
