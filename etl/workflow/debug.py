import luigi
from luigi.contrib.spark import PySparkTask
from pyspark.sql import SparkSession

from etl.jobs.transformation.links_generation.molecular_data_links_builder import add_links_in_molecular_data_table
from etl.workflow.extractor import ExtractExternalResources, ExtractDownloadedResourcesData
from etl.workflow.transformer import TransformCnaMolecularData


# Useful to debug methods without the need to compute the dataframe from scratch but to
# use already existing parquets. Mainly to be used in the cluster where changes to code are
# more cumbersome
class DebugTask(PySparkTask):
    data_dir = luigi.Parameter()
    data_dir_out = luigi.Parameter()

    def requires(self):
        return [
            TransformCnaMolecularData(),
            ExtractExternalResources(),
            ExtractDownloadedResourcesData()]

    def app_options(self):
        return [
            self.input()[0].path,
            self.input()[1].path,
            self.input()[2].path,
            self.data_dir,
            self.output()]

    def main(self, sc, *args):
        spark = SparkSession(sc)

        cna_parquet_path = args[0]
        resources_parquet_path = args[1]
        resources_data_parquet_path = args[2]
        input_path = args[3]
        output_path = args[4]

        cna_df = spark.read.parquet(cna_parquet_path)
        limit = 10000
        print("Taking only ", limit, "rows")
        cna_df = cna_df.limit(limit)
        cna_df = cna_df.drop("external_db_links")
        resources_df = spark.read.parquet(resources_parquet_path)
        resources_data_df = spark.read.parquet(resources_data_parquet_path)

        df = add_links_in_molecular_data_table(cna_df, resources_df, resources_data_df)
        print("result")
        df.show()


if __name__ == "__main__":
    luigi.run()
