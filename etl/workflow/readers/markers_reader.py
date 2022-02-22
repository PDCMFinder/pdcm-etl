import luigi
from luigi.contrib.spark import PySparkTask
from pyspark.sql import DataFrame, SparkSession
import csv

from etl.constants import Constants
from etl.workflow.config import PdcmConfig


def extract_markers(input_path):
    markers = []
    with open(input_path + "/markers.tsv") as fp:
        tsv_file = csv.reader(fp, delimiter="\t")
        first_row = True
        for line in tsv_file:
            if first_row:
                first_row = False
            else:
                markers.append(line)
    return markers


def create_marker_dataframe(markers) -> DataFrame:
    spark = SparkSession.builder.getOrCreate()
    columns = ["hgnc_id", "approved_symbol", "approved_name", "status", "previous_symbols", "alias_symbols",
               "accession_numbers", "refseq_ids", "alias_names", "ensembl_gene_id", "ncbi_gene_id"]
    df = spark.createDataFrame(data=markers, schema=columns)
    return df


class ReadMarkerFromTsv(PySparkTask):
    data_dir = luigi.Parameter()
    data_dir_out = luigi.Parameter()

    def main(self, sc, *args):
        spark = SparkSession(sc)

        input_path = args[0]
        output_path = args[1]

        columns = ["hgnc_id", "approved_symbol", "approved_name", "status", "previous_symbols", "alias_symbols",
                   "accession_numbers", "refseq_ids", "alias_names", "ensembl_gene_id", "ncbi_gene_id"]

        df = spark.read.option('sep', '\t').option('header', True).csv(input_path + "/markers/markers.tsv")
        df = df.select(columns)

        df.write.mode("overwrite").parquet(output_path)

    def output(self):
        return PdcmConfig().get_target(
            "{0}/{1}/{2}".format(self.data_dir_out, Constants.RAW_DIRECTORY, Constants.GENE_MARKER_MODULE))

    def app_options(self):
        return [
            self.data_dir,
            self.output().path]
