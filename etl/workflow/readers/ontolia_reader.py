import luigi
from luigi.contrib.spark import PySparkTask
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import *

from etl.constants import Constants
from etl.workflow.config import PdcmConfig


class ReadOntoliaFile(PySparkTask):
    """
        Reads the output file of Ontolia, a service that links regimen to treatments.

        The format of lines in the file is the following:

        NCIT_REGIMEN1=NCIT_TREATMENT1,NCIT_TREATMENT2,NCIT_TREATMENT1=3
    """
    data_dir = luigi.Parameter()
    data_dir_out = luigi.Parameter()

    def app_options(self):
        return [self.data_dir, self.output().path]

    def main(self, sc, *args):
        spark = SparkSession(sc)

        input_path = args[0]
        output_path = args[1]

        df = read_ontolia_file(spark, input_path + "/ontology/ontolia_output.txt")
        df.write.mode("overwrite").parquet(output_path)

    def output(self):
        return PdcmConfig().get_target(
            "{0}/{1}/{2}".format(self.data_dir_out, Constants.RAW_DIRECTORY, Constants.REGIMENT_TO_TREATMENT))


def read_ontolia_file(spark_session, file_path):
    """
        Reads the output file of Ontolia, a service that links regimen to treatments.
        Creates a dataframe with the format regimen (regimen ontology term) and treatments (a list of treatment
        ontology terms)
    """
    data_list = []

    if spark_session.sparkContext.master != "yarn":
        with open(file_path) as fp:
            lines = fp.readlines()
    else:
        rdd = spark_session.sparkContext.textFile(file_path)
        lines = rdd.collect()
    for line in lines:
        line = line.strip()
        data = line.split("=")
        regimen_ontolgy_term = data[0].strip()
        treatments_ontology_terms = data[1].strip()
        treatments_ontology_terms = ','.join(list(map(lambda x: x.strip(), treatments_ontology_terms.split(','))))
        data_list.append((regimen_ontolgy_term, treatments_ontology_terms))

    schema = schema = StructType([
        StructField("regimen", StringType(), False),
        StructField("treatments", StringType(), False)])

    df = spark_session.createDataFrame(data=data_list, schema=schema)

    return df


if __name__ == "__main__":
    luigi.run()
