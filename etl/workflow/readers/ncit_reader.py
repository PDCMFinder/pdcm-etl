import luigi
from luigi.contrib.spark import PySparkTask
from pyspark import SparkConf
from pyspark.sql import SparkSession

from etl.constants import Constants
from etl.workflow.config import PdcmConfig


def read_obo_file(session, file_path, columns):
    term_id = ""
    term_name = ""
    term_url = ""
    term_is_a = []

    term_list = []

    if session.sparkContext.master != "yarn":
        with open(file_path) as fp:
            lines = fp.readlines()
    else:
        rdd = session.sparkContext.textFile(file_path)
        lines = rdd.collect()
    for line in lines:
        if line.strip() == "[Term]":
            # check if the term is initialised and if so, add it to ontology_terms
            if term_id != "":
                term_list.append((term_id, term_name, term_url, ','.join(term_is_a)))
                # reset term attributes
                term_id = ""
                term_name = ""
                term_url = ""
                term_is_a = []

        elif line.startswith("id:"):
            term_id = line[4:].strip()
            term_url = "http://purl.obolibrary.org/obo/" + term_id.replace(":", "_")

        elif line.startswith("name:"):
            term_name = line[5:].strip()

        elif line.startswith("is_a:"):
            start = "is_a:"
            end = "!"
            is_a_id = line[line.find(start) + len(start):line.rfind(end)].strip()
            term_is_a.append(is_a_id)

    df = session.createDataFrame(data=term_list, schema=columns)

    return df


class ReadOntologyFromObo(PySparkTask):
    data_dir = luigi.Parameter()
    data_dir_out = luigi.Parameter()

    def setup(self, conf: SparkConf):
        conf.set("spark.kryoserializer.buffer.max", "128m")

    def main(self, sc, *args):
        spark = SparkSession(sc)

        input_path = args[0]
        output_path = args[1]

        columns = ["term_id", "term_name", "term_url", "is_a"]
        df = read_obo_file(spark, input_path + "/ontology/ncit.obo", columns)
        df.write.mode("overwrite").parquet(output_path)

    def output(self):
        return PdcmConfig().get_target(
            "{0}/{1}/{2}".format(self.data_dir_out, Constants.RAW_DIRECTORY, Constants.ONTOLOGY_MODULE))

    def app_options(self):
        return [
            self.data_dir,
            self.output().path]
