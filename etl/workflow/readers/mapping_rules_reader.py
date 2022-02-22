import luigi
import json
from luigi.contrib.spark import PySparkTask
from pyspark.sql import SparkSession

from etl.constants import Constants
from etl.workflow.config import PdcmConfig


class ReadDiagnosisMappingsFromJson(PySparkTask):
    data_dir = luigi.Parameter()
    data_dir_out = luigi.Parameter()

    def main(self, sc, *args):
        spark = SparkSession(sc)

        input_path = args[0]
        output_path = args[1]

        columns = ["datasource", "diagnosis", "primary_tissue", "tumor_type", "mapped_term_url", "justification",
                   "map_type"]
        df = read_diagnosis_mapping_file(spark, input_path, columns)
        df.show()
        df.write.mode("overwrite").parquet(output_path)

    def output(self):
        return PdcmConfig().get_target(
            "{0}/{1}/{2}".format(self.data_dir_out, Constants.RAW_DIRECTORY, Constants.MAPPING_DIAGNOSIS_MODULE))

    def app_options(self):
        return [
            self.data_dir,
            self.output().path]


def read_diagnosis_mapping_file(session, input_path, columns):
    if session.sparkContext.master != "yarn":
        with open(input_path + "/mapping/diagnosis_mappings.json", 'r') as jsonfile:
            data = jsonfile.read()
    else:
        rdd = session.sparkContext.textFile(input_path + "/mapping/diagnosis_mappings.json")
        data = rdd.collect()[0]
    obj = json.loads(data)
    data_rows = []
    for entity in obj['mappings']:
        datasource = entity['mappingValues']['DataSource']
        diagnosis = entity['mappingValues']['SampleDiagnosis']
        primary_tissue = entity['mappingValues']['OriginTissue']
        tumor_type = entity['mappingValues']['TumorType']
        mapped_term_url = entity['mappedTermUrl']
        justification = entity['justification']
        map_type = entity['mapType']
        data_rows.append((datasource, diagnosis, primary_tissue, tumor_type, mapped_term_url, justification, map_type))

    df = session.createDataFrame(data=data_rows, schema=columns)
    return df


class ReadTreatmentMappingsFromJson(PySparkTask):
    data_dir = luigi.Parameter()
    data_dir_out = luigi.Parameter()

    def main(self, sc, *args):
        spark = SparkSession(sc)

        input_path = args[0]
        output_path = args[1]

        columns = ["datasource", "treatment",  "mapped_term_url", "justification",
                   "map_type"]
        df = read_treatment_mapping_file(spark, input_path, columns)
        df.show()
        df.write.mode("overwrite").parquet(output_path)

    def output(self):
        return PdcmConfig().get_target(
            "{0}/{1}/{2}".format(self.data_dir_out, Constants.RAW_DIRECTORY, Constants.MAPPING_TREATMENTS_MODULE))

    def app_options(self):
        return [
            self.data_dir,
            self.output().path]


def read_treatment_mapping_file(session, input_path, columns):
    if session.sparkContext.master != "yarn":
        with open(input_path + "/mapping/treatment_mappings.json", 'r') as jsonfile:
            data = jsonfile.read()
    else:
        rdd = session.sparkContext.textFile(input_path + "/mapping/treatment_mappings.json")
        data = rdd.collect()[0]
    obj = json.loads(data)
    data_rows = []
    for entity in obj['mappings']:
        datasource = entity['mappingValues']['DataSource']
        treatment = entity['mappingValues']['TreatmentName']
        mapped_term_url = entity['mappedTermUrl']
        justification = entity['justification']
        map_type = entity['mapType']
        data_rows.append((datasource, treatment, mapped_term_url, justification, map_type))

    df = session.createDataFrame(data=data_rows, schema=columns)
    return df
