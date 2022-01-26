import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit

from etl.constants import Constants
from etl.jobs.util.cleaner import init_cap_and_trim_all
from etl.jobs.util.dataframe_functions import transform_to_fk
from etl.jobs.util.id_assigner import add_id


def main(argv):
    """
    Creates a parquet file with the transformed data for treatment_protocol.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with raw drug-dosing data
                    [2]: Parquet file path with raw patient-treatment data
                    [2]: Parquet file path with model_information data
                    [2]: Parquet file path with patient data
                    [2]: Parquet file path with response data
                    [2]: Parquet file path with response_classification data
                    [3]: Output file
    """
    raw_drug_dosing_parquet_path = argv[1]
    raw_patient_treatment_parquet_path = argv[2]
    model_parquet_path = argv[3]
    patient_parquet_path = argv[4]
    response_parquet_path = argv[5]
    response_classification_parquet_path = argv[6]
    output_path = argv[7]

    spark = SparkSession.builder.getOrCreate()
    raw_drug_dosing_df = spark.read.parquet(raw_drug_dosing_parquet_path)
    raw_patient_treatment_df = spark.read.parquet(raw_patient_treatment_parquet_path)
    model_df = spark.read.parquet(model_parquet_path)
    patient_df = spark.read.parquet(patient_parquet_path)
    response_df = spark.read.parquet(response_parquet_path)
    response_classification_df = spark.read.parquet(response_classification_parquet_path)
    treatment_protocol_df = transform_protocol(
        raw_drug_dosing_df, raw_patient_treatment_df, model_df, patient_df, response_df, response_classification_df)
    treatment_protocol_df.write.mode("overwrite").parquet(output_path)


def transform_protocol(
        raw_drug_dosing_df: DataFrame,
        raw_patient_treatment_df: DataFrame,
        model_df: DataFrame,
        patient_df: DataFrame,
        response_df: DataFrame,
        response_classification_df: DataFrame) -> DataFrame:
    treatment_protocol = get_data_from_drug_dosing(raw_drug_dosing_df, model_df).union(
        get_data_from_patient_treatment(raw_patient_treatment_df, patient_df)
    )
    treatment_protocol = treatment_protocol.drop_duplicates()
    treatment_protocol = set_fk_response(treatment_protocol, response_df)
    treatment_protocol = set_fk_response_classification(treatment_protocol, response_classification_df)
    treatment_protocol = add_id(treatment_protocol, "id")
    return treatment_protocol


def get_data_from_drug_dosing(drug_dosing_df: DataFrame, model_df: DataFrame) -> DataFrame:
    model_df = model_df.select("id", "external_model_id")
    data_from_drug_dosing = drug_dosing_df.select(
        "model_id", "treatment_name", "treatment_dose", "treatment_response", "response_classification")
    data_from_drug_dosing = data_from_drug_dosing.withColumn("patient_id", lit(None))
    data_from_drug_dosing = data_from_drug_dosing.withColumn("treatment_target", lit("drug dosing"))

    data_from_drug_dosing = data_from_drug_dosing.withColumnRenamed("model_id", "external_model_id")
    data_from_drug_dosing = transform_to_fk(
        data_from_drug_dosing, model_df, "external_model_id", "external_model_id", "id", "model_id")
    data_from_drug_dosing = data_from_drug_dosing.select(
        "model_id", "patient_id", "treatment_name", "treatment_dose", "treatment_response", "response_classification",
        "treatment_target")
    return data_from_drug_dosing


def get_data_from_patient_treatment(patient_treatment_df: DataFrame, patient_df: DataFrame) -> DataFrame:
    patient_df = patient_df.select("id", "external_patient_id")
    data_from_patient_treatment = patient_treatment_df.select(
        "patient_id", "treatment_name", "treatment_dose", "treatment_response", "response_classification")
    data_from_patient_treatment = data_from_patient_treatment.withColumn("model_id", lit(None))
    data_from_patient_treatment = data_from_patient_treatment.withColumn("treatment_target", lit("patient"))

    data_from_patient_treatment = data_from_patient_treatment.withColumnRenamed("patient_id", "external_patient_id")
    data_from_patient_treatment = transform_to_fk(
        data_from_patient_treatment, patient_df, "external_patient_id", "external_patient_id", "id", "patient_id")
    data_from_patient_treatment = data_from_patient_treatment.select(
        "model_id", "patient_id", "treatment_name", "treatment_dose", "treatment_response", "response_classification",
        "treatment_target")
    return data_from_patient_treatment


def set_fk_response(treatment_protocol: DataFrame, response_df: DataFrame) -> DataFrame:
    treatment_protocol = treatment_protocol.withColumn(
        "treatment_response", init_cap_and_trim_all("treatment_response"))
    treatment_protocol = transform_to_fk(
        treatment_protocol, response_df, "treatment_response", "name", "id", "response_id")
    return treatment_protocol


def set_fk_response_classification(treatment_protocol: DataFrame, response_classification_df: DataFrame) -> DataFrame:
    treatment_protocol = treatment_protocol.withColumn(
        "response_classification", init_cap_and_trim_all("response_classification"))

    treatment_protocol = transform_to_fk(
        treatment_protocol,
        response_classification_df,
        "response_classification",
        "name",
        "id",
        "response_classification_id")
    return treatment_protocol


def get_expected_columns(model_drug_dosing_df: DataFrame) -> DataFrame:
    return model_drug_dosing_df.select(
        "id",
        "model_id",
        "patient_id",
        "treatment_target",
        "response_id",
        "response_classification_id",
        "treatment_name",
        "treatment_dose",
        Constants.DATA_SOURCE_COLUMN)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
