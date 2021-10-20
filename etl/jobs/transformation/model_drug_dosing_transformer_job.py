import sys

from pyspark.sql import DataFrame, SparkSession

from etl.constants import Constants
from etl.jobs.util.cleaner import lower_and_trim_all, init_cap_and_trim_all
from etl.jobs.util.dataframe_functions import transform_to_fk
from etl.jobs.util.id_assigner import add_id


def main(argv):
    """
    Creates a parquet file with model data.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with raw drug dosing data
                    [2]: Parquet file path with treatment data
                    [3]: Parquet file path with response data
                    [4]: Parquet file path with response classification data
                    [5]: Parquet file path with model data
                    [6]: Output file
    """
    raw_drug_dosing_parquet_path = argv[1]
    treatment_parquet_path = argv[2]
    response_parquet_path = argv[3]
    response_classification_parquet_path = argv[4]
    model_parquet_path = argv[5]
    output_path = argv[6]

    spark = SparkSession.builder.getOrCreate()
    raw_drug_dosing_df = spark.read.parquet(raw_drug_dosing_parquet_path)
    treatment_df = spark.read.parquet(treatment_parquet_path)
    response_df = spark.read.parquet(response_parquet_path)
    response_classification_df = spark.read.parquet(response_classification_parquet_path)
    mode_df = spark.read.parquet(model_parquet_path)

    model_drug_dosing_df = transform_model_drug_dosing(
        raw_drug_dosing_df,
        treatment_df,
        response_df,
        response_classification_df,
        mode_df)
    model_drug_dosing_df.write.mode("overwrite").parquet(output_path)


def transform_model_drug_dosing(
        raw_model_drug_dosing_df: DataFrame,
        treatment_df: DataFrame,
        response_df: DataFrame,
        response_classification_df: DataFrame,
        mode_df: DataFrame) -> DataFrame:
    
    model_drug_dosing_df = raw_model_drug_dosing_df
    model_drug_dosing_df = add_id(model_drug_dosing_df, "id")
    model_drug_dosing_df = set_fk_treatment(model_drug_dosing_df, treatment_df)
    model_drug_dosing_df = set_fk_response(model_drug_dosing_df, response_df)
    model_drug_dosing_df = set_fk_response_classification(model_drug_dosing_df, response_classification_df)
    model_drug_dosing_df = set_fk_model(model_drug_dosing_df, mode_df)
    model_drug_dosing_df = get_expected_columns(model_drug_dosing_df)
    return model_drug_dosing_df


def set_fk_patient(model_drug_dosing_df: DataFrame, patient_df: DataFrame) -> DataFrame:
    patient_df = patient_df.select("id", "external_patient_id")
    model_drug_dosing_df = model_drug_dosing_df.withColumnRenamed("patient_id", "external_patient_id")
    model_drug_dosing_df = transform_to_fk(
        model_drug_dosing_df, patient_df, "external_patient_id", "external_patient_id", "id", "patient_id")
    return model_drug_dosing_df


def set_fk_treatment(model_drug_dosing_df: DataFrame, treatment_df: DataFrame) -> DataFrame:
    model_drug_dosing_df = model_drug_dosing_df.withColumn("treatment_name", lower_and_trim_all("treatment_name"))
    model_drug_dosing_df = transform_to_fk(
        model_drug_dosing_df, treatment_df, "treatment_name", "name", "id", "treatment_id")
    return model_drug_dosing_df


def set_fk_response(model_drug_dosing_df: DataFrame, response_df: DataFrame) -> DataFrame:
    model_drug_dosing_df = model_drug_dosing_df.withColumn(
        "treatment_response", init_cap_and_trim_all("treatment_response"))
    model_drug_dosing_df = transform_to_fk(
        model_drug_dosing_df, response_df, "treatment_response", "name", "id", "response_id")
    return model_drug_dosing_df


def set_fk_response_classification(model_drug_dosing_df: DataFrame, response_classification_df: DataFrame) -> DataFrame:
    model_drug_dosing_df = model_drug_dosing_df.withColumn(
        "response_classification", init_cap_and_trim_all("response_classification"))

    model_drug_dosing_df = transform_to_fk(
        model_drug_dosing_df,
        response_classification_df,
        "response_classification",
        "name",
        "id",
        "response_classification_id")
    return model_drug_dosing_df


def set_fk_model(model_drug_dosing_df: DataFrame, model_df: DataFrame) -> DataFrame:
    model_drug_dosing_df = model_drug_dosing_df.withColumnRenamed("model_id", "external_model_id")
    model_df = transform_to_fk(
        model_drug_dosing_df, model_df, "external_model_id", "external_model_id", "id", "model_id")
    return model_df


def get_expected_columns(model_drug_dosing_df: DataFrame) -> DataFrame:
    return model_drug_dosing_df.select(
        "id",
        "passage_range",
        "treatment_id",
        "treatment_type",
        "treatment_dose",
        "administration_route",
        "treatment_schedule",
        "treatment_length",
        "response_id",
        "response_classification_id",
        "model_id",
        Constants.DATA_SOURCE_COLUMN)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
