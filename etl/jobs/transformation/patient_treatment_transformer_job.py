import sys

from pyspark.sql import DataFrame, SparkSession, Column
from pyspark.sql.functions import col, trim

from etl.constants import Constants
from etl.jobs.util.cleaner import lower_and_trim_all, init_cap_and_trim_all
from etl.jobs.util.dataframe_functions import transform_to_fk
from etl.jobs.util.id_assigner import add_id


def main(argv):
    """
    Creates a parquet file with model data.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with raw patient treatment data
                    [2]: Parquet file path with patient data
                    [3]: Parquet file path with treatment data
                    [4]: Parquet file path with response data
                    [5]: Parquet file path with response classification data
                    [6]: Parquet file path with model data
                    [7]: Output file
    """
    raw_patient_treatment_parquet_path = argv[1]
    patient_parquet_path = argv[2]
    treatment_parquet_path = argv[3]
    response_parquet_path = argv[4]
    response_classification_parquet_path = argv[5]
    model_parquet_path = argv[6]
    output_path = argv[7]

    spark = SparkSession.builder.getOrCreate()
    raw_patient_treatment_df = spark.read.parquet(raw_patient_treatment_parquet_path)
    patient_df = spark.read.parquet(patient_parquet_path)
    treatment_df = spark.read.parquet(treatment_parquet_path)
    response_df = spark.read.parquet(response_parquet_path)
    response_classification_df = spark.read.parquet(response_classification_parquet_path)
    mode_df = spark.read.parquet(model_parquet_path)

    patient_treatment_df = transform_patient_treatment(
        raw_patient_treatment_df,
        patient_df,
        treatment_df,
        response_df,
        response_classification_df,
        mode_df)
    patient_treatment_df.write.mode("overwrite").parquet(output_path)


def transform_patient_treatment(
        raw_patient_treatment_df: DataFrame,
        patient_df: DataFrame,
        treatment_df: DataFrame,
        response_df: DataFrame,
        response_classification_df: DataFrame,
        mode_df: DataFrame) -> DataFrame:
    
    patient_treatment_df = raw_patient_treatment_df
    patient_treatment_df = add_id(patient_treatment_df, "id")
    patient_treatment_df = set_fk_patient(patient_treatment_df, patient_df)
    patient_treatment_df = set_fk_treatment(patient_treatment_df, treatment_df)
    patient_treatment_df = set_fk_response(patient_treatment_df, response_df)
    patient_treatment_df = set_fk_response_classification(patient_treatment_df, response_classification_df)
    patient_treatment_df = set_fk_model(patient_treatment_df, mode_df)
    patient_treatment_df = get_expected_columns(patient_treatment_df)
    return patient_treatment_df


def set_fk_patient(patient_treatment_df: DataFrame, patient_df: DataFrame) -> DataFrame:
    patient_df = patient_df.select("id", "external_patient_id")
    patient_treatment_df = patient_treatment_df.withColumnRenamed("patient_id", "external_patient_id")
    patient_treatment_df = transform_to_fk(
        patient_treatment_df, patient_df, "external_patient_id", "external_patient_id", "id", "patient_id")
    patient_treatment_df = patient_treatment_df.where("patient_id is not null")
    return patient_treatment_df


def set_fk_treatment(patient_treatment_df: DataFrame, treatment_df: DataFrame) -> DataFrame:
    patient_treatment_df = patient_treatment_df.withColumn("treatment_name", lower_and_trim_all("treatment_name"))
    treatment_df = treatment_df.withColumnRenamed("id", "treatment_id")
    treatment_df = treatment_df.withColumnRenamed("name", "treatment_name")

    patient_treatment_df = patient_treatment_df.join(treatment_df, on=["treatment_name", Constants.DATA_SOURCE_COLUMN])
    return patient_treatment_df


def set_fk_response(patient_treatment_df: DataFrame, response_df: DataFrame) -> DataFrame:
    patient_treatment_df = patient_treatment_df.withColumn(
        "treatment_response", init_cap_and_trim_all("treatment_response"))
    patient_treatment_df = transform_to_fk(
        patient_treatment_df, response_df, "treatment_response", "name", "id", "response_id")
    return patient_treatment_df


def set_fk_response_classification(patient_treatment_df: DataFrame, response_classification_df: DataFrame) -> DataFrame:
    patient_treatment_df = patient_treatment_df.withColumn(
        "response_classification", init_cap_and_trim_all("response_classification"))

    patient_treatment_df = transform_to_fk(
        patient_treatment_df,
        response_classification_df,
        "response_classification",
        "name",
        "id",
        "response_classification_id")
    return patient_treatment_df


def set_fk_model(patient_treatment_df: DataFrame, model_df: DataFrame) -> DataFrame:
    patient_treatment_df = patient_treatment_df.withColumnRenamed("model_id", "external_model_id")
    model_df = transform_to_fk(
        patient_treatment_df, model_df, "external_model_id", "external_model_id", "id", "model_id")
    return model_df


def get_expected_columns(patient_treatment_df: DataFrame) -> DataFrame:
    return patient_treatment_df.select(
        "id",
        "patient_id",
        "treatment_id",
        "treatment_dose",
        "treatment_starting_date",
        "treatment_duration",
        "treatment_event",
        "elapsed_time",
        "response_id",
        "response_classification_id",
        "model_id",
        Constants.DATA_SOURCE_COLUMN)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
