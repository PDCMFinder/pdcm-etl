import sys

from pyspark.sql import DataFrame, SparkSession

from etl.constants import Constants
from etl.jobs.util.cleaner import init_cap_and_trim_all
from etl.jobs.util.dataframe_functions import join_left_dfs, transform_to_fk
from etl.jobs.util.id_assigner import add_id


def main(argv):
    """
    Creates a parquet file with patient data.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with raw model data
                    [2]: Parquet file path with diagnosis data
                    [3]: Parquet file path with ethnicity data
                    [4]: Parquet file path with provider group data
                    [5]: Output file
    """
    raw_patient_parquet_path = argv[1]
    diagnosis_parquet_path = argv[2]
    ethnicity_parquet_path = argv[3]
    provider_group_parquet_path = argv[4]
    output_path = argv[5]

    spark = SparkSession.builder.getOrCreate()
    raw_patient_df = spark.read.parquet(raw_patient_parquet_path)
    diagnosis_df = spark.read.parquet(diagnosis_parquet_path)
    ethnicity_df = spark.read.parquet(ethnicity_parquet_path)
    provider_group_df = spark.read.parquet(provider_group_parquet_path)
    patient_df = transform_patient(raw_patient_df, diagnosis_df, ethnicity_df, provider_group_df)
    patient_df.write.mode("overwrite").parquet(output_path)


def transform_patient(
        raw_patient_df: DataFrame,
        diagnosis_df: DataFrame,
        ethnicity_df: DataFrame,
        provider_group_df: DataFrame) -> DataFrame:

    patient_df = clean_data_before_join(raw_patient_df)
    patient_df = set_fk_diagnosis(patient_df, diagnosis_df)
    patient_df = set_fk_ethnicity(patient_df, ethnicity_df)
    patient_df = set_fk_provider_group(patient_df, provider_group_df)
    patient_df = set_external_id(patient_df)
    patient_df = add_id(patient_df, "id")
    patient_df = get_columns_expected_order(patient_df)
    return patient_df


def clean_data_before_join(patient_df: DataFrame) -> DataFrame:
    patient_df = patient_df.withColumn("ethnicity", init_cap_and_trim_all("ethnicity"))
    return patient_df


def set_fk_diagnosis(raw_patient_df: DataFrame, diagnosis_df: DataFrame) -> DataFrame:
    patient_df = transform_to_fk(
        raw_patient_df, diagnosis_df, "initial_diagnosis", "name", "id", "initial_diagnosis_id")
    return patient_df


def set_fk_ethnicity(patient_df: DataFrame, ethnicity_df: DataFrame) -> DataFrame:
    patient_df = transform_to_fk(patient_df, ethnicity_df, "ethnicity", "name", "id", "ethnicity_id")
    return patient_df


def set_fk_provider_group(patient_df: DataFrame, provider_group_df: DataFrame) -> DataFrame:
    patient_df = transform_to_fk(
        patient_df,
        provider_group_df,
        Constants.DATA_SOURCE_COLUMN,
        Constants.DATA_SOURCE_COLUMN,
        "id",
        "provider_group_id")
    return patient_df


def set_external_id(patient_df: DataFrame) -> DataFrame:
    return patient_df.withColumnRenamed("patient_id", "external_id")


def get_columns_expected_order(patient_df: DataFrame) -> DataFrame:
    return patient_df.select(
        "id",
        "external_id",
        "sex",
        "history",
        "ethnicity_id",
        "ethnicity_assessment_method",
        "initial_diagnosis_id",
        "age_at_initial_diagnosis",
        "provider_group_id")


if __name__ == "__main__":
    sys.exit(main(sys.argv))
