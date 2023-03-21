import sys

from pyspark.sql import DataFrame, SparkSession

from etl.constants import Constants
from etl.jobs.util.cleaner import init_cap_and_trim_all
from etl.jobs.util.id_assigner import add_id


def main(argv):
    """
    Creates a parquet file with patient data.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with raw model data
                    [2]: Parquet file path with diagnosis data
                    [3]: Parquet file path with provider group data
                    [4]: Output file
    """
    raw_patient_parquet_path = argv[1]
    ethnicity_parquet_path = argv[2]
    provider_group_parquet_path = argv[3]
    output_path = argv[4]

    spark = SparkSession.builder.getOrCreate()
    raw_patient_df = spark.read.parquet(raw_patient_parquet_path)
    ethnicity_df = spark.read.parquet(ethnicity_parquet_path)
    provider_group_df = spark.read.parquet(provider_group_parquet_path)
    patient_df = transform_patient(raw_patient_df, ethnicity_df, provider_group_df)
    # Temporary fix to remove null rows
    patient_df = patient_df.where("external_patient_id is not null")
    patient_df.write.mode("overwrite").parquet(output_path)


def transform_patient(
        raw_patient_df: DataFrame,
        ethnicity_df: DataFrame,
        provider_group_df: DataFrame) -> DataFrame:

    patient_df = clean_data_before_join(raw_patient_df)
    patient_df = set_fk_ethnicity(patient_df, ethnicity_df)
    patient_df = set_fk_provider_group(patient_df, provider_group_df)
    patient_df = set_external_id(patient_df)
    patient_df = add_id(patient_df, "id")
    patient_df = get_columns_expected_order(patient_df)
    return patient_df


def clean_data_before_join(raw_patient_df: DataFrame) -> DataFrame:
    raw_patient_df = raw_patient_df.withColumn("ethnicity", init_cap_and_trim_all("ethnicity"))
    return raw_patient_df.drop_duplicates()


def set_fk_ethnicity(patient_df: DataFrame, ethnicity_df: DataFrame) -> DataFrame:
    ethnicity_df = ethnicity_df.withColumnRenamed("id", "ethnicity_id")
    ethnicity_df = ethnicity_df.withColumnRenamed("name", "name")
    patient_df = patient_df.withColumnRenamed("ethnicity", "patient_ethnicity")
    patient_df = patient_df.join(ethnicity_df, patient_df.patient_ethnicity == ethnicity_df.name, how='left')
    patient_df = patient_df.drop("name")
    return patient_df


def set_fk_provider_group(patient_df: DataFrame, provider_group_df: DataFrame) -> DataFrame:
    provider_group_df = provider_group_df.withColumnRenamed("name", "provider_name")
    provider_group_df = provider_group_df.withColumnRenamed("abbreviation", "provider_abbreviation")
    provider_group_df = provider_group_df.withColumnRenamed("id", "provider_group_id")
    patient_df = patient_df.join(provider_group_df, on=[Constants.DATA_SOURCE_COLUMN], how='left')
    return patient_df


def set_external_id(patient_df: DataFrame) -> DataFrame:
    return patient_df.withColumnRenamed("patient_id", "external_patient_id")


def get_columns_expected_order(patient_df: DataFrame) -> DataFrame:
    return patient_df.select(
        "id",
        "external_patient_id",
        "sex",
        "history",
        "ethnicity_id",
        "patient_ethnicity",
        "ethnicity_assessment_method",
        "initial_diagnosis",
        "age_at_initial_diagnosis",
        "provider_group_id",
        "provider_name",
        "provider_abbreviation",
        "project_group_name",
        Constants.DATA_SOURCE_COLUMN)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
