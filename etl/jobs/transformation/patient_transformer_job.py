import sys

from pyspark.sql import DataFrame, SparkSession

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
    patient_df = add_id(raw_patient_df, "id")
    patient_df = set_fk_diagnosis(patient_df, diagnosis_df)
    patient_df = set_fk_ethnicity(patient_df, ethnicity_df)
    patient_df = set_fk_provider_group(patient_df, provider_group_df)
    patient_df = set_external_id(patient_df)
    patient_df = get_columns_expected_order(patient_df)
    return patient_df


def set_fk_diagnosis(raw_patient_df: DataFrame, diagnosis_df: DataFrame) -> DataFrame:
    diagnosis_df_ref = diagnosis_df \
        .withColumnRenamed("name", "diagnosis_name_ref") \
        .withColumnRenamed("id", "id_ref")
    raw_patient_df_ref = raw_patient_df.withColumnRenamed("initial_diagnosis", "diagnosis_name_ref")
    patient_df = raw_patient_df_ref.join(diagnosis_df_ref, on=['diagnosis_name_ref'], how='left')
    patient_df = patient_df.withColumnRenamed("id_ref", "initial_diagnosis_id")
    patient_df = patient_df.drop("diagnosis_name_ref")
    return patient_df


def set_fk_ethnicity(patient_df: DataFrame, ethnicity_df: DataFrame) -> DataFrame:
    ethnicity_df_ref = ethnicity_df \
        .withColumnRenamed("name", "ethnicity_name_ref") \
        .withColumnRenamed("id", "id_ref")
    patient_df_ref = patient_df.withColumnRenamed("ethnicity", "ethnicity_name_ref")
    patient_df = patient_df_ref.join(ethnicity_df_ref, on=['ethnicity_name_ref'], how='left')
    patient_df = patient_df.withColumnRenamed("id_ref", "ethnicity_id")
    patient_df = patient_df.drop("ethnicity_name_ref")
    return patient_df


def set_fk_provider_group(patient_df: DataFrame, provider_group_df: DataFrame) -> DataFrame:
    provider_group_ref_df = provider_group_df \
        .withColumnRenamed("abbreviation", "provider_name_ref") \
        .withColumnRenamed("id", "id_ref")

    patient_df_ref = patient_df.withColumnRenamed("data_source", "provider_name_ref")
    patient_df = patient_df_ref.join(provider_group_ref_df, on=['provider_name_ref'], how='left')
    patient_df = patient_df.withColumnRenamed("id_ref", "provider_group_id")
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
