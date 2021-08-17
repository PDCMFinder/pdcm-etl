import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

from etl.constants import Constants
from etl.jobs.util.cleaner import init_cap_and_trim_all
from etl.jobs.util.dataframe_functions import transform_to_fk
from etl.jobs.util.id_assigner import add_id
from etl.jobs.util.raw_data_url_builder import build_raw_data_url


def main(argv):
    """
    Creates a parquet file with patient sample data.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with raw sample data
                    [2]: Parquet file path with diagnosis data
                    [3]: Parquet file path with tissue data
                    [4]: Parquet file path with tumour type data
                    [5]: Parquet file path with model data
                    [6]: Parquet file path with raw sample platform data
                    [7]: Output file
    """
    raw_sample_parquet_path = argv[1]
    diagnosis_parquet_path = argv[2]
    tissue_parquet_path = argv[3]
    tumour_type_parquet_path = argv[4]
    model_parquet_path = argv[5]
    raw_sample_platform_parquet_path = argv[6]
    output_path = argv[7]

    spark = SparkSession.builder.getOrCreate()
    raw_sample_df = spark.read.parquet(raw_sample_parquet_path)
    diagnosis_df = spark.read.parquet(diagnosis_parquet_path)
    tissue_df = spark.read.parquet(tissue_parquet_path)
    tumour_type_df = spark.read.parquet(tumour_type_parquet_path)
    model_df = spark.read.parquet(model_parquet_path)
    raw_sample_platform_df = spark.read.parquet(raw_sample_platform_parquet_path)
    patient_sample_df = transform_patient_sample(
        raw_sample_df, diagnosis_df, tissue_df, tumour_type_df, model_df, raw_sample_platform_df)
    patient_sample_df.write.mode("overwrite").parquet(output_path)


def transform_patient_sample(
        raw_sample_df: DataFrame,
        diagnosis_df: DataFrame,
        tissue_df: DataFrame,
        tumour_type_df: DataFrame,
        model_df: DataFrame,
        raw_sample_platform_df: DataFrame) -> DataFrame:
    patient_sample_df = extract_patient_sample(raw_sample_df)
    patient_sample_df = clean_data_before_join(patient_sample_df)
    patient_sample_df = add_id(patient_sample_df, "id")
    patient_sample_df = set_fk_diagnosis(patient_sample_df, diagnosis_df)
    patient_sample_df = set_fk_origin_tissue(patient_sample_df, tissue_df)
    patient_sample_df = set_fk_sample_site(patient_sample_df, tissue_df)
    patient_sample_df = set_fk_tumour_type(patient_sample_df, tumour_type_df)
    patient_sample_df = set_fk_model(patient_sample_df, model_df)
    patient_sample_df = set_raw_data_url(patient_sample_df, raw_sample_platform_df)
    patient_sample_df = get_columns_expected_order(patient_sample_df)
    return patient_sample_df


def extract_patient_sample(raw_sample_df: DataFrame) -> DataFrame:
    patient_sample_df = raw_sample_df.select(
        "diagnosis",
        col("sample_id").alias("external_patient_sample_id"),
        "grade",
        "grading_system",
        "stage",
        "staging_system",
        "primary_site",
        "collection_site",
        init_cap_and_trim_all("prior_treatment").alias("prior_treatment"),
        "tumour_type",
        col("model_id").alias("model_name"),
        Constants.DATA_SOURCE_COLUMN
    )
    return patient_sample_df


def clean_data_before_join(patient_sample_df: DataFrame) -> DataFrame:
    patient_sample_df = patient_sample_df.withColumn("tumour_type", init_cap_and_trim_all("tumour_type"))
    return patient_sample_df


def set_fk_diagnosis(patient_sample_df: DataFrame, diagnosis_df: DataFrame) -> DataFrame:
    patient_sample_df = transform_to_fk(
        patient_sample_df, diagnosis_df, "diagnosis", "name", "id", "diagnosis_id")
    return patient_sample_df


def set_fk_origin_tissue(patient_sample_df: DataFrame, tissue_df: DataFrame) -> DataFrame:
    patient_sample_df = transform_to_fk(patient_sample_df, tissue_df, "primary_site", "name", "id", "primary_site_id")
    return patient_sample_df


def set_fk_sample_site(patient_sample_df: DataFrame, tissue_df: DataFrame) -> DataFrame:
    patient_sample_df = transform_to_fk(
        patient_sample_df, tissue_df, "collection_site", "name", "id", "collection_site_id")
    return patient_sample_df


def set_fk_tumour_type(patient_sample_df: DataFrame, tumour_type_df: DataFrame) -> DataFrame:
    patient_sample_df = transform_to_fk(patient_sample_df, tumour_type_df, "tumour_type", "name", "id", "tumour_type_id")
    return patient_sample_df


def set_fk_model(patient_sample_df: DataFrame, model_df: DataFrame) -> DataFrame:
    model_df = model_df.select(
        col("id").alias("model_id_ref"),
        col("external_model_id").alias("model_name"),
        col("data_source").alias(Constants.DATA_SOURCE_COLUMN))
    patient_sample_df = patient_sample_df.join(model_df, on=['model_name', Constants.DATA_SOURCE_COLUMN], how='left')
    patient_sample_df = patient_sample_df.withColumnRenamed("model_id_ref", "model_id")
    return patient_sample_df


def set_raw_data_url(patient_sample_df: DataFrame, raw_sample_platform_df: DataFrame) -> DataFrame:
    raw_sample_platform_ref_df = raw_sample_platform_df.withColumnRenamed("sample_id", "sample_id_ref")
    raw_sample_platform_ref_df = raw_sample_platform_ref_df.withColumnRenamed("model_id", "model_id_ref")
    raw_sample_platform_ref_df = raw_sample_platform_ref_df.withColumnRenamed("data_source_tmp", "data_source_tmp_ref")

    patient_sample_df = patient_sample_df.join(
        raw_sample_platform_ref_df,
        (patient_sample_df.external_patient_sample_id == raw_sample_platform_ref_df.sample_id_ref)
        & (patient_sample_df.model_id == raw_sample_platform_ref_df.model_id_ref)
        & (raw_sample_platform_ref_df.sample_origin == 'patient')
        & (patient_sample_df.data_source_tmp == raw_sample_platform_ref_df.data_source_tmp_ref),  how='left')

    patient_sample_df = build_raw_data_url(patient_sample_df, "raw_data_url")

    return patient_sample_df


def get_columns_expected_order(patient_df: DataFrame) -> DataFrame:
    return patient_df.select(
        "id",
        "diagnosis_id",
        "external_patient_sample_id",
        "grade",
        "grading_system",
        "stage",
        "staging_system",
        "primary_site_id",
        "collection_site_id",
        "raw_data_url",
        "prior_treatment",
        "tumour_type_id",
        "model_id")


if __name__ == "__main__":
    sys.exit(main(sys.argv))
