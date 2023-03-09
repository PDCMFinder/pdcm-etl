import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

from etl.constants import Constants
from etl.jobs.util.cleaner import init_cap_and_trim_all, lower_and_trim_all
from etl.jobs.util.dataframe_functions import transform_to_fk
from etl.jobs.util.id_assigner import add_id


def main(argv):
    """
    Creates a parquet file with patient sample data.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with raw sample data
                    [2]: Parquet file path with tissue data
                    [3]: Parquet file path with tumour type data
                    [4]: Parquet file path with model data
                    [5]: Output file
    """
    raw_sample_parquet_path = argv[1]
    patient_parquet_path = argv[2]
    tissue_parquet_path = argv[3]
    tumour_type_parquet_path = argv[4]
    model_parquet_path = argv[5]
    output_path = argv[6]

    spark = SparkSession.builder.getOrCreate()
    raw_sample_df = spark.read.parquet(raw_sample_parquet_path)
    patient_df = spark.read.parquet(patient_parquet_path)
    tissue_df = spark.read.parquet(tissue_parquet_path)
    tumour_type_df = spark.read.parquet(tumour_type_parquet_path)
    model_df = spark.read.parquet(model_parquet_path)
    patient_sample_df = transform_patient_sample(raw_sample_df, patient_df, tissue_df, tumour_type_df, model_df)
    patient_sample_df.write.mode("overwrite").parquet(output_path)


def transform_patient_sample(
        raw_sample_df: DataFrame,
        patient_df: DataFrame,
        tissue_df: DataFrame,
        tumour_type_df: DataFrame,
        model_df: DataFrame) -> DataFrame:
    patient_sample_df = extract_patient_sample(raw_sample_df)
    patient_sample_df = clean_data_before_join(patient_sample_df)
    patient_sample_df = set_fk_patient(patient_sample_df, patient_df)
    patient_sample_df = set_fk_origin_tissue(patient_sample_df, tissue_df)
    patient_sample_df = set_fk_sample_site(patient_sample_df, tissue_df)
    patient_sample_df = set_fk_tumour_type(patient_sample_df, tumour_type_df)
    patient_sample_df = set_fk_model(patient_sample_df, model_df)
    patient_sample_df = add_id(patient_sample_df, "id")
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
        init_cap_and_trim_all("treated_prior_to_collection").alias("prior_treatment"),
        "tumour_type",
        "patient_id",
        "age_in_years_at_collection",
        "collection_event",
        "collection_date",
        "months_since_collection_1",
        "treatment_naive_at_collection",
        "treated_at_collection",
        "virology_status",
        "sharable",
        col("model_id").alias("model_name"),
        Constants.DATA_SOURCE_COLUMN
    ).where("sample_id is not null").drop_duplicates()
    return patient_sample_df


def clean_data_before_join(patient_sample_df: DataFrame) -> DataFrame:
    patient_sample_df = patient_sample_df.withColumn("tumour_type", init_cap_and_trim_all("tumour_type"))
    return patient_sample_df


def set_fk_patient(patient_sample_df: DataFrame, patient_df: DataFrame) -> DataFrame:
    patient_sample_df = patient_sample_df.drop_duplicates()
    patient_sample_df = patient_sample_df.withColumnRenamed("patient_id", "external_patient_id")
    patient_df = patient_df.select("id", "external_patient_id", Constants.DATA_SOURCE_COLUMN)
    patient_df = patient_df.withColumnRenamed("id", "patient_id")
    patient_sample_df = patient_sample_df.join(
        patient_df, on=["external_patient_id", Constants.DATA_SOURCE_COLUMN], how='left')
    return patient_sample_df


def set_fk_origin_tissue(patient_sample_df: DataFrame, tissue_df: DataFrame) -> DataFrame:
    patient_sample_df = patient_sample_df.withColumn("primary_site", lower_and_trim_all("primary_site"))
    patient_sample_df = transform_to_fk(patient_sample_df, tissue_df, "primary_site", "name", "id", "primary_site_id")
    return patient_sample_df


def set_fk_sample_site(patient_sample_df: DataFrame, tissue_df: DataFrame) -> DataFrame:
    patient_sample_df = patient_sample_df.withColumn("collection_site", lower_and_trim_all("collection_site"))
    patient_sample_df = transform_to_fk(
        patient_sample_df, tissue_df, "collection_site", "name", "id", "collection_site_id")
    return patient_sample_df


def set_fk_tumour_type(patient_sample_df: DataFrame, tumour_type_df: DataFrame) -> DataFrame:
    patient_sample_df = transform_to_fk(patient_sample_df, tumour_type_df, "tumour_type", "name", "id",
                                        "tumour_type_id")
    return patient_sample_df


def set_fk_model(patient_sample_df: DataFrame, model_df: DataFrame) -> DataFrame:
    model_df = model_df.select(
        col("id").alias("model_id_ref"),
        col("external_model_id").alias("model_name"),
        col("data_source").alias(Constants.DATA_SOURCE_COLUMN))
    patient_sample_df = patient_sample_df.join(model_df, on=['model_name', Constants.DATA_SOURCE_COLUMN], how='left')
    patient_sample_df = patient_sample_df.withColumnRenamed("model_id_ref", "model_id")
    return patient_sample_df


if __name__ == "__main__":
    sys.exit(main(sys.argv))
