import sys

from pyspark.sql import DataFrame, SparkSession
from etl.jobs.util.dataframe_functions import transform_to_fk
from etl.jobs.util.id_assigner import add_id


def main(argv):
    """
    Creates a parquet file with patient data.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with raw sample data
                    [2]: Parquet file path with patient sample data
                    [3]: Parquet file path with patient data
                    [4]: Output file
    """
    raw_sample_parquet_path = argv[1]
    patient_sample_parquet_path = argv[2]
    patient_parquet_path = argv[3]

    output_path = argv[4]

    spark = SparkSession.builder.getOrCreate()
    raw_sample_df = spark.read.parquet(raw_sample_parquet_path)
    patient_sample_df = spark.read.parquet(patient_sample_parquet_path)
    patient_df = spark.read.parquet(patient_parquet_path)

    patient_snapshot_df = transform_patient_snapshot(raw_sample_df, patient_sample_df, patient_df)
    patient_snapshot_df.write.mode("overwrite").parquet(output_path)


def transform_patient_snapshot(raw_sample_df: DataFrame, patient_sample_df: DataFrame, patient_df: DataFrame) -> DataFrame:
    patient_snapshot_df = clean_data_before_join(raw_sample_df)
    patient_snapshot_df = set_fk_patient(patient_snapshot_df, patient_df)
    patient_snapshot_df = set_fk_patient_sample(patient_snapshot_df, patient_sample_df)
    patient_snapshot_df = add_id(patient_snapshot_df, "id")
    patient_snapshot_df = get_columns_expected_order(patient_snapshot_df)

    return patient_snapshot_df


def clean_data_before_join(raw_sample_df: DataFrame) -> DataFrame:
    # TODO: Do we need a transformation for the age?
    return raw_sample_df


def set_fk_patient(sample_df: DataFrame, patient_df: DataFrame) -> DataFrame:
    patient_snapshot_df = sample_df.withColumnRenamed("patient_id", "patient_id_ref")
    patient_snapshot_df = transform_to_fk(
        patient_snapshot_df, patient_df, "patient_id_ref", "external_patient_id", "id", "patient_id")
    return patient_snapshot_df


def set_fk_patient_sample(patient_snapshot_df: DataFrame, patient_sample_df: DataFrame) -> DataFrame:
    patient_snapshot_df = patient_snapshot_df.withColumnRenamed("sample_id", "sample_id_ref")
    patient_snapshot_df = transform_to_fk(
        patient_snapshot_df, patient_sample_df, "sample_id_ref", "external_patient_sample_id", "id", "sample_id")
    return patient_snapshot_df


def get_columns_expected_order(patient_snapshot_df: DataFrame) -> DataFrame:
    return patient_snapshot_df.select(
        "id",
        "patient_id",
        "age_in_years_at_collection",
        "collection_event",
        "collection_date",
        "months_since_collection_1",
        "treatment_naive_at_collection",
        "virology_status",
        "sample_id")


if __name__ == "__main__":
    sys.exit(main(sys.argv))
