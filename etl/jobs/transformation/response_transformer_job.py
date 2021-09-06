import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

from etl.constants import Constants
from etl.jobs.util.cleaner import lower_and_trim_all, init_cap_and_trim_all
from etl.jobs.util.id_assigner import add_id


def main(argv):
    """
    Creates a parquet file with diagnosis data.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with drug-dosing data
                    [2]: Parquet file path with patient-treatment data
                    [3]: Output file
    """
    drug_dosing_parquet_path = argv[1]
    patient_treatment_parquet_path = argv[2]
    output_path = argv[3]

    spark = SparkSession.builder.getOrCreate()
    drug_dosing_df = spark.read.parquet(drug_dosing_parquet_path)
    patient_treatment_df = spark.read.parquet(patient_treatment_parquet_path)
    response_df = transform_response(drug_dosing_df, patient_treatment_df)
    response_df.write.mode("overwrite").parquet(output_path)


def transform_response(drug_dosing_df: DataFrame, patient_treatment_df: DataFrame) -> DataFrame:
    response_df = get_data_from_drug_dosing(drug_dosing_df).union(
        get_data_from_patient_treatment(patient_treatment_df)
    )
    response_df = response_df.drop_duplicates()
    response_df = add_id(response_df, "id")
    response_df = get_columns_expected_order(response_df)
    return response_df


def get_data_from_drug_dosing(drug_dosing_df: DataFrame) -> DataFrame:
    return drug_dosing_df.select(
        init_cap_and_trim_all("treatment_response").alias("description"),
        col("response_classification").alias("classification"))


def get_data_from_patient_treatment(patient_treatment_df: DataFrame) -> DataFrame:
    return patient_treatment_df.select(
        init_cap_and_trim_all("treatment_response").alias("description"),
        col("response_classification").alias("classification"))


def get_columns_expected_order(response_df: DataFrame) -> DataFrame:
    return response_df.select("id", "description", "classification")


if __name__ == "__main__":
    sys.exit(main(sys.argv))