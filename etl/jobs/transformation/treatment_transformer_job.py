import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

from etl.constants import Constants
from etl.jobs.util.cleaner import lower_and_trim_all
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
    treatment_df = transform_treatment(drug_dosing_df, patient_treatment_df)
    treatment_df.write.mode("overwrite").parquet(output_path)


def transform_treatment(drug_dosing_df: DataFrame, patient_treatment_df: DataFrame) -> DataFrame:
    treatment_df = get_treatment_from_drug_dosing(drug_dosing_df).union(
        get_treatment_patient_treatment(patient_treatment_df)
    )
    treatment_df = treatment_df.withColumn("name", lower_and_trim_all("name"))
    treatment_df = treatment_df.drop_duplicates()
    treatment_df = add_id(treatment_df, "id")
    treatment_df = treatment_df.select("id", "name")
    treatment_df.show()
    return treatment_df


def get_treatment_from_drug_dosing(drug_dosing_df: DataFrame) -> DataFrame:
    return drug_dosing_df.select(col("treatment_name").alias("name"))


def get_treatment_patient_treatment(patient_treatment_df: DataFrame) -> DataFrame:
    return patient_treatment_df.select(col("treatment_name").alias("name"))


if __name__ == "__main__":
    sys.exit(main(sys.argv))
