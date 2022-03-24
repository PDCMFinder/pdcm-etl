import sys

from pyspark.sql import DataFrame, SparkSession

from etl.jobs.util.cleaner import lower_and_trim_all
from etl.jobs.util.id_assigner import add_id


def main(argv):
    """
    Creates a parquet file with diagnosis data.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with raw patient data
                    [2]: Parquet file path with raw sample data
                    [3]: Output file
    """
    raw_patient_parquet_path = argv[1]
    raw_sample_parquet_path = argv[2]
    output_path = argv[3]

    spark = SparkSession.builder.getOrCreate()
    raw_patient_df = spark.read.parquet(raw_patient_parquet_path)
    raw_sample_df = spark.read.parquet(raw_sample_parquet_path)
    diagnosis_df = transform_diagnosis(raw_patient_df, raw_sample_df)
    diagnosis_df.write.mode("overwrite").parquet(output_path)


def transform_diagnosis(raw_patient_df: DataFrame, raw_sample_df: DataFrame) -> DataFrame:
    diagnosis_df = get_diagnosis_from_patient(raw_patient_df).union(
        get_diagnosis_from_sample(raw_sample_df)
    )
    diagnosis_df = diagnosis_df.drop_duplicates()
    diagnosis_df = add_id(diagnosis_df, "id")
    diagnosis_df = diagnosis_df.select("id", "name")
    return diagnosis_df


def get_diagnosis_from_patient(raw_patient_df: DataFrame) -> DataFrame:
    return raw_patient_df.select(lower_and_trim_all("initial_diagnosis").alias("name"))


def get_diagnosis_from_sample(raw_sample_df: DataFrame) -> DataFrame:
    return raw_sample_df.select(lower_and_trim_all("diagnosis").alias("name"))


if __name__ == "__main__":
    sys.exit(main(sys.argv))
