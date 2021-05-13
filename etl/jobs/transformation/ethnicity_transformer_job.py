import sys

from pyspark.sql import DataFrame, SparkSession, Column
from pyspark.sql.functions import trim, initcap

from etl.jobs.util.cleaner import init_cap_and_trim_all
from etl.jobs.util.id_assigner import add_id


def main(argv):
    """
    Creates a parquet file with ethnicity data.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with raw patient data
                    [2]: Output file
    """
    raw_patient_parquet_path = argv[1]
    output_path = argv[2]

    spark = SparkSession.builder.getOrCreate()
    raw_patient_df = spark.read.parquet(raw_patient_parquet_path)
    ethnicity_df = transform_ethnicity(raw_patient_df)
    ethnicity_df.write.mode("overwrite").parquet(output_path)


def transform_ethnicity(raw_patient_df: DataFrame) -> DataFrame:
    ethnicity_df = get_ethnicity_from_patient(raw_patient_df)
    ethnicity_df = add_id(ethnicity_df, "id")
    ethnicity_df = get_columns_expected_order(ethnicity_df)
    return ethnicity_df


def get_ethnicity_from_patient(raw_patient_df: DataFrame) -> DataFrame:
    ethnicity_df = raw_patient_df.select(init_cap_and_trim_all("ethnicity").alias("name"))
    ethnicity_df = ethnicity_df.select("name").where("name is not null")
    ethnicity_df = ethnicity_df.drop_duplicates()
    return ethnicity_df


def get_columns_expected_order(ethnicity_df: DataFrame) -> DataFrame:
    return ethnicity_df.select("id", "name")


if __name__ == "__main__":
    sys.exit(main(sys.argv))
