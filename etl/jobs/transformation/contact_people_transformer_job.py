import sys

from pyspark.sql import DataFrame, SparkSession

from etl.constants import Constants
from etl.jobs.util.id_assigner import add_id


def main(argv):
    """
    Creates a parquet file with provider group data.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with raw sharing data
                    [2]: Output file
    """
    raw_sharing_parquet_path = argv[1]
    output_path = argv[2]

    spark = SparkSession.builder.getOrCreate()
    raw_sharing_df = spark.read.parquet(raw_sharing_parquet_path)
    contact_people_df = transform_contact_people(raw_sharing_df)
    contact_people_df.write.mode("overwrite").parquet(output_path)


def transform_contact_people(raw_sharing_df: DataFrame) -> DataFrame:
    contact_people_df = extract_contact_people(raw_sharing_df)
    contact_people_df = add_id(contact_people_df, "id")
    contact_people_df = get_columns_expected_order(contact_people_df)

    return contact_people_df


def extract_contact_people(raw_sharing_df: DataFrame) -> DataFrame:
    contact_people_df = raw_sharing_df.select(
        "name", "email", Constants.DATA_SOURCE_COLUMN).where("name is not null and email is not null")
    contact_people_df = contact_people_df.withColumnRenamed("name", "name_list")
    contact_people_df = contact_people_df.withColumnRenamed("email", "email_list")
    contact_people_df = contact_people_df.drop_duplicates()

    return contact_people_df


def get_columns_expected_order(provider_group_df: DataFrame) -> DataFrame:
    return provider_group_df.select("id", "name_list", "email_list", Constants.DATA_SOURCE_COLUMN)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
