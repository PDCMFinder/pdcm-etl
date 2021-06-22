import sys

from pyspark.sql import DataFrame, SparkSession
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
    contact_form_df = transform_contact_form(raw_sharing_df)
    contact_form_df.write.mode("overwrite").parquet(output_path)


def transform_contact_form(raw_sharing_df: DataFrame) -> DataFrame:
    contact_form_df = extract_contact_form(raw_sharing_df)
    contact_form_df = add_id(contact_form_df, "id")
    contact_form_df = get_columns_expected_order(contact_form_df)

    return contact_form_df


def extract_contact_form(raw_sharing_df: DataFrame) -> DataFrame:
    contact_form_df = raw_sharing_df.select("form_url").where("form_url is not null")
    contact_form_df = contact_form_df.drop_duplicates()

    return contact_form_df


def get_columns_expected_order(provider_group_df: DataFrame) -> DataFrame:
    return provider_group_df.select("id", "form_url")


if __name__ == "__main__":
    sys.exit(main(sys.argv))
