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
    source_database_df = transform_source_database(raw_sharing_df)
    source_database_df.write.mode("overwrite").parquet(output_path)


def transform_source_database(raw_sharing_df: DataFrame) -> DataFrame:
    source_database_df = extract_source_database(raw_sharing_df)
    source_database_df = add_id(source_database_df, "id")
    source_database_df = get_columns_expected_order(source_database_df)

    return source_database_df


def extract_source_database(raw_sharing_df: DataFrame) -> DataFrame:
    source_database_df = raw_sharing_df.select("database_url").where("database_url is not null")
    source_database_df = source_database_df.drop_duplicates()

    return source_database_df


def get_columns_expected_order(provider_group_df: DataFrame) -> DataFrame:
    return provider_group_df.select("id", "database_url")


if __name__ == "__main__":
    sys.exit(main(sys.argv))
