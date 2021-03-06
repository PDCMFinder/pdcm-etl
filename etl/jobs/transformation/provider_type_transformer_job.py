import sys

from pyspark.sql import DataFrame, SparkSession, Column
from pyspark.sql.functions import col, trim

from etl.constants import Constants
from etl.jobs.util.id_assigner import add_id


def main(argv):
    """
    Creates a parquet file with provider type data.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with raw sharing data
                    [2]: Output file
    """
    raw_source_parquet_path = argv[1]
    output_path = argv[2]

    spark = SparkSession.builder.getOrCreate()
    raw_source_df = spark.read.parquet(raw_source_parquet_path)
    provider_group_df = transform_provider_group(raw_source_df)
    provider_group_df.write.mode("overwrite").parquet(output_path)


def transform_provider_group(raw_source_df: DataFrame) -> DataFrame:
    provider_type_df = get_provider_type_from_source(raw_source_df)
    provider_type_df = add_id(provider_type_df, "id")
    provider_type_df = get_columns_expected_order(provider_type_df)
    return provider_type_df


def get_provider_type_from_source(raw_source_df: DataFrame) -> DataFrame:
    provider_type_df = raw_source_df.select(
        format_name_column("provider_type").alias("name"), Constants.DATA_SOURCE_COLUMN)
    provider_type_df = provider_type_df.drop_duplicates()
    return provider_type_df


def format_name_column(column_name) -> Column:
    return trim(col(column_name))


def get_columns_expected_order(provider_type_df: DataFrame) -> DataFrame:
    return provider_type_df.select("id", "name", Constants.DATA_SOURCE_COLUMN)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
