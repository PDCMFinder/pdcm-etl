import sys

from pyspark.sql import DataFrame, SparkSession, Column
from pyspark.sql.functions import col, trim, lit

from etl.constants import Constants
from etl.jobs.util.dataframe_functions import transform_to_fk
from etl.jobs.util.id_assigner import add_id


def main(argv):
    """
    Creates a parquet file with provider type data.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with raw sample platform data
                    [2]: Output file
    """
    raw_sample_platform_parquet_path = argv[1]
    provider_group_parquet_path = argv[2]
    output_path = argv[3]

    spark = SparkSession.builder.getOrCreate()
    raw_sample_platform_df = spark.read.parquet(raw_sample_platform_parquet_path)
    provider_group_df = spark.read.parquet(provider_group_parquet_path)
    platform_df = transform_platform(raw_sample_platform_df, provider_group_df)
    print("Ready")
    platform_df.show()
    platform_df.write.mode("overwrite").parquet(output_path)


def transform_platform(raw_sample_platform_df: DataFrame, provider_group_df) -> DataFrame:
    platform_df = get_platform_data(raw_sample_platform_df)
    platform_df = set_fk_provider_group(platform_df, provider_group_df)
    platform_df = add_id(platform_df, "id")
    platform_df = get_columns_expected_order(platform_df)
    return platform_df


def get_platform_data(raw_sample_platform_df: DataFrame) -> DataFrame:
    platform_df = raw_sample_platform_df.select(
        "platform", "platform_type", lit("").alias("library_selection"), Constants.DATA_SOURCE_COLUMN)
    platform_df = platform_df.withColumnRenamed("platform", "instrument_model")
    platform_df = platform_df.withColumnRenamed("platform_type", "library_strategy")
    platform_df = platform_df.drop_duplicates()
    return platform_df


def set_fk_provider_group(platform_df: DataFrame, provider_group_df: DataFrame) -> DataFrame:
    platform_df = transform_to_fk(
        platform_df,
        provider_group_df,
        Constants.DATA_SOURCE_COLUMN,
        Constants.DATA_SOURCE_COLUMN,
        "id",
        "provider_group_id")
    return platform_df


def format_name_column(column_name) -> Column:
    return trim(col(column_name))


def get_columns_expected_order(ethnicity_df: DataFrame) -> DataFrame:
    return ethnicity_df.select("id", "library_strategy", "provider_group_id", "instrument_model", "library_selection")


if __name__ == "__main__":
    sys.exit(main(sys.argv))
