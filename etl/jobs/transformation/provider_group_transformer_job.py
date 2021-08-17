import sys

from pyspark.sql import DataFrame, SparkSession, Column
from pyspark.sql.functions import col

from etl.constants import Constants
from etl.jobs.util.cleaner import trim_all
from etl.jobs.util.dataframe_functions import transform_to_fk
from etl.jobs.util.id_assigner import add_id


def main(argv):
    """
    Creates a parquet file with provider group data.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with raw sharing data
                    [2]: Parquet file path with provider type data
                    [3]: Output file
    """
    raw_sharing_parquet_path = argv[1]
    provider_type_parquet_path = argv[2]
    output_path = argv[3]

    spark = SparkSession.builder.getOrCreate()
    raw_sharing_df = spark.read.parquet(raw_sharing_parquet_path)
    provider_type_df = spark.read.parquet(provider_type_parquet_path)
    provider_group_df = transform_provider_group(raw_sharing_df, provider_type_df)
    provider_group_df.write.mode("overwrite").parquet(output_path)


def transform_provider_group(raw_sharing_df: DataFrame, provider_type_df: DataFrame) -> DataFrame:
    provider_group_df = extract_data_sharing(raw_sharing_df)

    provider_group_df = set_fk_provider_type(
        provider_group_df, provider_type_df)

    provider_group_df = add_id(provider_group_df, "id")
    provider_group_df = get_columns_expected_order(provider_group_df)
    return provider_group_df


def extract_data_sharing(raw_sharing_df: DataFrame) -> DataFrame:
    data_from_sharing_df = raw_sharing_df.select(
        format_column("provider_abbreviation").alias("provider_abbreviation"),
        format_column("provider_type").alias("provider_type"),
        format_column("provider_name").alias("provider_name"),
        Constants.DATA_SOURCE_COLUMN
    )
    data_from_sharing_df = data_from_sharing_df.distinct()

    return data_from_sharing_df


def format_column(column_name) -> Column:
    return trim_all(column_name)


def join_sharing_loader(
        data_from_sharing_df: DataFrame,
        data_from_loader_df: DataFrame) -> DataFrame:
    data_from_loader_ref_df = data_from_loader_df.withColumnRenamed(
        "abbreviation", "provider_abbreviation")
    join_sharing_loader_df = data_from_sharing_df.join(
        data_from_loader_ref_df, on=[Constants.DATA_SOURCE_COLUMN])
    join_sharing_loader_df = join_sharing_loader_df.withColumnRenamed("provider_name", "name")
    return join_sharing_loader_df


def set_fk_provider_type(provider_group_df, provider_type_df):
    provider_group_df = transform_to_fk(
        provider_group_df, provider_type_df, "provider_type", "name", "id", "provider_type_id")
    provider_group_df = provider_group_df.withColumnRenamed("provider_name", "name")
    return provider_group_df


def get_columns_expected_order(provider_group_df: DataFrame) -> DataFrame:
    return provider_group_df.select(
        col("id"),
        col("name"),
        col("provider_abbreviation").alias("abbreviation"),
        col("provider_type_id"),
        Constants.DATA_SOURCE_COLUMN
    )


if __name__ == "__main__":
    sys.exit(main(sys.argv))
