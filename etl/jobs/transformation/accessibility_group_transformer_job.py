import sys

from pyspark.sql import DataFrame, SparkSession, Column
from pyspark.sql.functions import col, trim

from etl.constants import Constants
from etl.jobs.util.cleaner import trim_all
from etl.jobs.util.id_assigner import add_id


def main(argv):
    """
    Creates a parquet file with provider type data.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with raw sharing data
                    [2]: Output file
    """
    raw_sharing_parquet_path = argv[1]

    output_path = argv[3]

    spark = SparkSession.builder.getOrCreate()
    raw_sharing_df = spark.read.parquet(raw_sharing_parquet_path)

    accessibility_group_df = transform_accessibility_group(raw_sharing_df)
    accessibility_group_df.write.mode("overwrite").parquet(output_path)


def transform_accessibility_group(raw_sharing_df: DataFrame) -> DataFrame:
    accessibility_group_df = get_accessibility_group_from_sharing(raw_sharing_df)
    accessibility_group_df = add_id(accessibility_group_df, "id")
    accessibility_group_df = get_columns_expected_order(accessibility_group_df)
    return accessibility_group_df


def get_accessibility_group_from_sharing(raw_sharing_df: DataFrame) -> DataFrame:
    accessibility_group_df = raw_sharing_df.withColumn("europdx_access_modalities", trim_all("europdx_access_modality"))
    accessibility_group_df = accessibility_group_df.withColumn("accessibility", trim_all("accessibility"))
    accessibility_group_df = accessibility_group_df.select(
        "europdx_access_modalities", "accessibility", Constants.DATA_SOURCE_COLUMN)
    accessibility_group_df = accessibility_group_df.drop_duplicates()
    return accessibility_group_df


def format_name_column(column_name) -> Column:
    return trim(col(column_name))


def get_columns_expected_order(accessibility_group_df: DataFrame) -> DataFrame:
    return accessibility_group_df.select(
        "id", "europdx_access_modalities", "accessibility", Constants.DATA_SOURCE_COLUMN)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
