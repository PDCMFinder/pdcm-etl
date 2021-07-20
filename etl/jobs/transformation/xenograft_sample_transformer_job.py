import sys

from pyspark.sql import DataFrame, SparkSession, Column
from pyspark.sql.functions import col, trim

from etl.jobs.util.id_assigner import add_id


def main(argv):
    """
    Creates a parquet file with provider type data.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with raw sample platform data
                    [2]: Output file
    """
    raw_sample_platform_parquet_path = argv[1]
    output_path = argv[2]

    spark = SparkSession.builder.getOrCreate()
    raw_sample_platform_df = spark.read.parquet(raw_sample_platform_parquet_path)
    xenograft_sample_df = transform_xenograft_sample(raw_sample_platform_df)
    xenograft_sample_df.write.mode("overwrite").parquet(output_path)


def transform_xenograft_sample(raw_sample_platform_df: DataFrame) -> DataFrame:
    xenograft_sample_df = get_xenograft_sample_from_sample_platform(raw_sample_platform_df)
    xenograft_sample_df = add_id(xenograft_sample_df, "id")
    xenograft_sample_df = get_columns_expected_order(xenograft_sample_df)
    return xenograft_sample_df


def get_xenograft_sample_from_sample_platform(raw_sample_platform_df: DataFrame) -> DataFrame:
    xenograft_sample_df = raw_sample_platform_df.select("sample_id").where("sample_origin = 'xenograft'")
    xenograft_sample_df = xenograft_sample_df.drop_duplicates()
    xenograft_sample_df = xenograft_sample_df.withColumnRenamed("sample_id", "external_xenograft_sample_id")
    return xenograft_sample_df


def format_name_column(column_name) -> Column:
    return trim(col(column_name))


def get_columns_expected_order(ethnicity_df: DataFrame) -> DataFrame:
    return ethnicity_df.select("id", "external_xenograft_sample_id")


if __name__ == "__main__":
    sys.exit(main(sys.argv))
