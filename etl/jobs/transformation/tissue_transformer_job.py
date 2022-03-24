import sys

from pyspark.sql import DataFrame, SparkSession

from etl.jobs.util.cleaner import lower_and_trim_all
from etl.jobs.util.id_assigner import add_id


def main(argv):
    """
    Creates a parquet file with tissue data.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with raw patient data
                    [2]: Output file
    """
    raw_sample_parquet_path = argv[1]
    output_path = argv[2]

    spark = SparkSession.builder.getOrCreate()
    raw_sample_df = spark.read.parquet(raw_sample_parquet_path)
    tissue_df = transform_tissue(raw_sample_df)
    tissue_df.write.mode("overwrite").parquet(output_path)


def transform_tissue(raw_sample_df: DataFrame) -> DataFrame:
    tissue_df = get_collection_site_from_sample(raw_sample_df).union(get_primary_type_from_sample(raw_sample_df))
    tissue_df = tissue_df.drop_duplicates()
    tissue_df = add_id(tissue_df, "id")
    tissue_df = tissue_df.select("id", "name")
    return tissue_df


def get_collection_site_from_sample(raw_sample_df: DataFrame) -> DataFrame:
    return raw_sample_df.select(lower_and_trim_all("collection_site").alias("name"))


def get_primary_type_from_sample(raw_sample_df: DataFrame) -> DataFrame:
    return raw_sample_df.select(lower_and_trim_all("primary_site").alias("name"))


if __name__ == "__main__":
    sys.exit(main(sys.argv))
