import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

from etl.jobs.util.cleaner import init_cap_and_trim_all
from etl.jobs.util.id_assigner import add_id


def main(argv):
    """
    Creates a parquet file with tissue data.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with raw sample data
                    [2]: Output file
    """
    raw_sample_parquet_path = argv[1]
    output_path = argv[2]

    spark = SparkSession.builder.getOrCreate()
    raw_sample_df = spark.read.parquet(raw_sample_parquet_path)
    tumour_type_df = transform_tumour_type(raw_sample_df)
    tumour_type_df.write.mode("overwrite").parquet(output_path)


def transform_tumour_type(raw_sample_df: DataFrame) -> DataFrame:
    tumour_type = get_tumour_type_from_sample(raw_sample_df)
    tumour_type = tumour_type.drop_duplicates()
    tumour_type = add_id(tumour_type, "id")
    tumour_type = tumour_type.select("id", "name")
    return tumour_type


def get_tumour_type_from_sample(raw_sample_df: DataFrame) -> DataFrame:
    return raw_sample_df.select(init_cap_and_trim_all("tumour_type").alias("name"))


if __name__ == "__main__":
    sys.exit(main(sys.argv))
