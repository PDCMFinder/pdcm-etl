import sys

from pyspark.sql import DataFrame, SparkSession

from etl.jobs.util.cleaner import init_cap_and_trim_all
from etl.jobs.util.id_assigner import add_id


def main(argv):
    """
    Creates a parquet file with tissue data.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with raw model data
                    [2]: Output file
    """
    raw_model_parquet_path = argv[1]
    output_path = argv[2]

    spark = SparkSession.builder.getOrCreate()
    raw_model_df = spark.read.parquet(raw_model_parquet_path)
    engraftment_sample_type_df = transform_engraftment_sample_type(raw_model_df)
    engraftment_sample_type_df.write.mode("overwrite").parquet(output_path)


def transform_engraftment_sample_type(raw_model_df: DataFrame) -> DataFrame:
    engraftment_sample_type_df = get_engraftment_sample_type_from_model(raw_model_df)
    engraftment_sample_type_df = engraftment_sample_type_df.drop_duplicates()
    engraftment_sample_type_df = add_id(engraftment_sample_type_df, "id")
    engraftment_sample_type_df = engraftment_sample_type_df.select("id", "name").where("name is not null")
    return engraftment_sample_type_df


def get_engraftment_sample_type_from_model(raw_model_df: DataFrame) -> DataFrame:
    return raw_model_df.select(init_cap_and_trim_all("sample_type").alias("name"))


if __name__ == "__main__":
    sys.exit(main(sys.argv))
