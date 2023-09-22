import sys

from pyspark.sql import DataFrame, SparkSession

from etl.jobs.util.id_assigner import add_id


def main(argv):
    """
    Creates a parquet file with the image study transformed data.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with raw image study data
                    [2]: Output file
    """
    image_study_parquet_path = argv[1]
    output_path = argv[2]

    spark = SparkSession.builder.getOrCreate()
    raw_image_study_df = spark.read.parquet(image_study_parquet_path)

    image_study_df = transform_image_study(raw_image_study_df)
    image_study_df.write.mode("overwrite").parquet(output_path)


def transform_image_study(raw_image_study_df: DataFrame) -> DataFrame:
    image_study_df = add_id(raw_image_study_df, "id")

    return image_study_df


if __name__ == "__main__":
    sys.exit(main(sys.argv))
