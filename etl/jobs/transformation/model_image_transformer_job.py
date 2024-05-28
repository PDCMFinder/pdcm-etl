import sys

from pyspark.sql import DataFrame, SparkSession

from etl.constants import Constants
from etl.jobs.util.cleaner import null_values_to_empty_string
from etl.jobs.util.id_assigner import add_id


def main(argv):
    """
    Creates a parquet file with the model image transformed data.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with raw model image data
                    [2]: Parquet file path with transformed model data
                    [3]: Output file
    """
    raw_model_image_parquet_path = argv[1]
    model_parquet_path = argv[2]

    output_path = argv[3]

    spark = SparkSession.builder.getOrCreate()
    raw_model_image_df = spark.read.parquet(raw_model_image_parquet_path)
    model_df = spark.read.parquet(model_parquet_path)

    model_image_df = transform_model_image(raw_model_image_df, model_df)
    model_image_df.write.mode("overwrite").parquet(output_path)


def transform_model_image(raw_model_image_df: DataFrame, model_df: DataFrame) -> DataFrame:
    model_image_df = raw_model_image_df.drop_duplicates()
    model_image_df = set_fk_model(model_image_df, model_df)
    model_image_df = add_id(model_image_df, "id")
    model_image_df = null_values_to_empty_string(model_image_df)

    return model_image_df


def set_fk_model(model_image_df: DataFrame, model_df: DataFrame) -> DataFrame:
    model_df = model_df.select("id", "external_model_id", "data_source")
    model_df = model_df.withColumnRenamed("data_source", Constants.DATA_SOURCE_COLUMN)
    model_image_df = model_image_df.withColumnRenamed("model_id", "external_model_id")
    model_image_df = model_image_df.join(model_df, on=["external_model_id", Constants.DATA_SOURCE_COLUMN], how='inner')
    model_image_df = model_image_df.withColumnRenamed("id", "model_id")
    return model_image_df


if __name__ == "__main__":
    sys.exit(main(sys.argv))
