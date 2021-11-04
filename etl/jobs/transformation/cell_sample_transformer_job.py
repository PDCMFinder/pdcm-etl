import sys

from pyspark.sql import DataFrame, SparkSession

from etl.constants import Constants
from etl.jobs.util.dataframe_functions import transform_to_fk
from etl.jobs.util.id_assigner import add_id


def main(argv):
    """
    Creates a parquet file with provider type data.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with raw molecular metadata sample
                    [2]: Parquet file path with model data
                    [3]: Parquet file path with platform data
                    [4]: Output file
    """
    raw_molecular_metadata_sample_parquet_path = argv[1]
    model_parquet_path = argv[2]
    platform_parquet_path = argv[3]

    output_path = argv[4]

    spark = SparkSession.builder.getOrCreate()
    raw_molecular_metadata_sample_df = spark.read.parquet(raw_molecular_metadata_sample_parquet_path)
    model_df = spark.read.parquet(model_parquet_path)
    platform_df = spark.read.parquet(platform_parquet_path)

    cell_sample_df = transform_cell_sample(
        raw_molecular_metadata_sample_df, model_df, platform_df)
    cell_sample_df.write.mode("overwrite").parquet(output_path)


def transform_cell_sample(
        raw_molecular_metadata_sample_df: DataFrame, model_df: DataFrame, platform_df: DataFrame) -> DataFrame:
    cell_sample_df = get_cell_sample_from_molecular_metadata_sample(raw_molecular_metadata_sample_df)
    cell_sample_df = set_fk_model(cell_sample_df, model_df)
    cell_sample_df = set_fk_platform(cell_sample_df, platform_df)
    cell_sample_df = add_id(cell_sample_df, "id")
    cell_sample_df = get_expected_columns(cell_sample_df)
    return cell_sample_df


def get_cell_sample_from_molecular_metadata_sample(raw_molecular_metadata_sample_df: DataFrame) -> DataFrame:
    cell_sample_df = raw_molecular_metadata_sample_df.select(
        "sample_id",
        "model_id",
        "passage",
        "platform_id",
        Constants.DATA_SOURCE_COLUMN).where("sample_origin = 'cell'")
    cell_sample_df = cell_sample_df.drop_duplicates()
    cell_sample_df = cell_sample_df.withColumnRenamed("sample_id", "external_cell_sample_id")
    cell_sample_df = cell_sample_df.withColumnRenamed("model_id", "external_model_id")
    return cell_sample_df


def set_fk_model(cell_sample_df: DataFrame, model_df: DataFrame) -> DataFrame:
    model_df = model_df.select("id", "external_model_id", "data_source")
    model_df = model_df.withColumnRenamed("id", "model_id")
    model_df = model_df.withColumnRenamed("data_source", Constants.DATA_SOURCE_COLUMN)
    cell_sample_df = cell_sample_df.join(
        model_df, on=["external_model_id", Constants.DATA_SOURCE_COLUMN], how='left')
    cell_sample_df = cell_sample_df.drop(model_df[Constants.DATA_SOURCE_COLUMN])
    return cell_sample_df


def set_fk_platform(cell_sample_df: DataFrame, platform_df: DataFrame) -> DataFrame:
    cell_sample_df = cell_sample_df.withColumnRenamed("platform_id", "external_platform_id")
    platform_df = platform_df.select("id", "platform_id")
    platform_df = platform_df.withColumnRenamed("platform_id", "external_platform_id")
    cell_sample_df = transform_to_fk(
        cell_sample_df, platform_df, "external_platform_id", "external_platform_id", "id", "platform_id")
    return cell_sample_df


def get_expected_columns(xenograft_sample_df: DataFrame) -> DataFrame:
    return xenograft_sample_df.select(
        "id", "external_cell_sample_id", "passage", "model_id", "platform_id", Constants.DATA_SOURCE_COLUMN)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
