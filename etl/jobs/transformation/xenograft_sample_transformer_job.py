import sys

from pyspark.sql import DataFrame, SparkSession

from etl.constants import Constants
from etl.jobs.util.dataframe_functions import transform_to_fk
from etl.jobs.util.id_assigner import add_id
from pyspark.sql.functions import col


def main(argv):
    """
    Creates a parquet file with provider type data.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with raw sample platform data
                    [2]: Output file
    """
    raw_molecular_metadata_sample_parquet_path = argv[1]
    host_strain_parquet_path = argv[2]
    model_parquet_path = argv[3]
    platform_parquet_path = argv[4]

    output_path = argv[5]

    spark = SparkSession.builder.getOrCreate()
    raw_molecular_metadata_sample_df = spark.read.parquet(raw_molecular_metadata_sample_parquet_path)
    host_strain_df = spark.read.parquet(host_strain_parquet_path)
    model_df = spark.read.parquet(model_parquet_path)
    platform_df = spark.read.parquet(platform_parquet_path)

    xenograft_sample_df = transform_xenograft_sample(
        raw_molecular_metadata_sample_df, host_strain_df, model_df, platform_df)
    xenograft_sample_df.write.mode("overwrite").parquet(output_path)


def transform_xenograft_sample(
        raw_molecular_metadata_sample_df: DataFrame,
        host_strain_df: DataFrame,
        model_df: DataFrame,
        platform_df: DataFrame) -> DataFrame:
    xenograft_sample_df = get_xenograft_sample_from_sample_platform(raw_molecular_metadata_sample_df)
    xenograft_sample_df = set_fk_host_strain(xenograft_sample_df, host_strain_df)
    xenograft_sample_df = set_fk_model(xenograft_sample_df, model_df)
    xenograft_sample_df = set_fk_platform(xenograft_sample_df, platform_df)
    xenograft_sample_df = add_id(xenograft_sample_df, "id")
    xenograft_sample_df = get_expected_columns(xenograft_sample_df)
    return xenograft_sample_df


def get_xenograft_sample_from_sample_platform(raw_molecular_metadata_sample_df: DataFrame) -> DataFrame:
    xenograft_sample_df = raw_molecular_metadata_sample_df.select(
        "sample_id",
        "model_id",
        "passage",
        "host_strain_nomenclature",
        "raw_data_url",
        "platform_id",
        Constants.DATA_SOURCE_COLUMN).where("sample_origin = 'xenograft'")
    xenograft_sample_df = xenograft_sample_df.drop_duplicates()
    xenograft_sample_df = xenograft_sample_df.withColumnRenamed("sample_id", "external_xenograft_sample_id")
    return xenograft_sample_df


def set_fk_host_strain(xenograft_sample_df: DataFrame, host_strain_df: DataFrame) -> DataFrame:
    host_strain_df = host_strain_df.withColumn("host_strain_nomenclature_bk", col("nomenclature"))
    xenograft_sample_df = transform_to_fk(
        xenograft_sample_df, host_strain_df, "host_strain_nomenclature", "nomenclature", "id", "host_strain_id")
    return xenograft_sample_df


def set_fk_model(xenograft_sample_df: DataFrame, model_df: DataFrame) -> DataFrame:
    xenograft_sample_df = xenograft_sample_df.withColumnRenamed("model_id", "external_model_id")
    model_df = model_df.select("id", "external_model_id", "data_source")
    model_df = model_df.withColumnRenamed("id", "model_id")
    model_df = model_df.withColumnRenamed("data_source", Constants.DATA_SOURCE_COLUMN)
    xenograft_sample_df = xenograft_sample_df.join(
        model_df, on=["external_model_id", Constants.DATA_SOURCE_COLUMN], how='left')
    return xenograft_sample_df


def set_fk_platform(xenograft_sample_df: DataFrame, platform_df: DataFrame) -> DataFrame:
    xenograft_sample_df = xenograft_sample_df.withColumnRenamed("platform_id", "external_platform_id")
    platform_df = platform_df.withColumnRenamed("platform_id", "external_platform_id")
    xenograft_sample_df = transform_to_fk(
        xenograft_sample_df, platform_df, "external_platform_id", "external_platform_id", "id", "platform_id")
    return xenograft_sample_df


def get_expected_columns(xenograft_sample_df: DataFrame) -> DataFrame:
    return xenograft_sample_df.select(
        "id", "external_xenograft_sample_id", "passage", "host_strain_id", "model_id", "raw_data_url", "platform_id")


if __name__ == "__main__":
    sys.exit(main(sys.argv))
