import sys

from pyspark.sql import DataFrame, SparkSession
from etl.jobs.util.cleaner import init_cap_and_trim_all
from etl.jobs.util.dataframe_functions import transform_to_fk
from etl.jobs.util.id_assigner import add_id


def main(argv):
    """
    Creates a parquet file with provider group data.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with raw model validation data
                    [2]: Parquet file path with raw model data
                    [3]: Output file
    """
    raw_model_validation_parquet_path = argv[1]
    model_parquet_path = argv[2]

    output_path = argv[3]

    spark = SparkSession.builder.getOrCreate()
    raw_model_validation_df = spark.read.parquet(raw_model_validation_parquet_path)
    model_df = spark.read.parquet(model_parquet_path)
    quality_assurance_df = transform_quality_assurance(raw_model_validation_df, model_df)
    quality_assurance_df.write.mode("overwrite").parquet(output_path)


def transform_quality_assurance(raw_model_validation_df: DataFrame, model_df: DataFrame) -> DataFrame:
    quality_assurance_df = extract_model_validation(raw_model_validation_df)
    quality_assurance_df = set_fk_model(quality_assurance_df, model_df)
    quality_assurance_df = add_id(quality_assurance_df, "id")
    quality_assurance_df = get_columns_expected_order(quality_assurance_df)
    return quality_assurance_df


def extract_model_validation(raw_model_validation_df: DataFrame) -> DataFrame:
    quality_assurance_df = raw_model_validation_df.withColumnRenamed("passages_tested", "passages")
    quality_assurance_df = quality_assurance_df.withColumnRenamed("validation_technique", "technology")
    quality_assurance_df = quality_assurance_df.withColumn("technology", init_cap_and_trim_all("technology"))
    quality_assurance_df = quality_assurance_df.withColumnRenamed(
        "validation_host_strain_full", "validation_host_strain")

    return quality_assurance_df


def set_fk_model(quality_assurance_df, model_df):
    quality_assurance_df = quality_assurance_df.withColumnRenamed("model_id", "model_id_ref")
    quality_assurance_df = transform_to_fk(
        quality_assurance_df, model_df, "model_id_ref", "source_pdx_id", "id", "model_id")
    return quality_assurance_df


def get_columns_expected_order(quality_assurance_df: DataFrame) -> DataFrame:
    return quality_assurance_df.select(
        "id",
        "description",
        "passages",
        "technology",
        "validation_host_strain",
        "model_id"
    )


if __name__ == "__main__":
    sys.exit(main(sys.argv))
