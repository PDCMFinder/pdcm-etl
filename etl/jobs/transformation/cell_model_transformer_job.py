import sys

from pyspark.sql import DataFrame, SparkSession

from etl.constants import Constants
from etl.jobs.util.id_assigner import add_id


def main(argv):
    """
    Creates a parquet file with model data.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with raw model data
                    [2]: Parquet file path with publication group data
                    [3]: Output file
    """
    raw_other_model_parquet_path = argv[1]
    model_parquet_path = argv[2]
    output_path = argv[3]

    spark = SparkSession.builder.getOrCreate()
    raw_cell_model_df = spark.read.parquet(raw_other_model_parquet_path)
    model_df = spark.read.parquet(model_parquet_path)
    cell_model_df = transform_cell_model(raw_cell_model_df, model_df)
    cell_model_df.write.mode("overwrite").parquet(output_path)


def transform_cell_model(raw_cell_model_df: DataFrame, model_df: DataFrame) -> DataFrame:
    cell_model_df = set_fk_model(raw_cell_model_df, model_df)
    cell_model_df = cell_model_df.withColumnRenamed(Constants.DATA_SOURCE_COLUMN, "provider_abb")
    cell_model_df = add_id(cell_model_df, "id")

    return cell_model_df


def get_data_from_other_model(raw_other_model_df) -> DataFrame:
    model_df = raw_other_model_df.select(
        "model_id", "publications", Constants.DATA_SOURCE_COLUMN).drop_duplicates()
    model_df = model_df.withColumnRenamed("model_id", "external_model_id")
    return model_df


def set_fk_model(other_model_df: DataFrame, model_df: DataFrame) -> DataFrame:
    model_df = model_df.select("id", "external_model_id")
    model_df = model_df.withColumnRenamed("id", "model_id")
    other_model_df = other_model_df.withColumnRenamed("model_id", "external_model_id")
    other_model_df = other_model_df.join(model_df, on=['external_model_id'], how='left')
    return other_model_df


if __name__ == "__main__":
    sys.exit(main(sys.argv))
