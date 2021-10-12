import sys

from pyspark.sql import DataFrame, SparkSession, Column
from pyspark.sql.functions import col, trim

from etl.constants import Constants
from etl.jobs.util.dataframe_functions import transform_to_fk
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
    raw_other_model_df = spark.read.parquet(raw_other_model_parquet_path)
    raw_other_model_df.show()
    model_df = spark.read.parquet(model_parquet_path)
    print("init model_df")
    model_df.show()
    other_model_df = transform_other_model(raw_other_model_df, model_df)
    other_model_df.show()
    other_model_df.write.mode("overwrite").parquet(output_path)


def transform_other_model(raw_other_model_df: DataFrame, model_df: DataFrame) -> DataFrame:
    
    other_model_df = raw_other_model_df.withColumnRenamed("patient_sample_id", "origin_patient_sample_id")
    other_model_df = set_fk_model(other_model_df, model_df)
    other_model_df = other_model_df.withColumnRenamed(Constants.DATA_SOURCE_COLUMN, "provider_abb")
    other_model_df = add_id(other_model_df, "id")

    return other_model_df


def get_data_from_other_model(raw_other_model_df) -> DataFrame:
    model_df = raw_other_model_df.select(
        "model_id", "publications", Constants.DATA_SOURCE_COLUMN).drop_duplicates()
    model_df = model_df.withColumnRenamed("model_id", "external_model_id")
    return model_df


def set_fk_model(other_model_df: DataFrame, model_df: DataFrame) -> DataFrame:
    model_df = model_df.select("id", "external_model_id")
    model_df = model_df.withColumnRenamed("id", "model_id")
    print("model_df")
    model_df.show()
    other_model_df = other_model_df.withColumnRenamed("model_id", "external_model_id")
    other_model_df = other_model_df.join(model_df, on=['external_model_id'], how='left')
    # other_model_df = transform_to_fk(
    #     other_model_df, model_df, "external_model_id", "external_model_id", "id", "model_id")
    return other_model_df


if __name__ == "__main__":
    sys.exit(main(sys.argv))
