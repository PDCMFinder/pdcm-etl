import sys

from pyspark.sql import DataFrame, SparkSession, Column
from pyspark.sql.functions import col, trim

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
    raw_model_parquet_path = argv[1]
    publication_group_parquet_path = argv[2]
    output_path = argv[3]

    spark = SparkSession.builder.getOrCreate()
    raw_model_df = spark.read.parquet(raw_model_parquet_path)
    publication_group_df = spark.read.parquet(publication_group_parquet_path)
    model_df = transform_model(raw_model_df, publication_group_df)
    model_df.write.mode("overwrite").parquet(output_path)


def transform_model(raw_model_df: DataFrame, publication_group_df: DataFrame) -> DataFrame:
    model_df = get_data(raw_model_df)
    model_df = set_fk_publication_group(model_df, publication_group_df)
    model_df = add_id(model_df, "id")
    model_df = get_columns_expected_order(model_df)
    return model_df


def set_fk_publication_group(model_df: DataFrame, publication_group_df: DataFrame) -> DataFrame:
    model_df = transform_to_fk(
        model_df, publication_group_df, "publications", "pub_med_ids", "id", "publication_group_id")
    return model_df


def get_data(raw_model_df) -> DataFrame:
    model_df = raw_model_df.select("model_id", "publications", "data_source").drop_duplicates()
    model_df = model_df.withColumnRenamed("model_id", "source_pdx_id")
    return model_df


def get_provider_type_from_sharing(raw_sharing_df: DataFrame) -> DataFrame:
    provider_type_df = raw_sharing_df.select(format_name_column("provider_type").alias("name"))
    provider_type_df = provider_type_df.select("name").where("name is not null")
    provider_type_df = provider_type_df.drop_duplicates()
    provider_type_df.show()
    return provider_type_df


def format_name_column(column_name) -> Column:
    return trim(col(column_name))


def get_columns_expected_order(ethnicity_df: DataFrame) -> DataFrame:
    return ethnicity_df.select("id", "source_pdx_id", "data_source", "publication_group_id")


if __name__ == "__main__":
    sys.exit(main(sys.argv))
