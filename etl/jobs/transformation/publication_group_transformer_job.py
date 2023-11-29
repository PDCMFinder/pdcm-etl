import sys

from pyspark.sql import DataFrame, SparkSession, Column
from pyspark.sql.functions import col, trim
from etl.jobs.util.id_assigner import add_id


def main(argv):
    """
    Creates a parquet file with publication group type data.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with raw model data
                    [2]: Parquet file path with raw cell model data
                    [3]: Output file
    """
    raw_model_parquet_path = argv[1]
    raw_cell_model_parquet_path = argv[2]
    output_path = argv[3]

    spark = SparkSession.builder.getOrCreate()
    raw_model_df = spark.read.parquet(raw_model_parquet_path)
    raw_cell_model_df = spark.read.parquet(raw_cell_model_parquet_path)
    publication_group_df = transform_publication_group(raw_model_df, raw_cell_model_df)
    publication_group_df.write.mode("overwrite").parquet(output_path)


def transform_publication_group(raw_model_df: DataFrame, raw_cell_model_df: DataFrame) -> DataFrame:
    publication_group_df = extract_publications_from_models(raw_model_df, raw_cell_model_df)
    publication_group_df = add_id(publication_group_df, "id")
    publication_group_df = get_columns_expected_order(publication_group_df)
    return publication_group_df


def extract_publications_from_models(raw_model_df: DataFrame, raw_cell_model_df: DataFrame) -> DataFrame:
    pdx_publications_df = raw_model_df.select("publications").where("publications is not null").drop_duplicates()
    cell_model_publications_df = raw_cell_model_df.select("publications").where("publications is not null").drop_duplicates()
    publication_group_df = pdx_publications_df.union(cell_model_publications_df)
    publication_group_df = publication_group_df.withColumnRenamed("publications", "pubmed_ids")
    return publication_group_df


def format_name_column(column_name) -> Column:
    return trim(col(column_name))


def get_columns_expected_order(publication_group_df: DataFrame) -> DataFrame:
    return publication_group_df.select("id", "pubmed_ids")


if __name__ == "__main__":
    sys.exit(main(sys.argv))
