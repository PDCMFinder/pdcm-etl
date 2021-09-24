import sys

from pyspark.sql import DataFrame, SparkSession


def main(argv):
    """
    Creates a parquet file with provider type data.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with raw molecular markers
                    [2]: Output file
    """
    search_index_parquet_path = argv[1]
    output_path = argv[11]

    spark = SparkSession.builder.getOrCreate()
    search_facet_df = spark.read.parquet(search_index_parquet_path)
    search_facet_df.write.mode("overwrite").parquet(output_path)


def transform_search_facet(search_index_df) -> DataFrame:
    facet_templates = [{"section_name": "Cancer Model", "subsections": [{"section_name": "Dataset available", "column_name": "data_available"}]}]
    return search_index_df


if __name__ == "__main__":
    sys.exit(main(sys.argv))
