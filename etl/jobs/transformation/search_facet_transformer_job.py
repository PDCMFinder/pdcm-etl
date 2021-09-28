import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import array_join, collect_list, col, concat_ws, collect_set, size, when, lit, array
from pyspark.sql.types import ArrayType, StringType

from etl.jobs.util.dataframe_functions import join_left_dfs, join_dfs
from etl.jobs.util.id_assigner import add_id
from etl.workflow.config import PdcmConfig

column_names = ["facet_section",
                "facet_name",
                "facet_options",
                "facet_type"]


def main(argv):
    """
    Creates a parquet file with provider type data.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with raw molecular markers
                    [2]: Output file
    """
    search_index_parquet_path = argv[1]
    output_path = argv[2]

    spark = SparkSession.builder.getOrCreate()
    search_index_df = spark.read.parquet(search_index_parquet_path)

    search_facet_df = transform_search_facet(search_index_df)
    search_facet_df.write.mode("overwrite").parquet(output_path)


def transform_search_facet(search_index_df) -> DataFrame:
    return search_index_df


if __name__ == "__main__":
    sys.exit(main(sys.argv))
