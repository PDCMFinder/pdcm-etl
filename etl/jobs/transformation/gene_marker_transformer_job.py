import sys

from pyspark.sql import DataFrame, SparkSession
from etl.jobs.util.id_assigner import add_id


def main(argv):
    """
    Creates a parquet file with provider group data.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with raw sharing data
                    [2]: Output file
    """
    raw_gene_marker_parquet_path = argv[1]
    output_path = argv[2]

    spark = SparkSession.builder.getOrCreate()
    raw_gene_marker_df = spark.read.parquet(raw_gene_marker_parquet_path)
    gene_marker_df = transform_gene_marker(raw_gene_marker_df)
    gene_marker_df.write.mode("overwrite").parquet(output_path)


def transform_gene_marker(raw_gene_marker_df: DataFrame) -> DataFrame:
    gene_marker_df = add_id(raw_gene_marker_df, "id")
    return gene_marker_df


if __name__ == "__main__":
    sys.exit(main(sys.argv))
