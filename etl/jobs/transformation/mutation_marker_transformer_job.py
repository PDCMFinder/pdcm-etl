import sys

from pyspark.sql import DataFrame, SparkSession

from etl.jobs.util.id_assigner import add_id


def main(argv):
    """
    Creates a parquet file with provider type data.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with raw molecular markers
                    [2]: Output file
    """
    raw_mutation_marker_parquet_path = argv[1]

    output_path = argv[2]

    spark = SparkSession.builder.getOrCreate()
    raw_mutation_marker_df = spark.read.parquet(raw_mutation_marker_parquet_path)

    mutation_marker_df = transform_mutation_marker(raw_mutation_marker_df)
    mutation_marker_df.write.mode("overwrite").parquet(output_path)


def transform_mutation_marker(raw_mutation_marker_df: DataFrame) -> DataFrame:
    mutation_marker_df = get_mutation_marker_df(raw_mutation_marker_df)
    mutation_marker_df = mutation_marker_df.withColumnRenamed("symbol", "tmp_symbol")

    mutation_marker_df = add_id(mutation_marker_df, "id")
    mutation_marker_df.show()
    # mutation_marker_df = get_expected_columns(mutation_marker_df)
    return mutation_marker_df


def get_mutation_marker_df(raw_mutation_marker_df: DataFrame) -> DataFrame:
    return raw_mutation_marker_df.select(
        "symbol",
        "biotype",
        "coding_sequence_change",
        "variant_class",
        "codon_change",
        "amino_acid_change",
        "consequence",
        "functional_prediction",
        "seq_start_position",
        "ref_allele",
        "alt_allele",
        "ncbi_transcript_id",
        "ensembl_transcript_id",
        "variation_id"
        ).drop_duplicates()


# def get_expected_columns(ethnicity_df: DataFrame) -> DataFrame:
#     return ethnicity_df.select(
#         "id",
#         "biotype",
#         "coding_sequence_change",
#         "variant_class",
#         "codon_change",
#         "amino_acid_change",
#         "consequence",
#         "functional_prediction",
#         "seq_start_position",
#         "ref_allele",
#         "alt_allele",
#         "ncbi_transcript_id",
#         "ensembl_transcript_id",
#         "variation_id")


if __name__ == "__main__":
    sys.exit(main(sys.argv))
