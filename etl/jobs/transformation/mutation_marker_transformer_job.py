import sys

from pyspark.sql import DataFrame, SparkSession

from etl.jobs.transformation.harmonisation.markers_harmonisation import harmonise_mutation_marker_symbols
from etl.jobs.util.id_assigner import add_id


def main(argv):
    """
    Creates a parquet file with provider type data.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with raw molecular markers
                    [2]: Output file
    """
    raw_mutation_marker_parquet_path = argv[1]
    gene_markers_parquet_path = argv[2]

    output_path = argv[3]

    spark = SparkSession.builder.getOrCreate()
    raw_mutation_marker_df = spark.read.parquet(raw_mutation_marker_parquet_path)
    mutation_marker_df = transform_mutation_marker(raw_mutation_marker_df, gene_markers_parquet_path)
    mutation_marker_df.write.mode("overwrite").parquet(output_path)


def transform_mutation_marker(raw_mutation_marker_df: DataFrame, gene_markers_parquet_path) -> DataFrame:
    mutation_marker_df = get_mutation_marker_df(raw_mutation_marker_df)
    mutation_marker_df = harmonise_mutation_marker_symbols(mutation_marker_df, gene_markers_parquet_path)
    mutation_marker_df = add_id(mutation_marker_df, "id")
    return mutation_marker_df


def get_mutation_marker_df(raw_mutation_marker_df: DataFrame) -> DataFrame:
    return raw_mutation_marker_df.select(
        "sample_id",
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
        "variation_id",
        "ensembl_gene_id",
        "ncbi_gene_id"
        ).drop_duplicates()


if __name__ == "__main__":
    sys.exit(main(sys.argv))
